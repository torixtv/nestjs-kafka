import {
  Injectable,
  Logger,
  Inject,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import {
  Consumer,
  Kafka,
  Admin,
  ITopicConfig,
  EachMessagePayload,
  KafkaMessage,
} from 'kafkajs';
import {
  KAFKAJS_INSTANCE,
  KAFKA_MODULE_OPTIONS,
} from '../core/kafka.constants';
import { KafkaModuleOptions } from '../interfaces/kafka.interfaces';
import { KafkaHandlerRegistry } from './kafka.registry';
import { KafkaProducerService } from '../core/kafka.producer';
import {
  createDlqSpan,
  executeWithSpan,
  isTracingEnabled,
} from '../utils/tracing.utils';

export interface DlqMessageHeaders {
  'x-original-topic': string;
  'x-handler-id': string;
  'x-retry-count': string;
  'x-dlq-timestamp': string;
  'x-dlq-reason': string;
  'x-correlation-id'?: string;
  'x-original-partition'?: string;
  'x-original-offset'?: string;
  'x-original-timestamp'?: string;
}

export interface DlqReprocessingOptions {
  batchSize?: number;
  timeoutMs?: number;
  stopOnError?: boolean;
}

@Injectable()
export class KafkaDlqService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaDlqService.name);
  private admin: Admin;
  private dlqTopicName: string;
  private isReprocessing = false;
  private reprocessingConsumer: Consumer | null = null;

  private metrics = {
    messagesReprocessed: 0,
    messagesSkipped: 0,
    messagesStored: 0,
    errorsEncountered: 0,
    reprocessingSessions: 0,
  };

  constructor(
    @Inject(KAFKAJS_INSTANCE) private readonly kafka: Kafka,
    @Inject(KAFKA_MODULE_OPTIONS) private readonly options: KafkaModuleOptions,
    private readonly handlerRegistry: KafkaHandlerRegistry,
    private readonly producer: KafkaProducerService,
  ) {
    this.admin = this.kafka.admin();
    this.dlqTopicName = this.buildDlqTopicName();
  }

  async onModuleInit(): Promise<void> {
    if (this.options.dlq?.enabled) {
      await this.ensureDlqTopicExists();
      this.logger.log('DLQ service initialized');
    } else {
      this.logger.log('DLQ is disabled, service will remain inactive');
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (this.isReprocessing && this.reprocessingConsumer) {
      await this.stopReprocessing();
    }
    await this.admin.disconnect();
  }

  private buildDlqTopicName(): string {
    const clientId = this.options.client?.clientId || 'kafka-service';
    return `${clientId}.dlq`;
  }

  private async ensureDlqTopicExists(): Promise<void> {
    try {
      await this.admin.connect();

      const existingTopics = await this.admin.listTopics();

      if (existingTopics.includes(this.dlqTopicName)) {
        this.logger.debug(`DLQ topic already exists: ${this.dlqTopicName}`);
        return;
      }

      const topicConfig: ITopicConfig = {
        topic: this.dlqTopicName,
        numPartitions: this.options.dlq?.topicPartitions || 3,
        replicationFactor: this.options.dlq?.topicReplicationFactor || 1,
        configEntries: [
          {
            name: 'cleanup.policy',
            value: 'delete',
          },
          {
            name: 'retention.ms',
            value: String(
              this.options.dlq?.topicRetentionMs || 7 * 24 * 60 * 60 * 1000, // 7 days
            ),
          },
          {
            name: 'segment.ms',
            value: String(
              this.options.dlq?.topicSegmentMs || 24 * 60 * 60 * 1000, // 24 hours
            ),
          },
        ],
      };

      await this.admin.createTopics({
        topics: [topicConfig],
        waitForLeaders: true,
      });

      this.logger.log(`Created DLQ topic: ${this.dlqTopicName}`);
    } catch (error) {
      this.logger.error(
        `Failed to ensure DLQ topic exists: ${this.dlqTopicName}`,
        error,
      );
      throw error;
    } finally {
      await this.admin.disconnect();
    }
  }

  async startReprocessing(options: DlqReprocessingOptions = {}): Promise<void> {
    if (!this.options.dlq?.enabled) {
      throw new Error('DLQ is not enabled');
    }

    if (this.isReprocessing) {
      this.logger.warn('DLQ reprocessing is already in progress');
      return;
    }

    this.logger.log(
      `Starting DLQ reprocessing for topic: ${this.dlqTopicName}`,
    );
    this.isReprocessing = true;
    this.metrics.reprocessingSessions++;

    const baseGroupId = this.options.consumer?.groupId || 'kafka-service';
    const { groupId: _, ...consumerOptions } = this.options.consumer || {};

    // Create a fixed consumer group for DLQ reprocessing to avoid proliferation
    const reprocessingGroupId = `${baseGroupId}.dlq.reprocessing`;
    this.reprocessingConsumer = this.kafka.consumer({
      groupId: reprocessingGroupId,
      ...consumerOptions,
    });

    try {
      await this.reprocessingConsumer.connect();
      // Subscribe with fromBeginning to reprocess all messages in DLQ
      // This ensures we reprocess all failed messages when requested
      await this.reprocessingConsumer.subscribe({
        topic: this.dlqTopicName,
        fromBeginning: true,
      });

      const startTime = Date.now();
      const timeoutMs = options.timeoutMs || 30000; // 30 second default timeout
      const batchSize = options.batchSize || 100;
      const stopOnError = options.stopOnError ?? false;

      let processedCount = 0;
      let lastMessageTime = Date.now();

      await this.reprocessingConsumer.run({
        autoCommit: true,
        partitionsConsumedConcurrently: 1,
        eachMessage: async (payload) => {
          const { message, partition, topic } = payload;

          try {
            const headers = this.extractHeaders(message);
            if (!headers) {
              this.logger.warn('Skipping message with invalid DLQ headers');
              this.metrics.messagesSkipped++;
              return;
            }

            this.logger.debug(
              `Reprocessing DLQ message - Handler: ${headers['x-handler-id']}, Original Topic: ${headers['x-original-topic']}`,
            );

            await this.reprocessMessage(payload, headers);

            processedCount++;
            lastMessageTime = Date.now();
            this.metrics.messagesReprocessed++;

            // Check if we should stop due to batch size limit
            if (batchSize && processedCount >= batchSize) {
              this.logger.log(`Reached batch size limit: ${batchSize}`);
              return;
            }

            // Check timeout
            if (Date.now() - startTime > timeoutMs) {
              this.logger.log('Reprocessing timeout reached');
              return;
            }
          } catch (error) {
            this.metrics.errorsEncountered++;
            this.logger.error('Error reprocessing DLQ message:', error);

            if (stopOnError) {
              this.logger.error('Stopping reprocessing due to error');
              throw error;
            }

            // With autoCommit, Kafka will handle offset management automatically
          }
        },
      });

      // Wait for a moment to see if there are more messages
      // If no new messages for 5 seconds, assume queue is empty
      const checkInterval = 1000;
      let noMessageCount = 0;
      const maxNoMessageChecks = 5;

      const checkForCompletion = setInterval(() => {
        const timeSinceLastMessage = Date.now() - lastMessageTime;
        if (timeSinceLastMessage > checkInterval) {
          noMessageCount++;
          if (noMessageCount >= maxNoMessageChecks) {
            clearInterval(checkForCompletion);
            this.stopReprocessing().then(() => {
              this.logger.log(
                `DLQ reprocessing completed. Processed ${processedCount} messages`,
              );
            });
          }
        } else {
          noMessageCount = 0;
        }
      }, checkInterval);

      // Cleanup interval on timeout
      setTimeout(() => {
        clearInterval(checkForCompletion);
        this.stopReprocessing().then(() => {
          this.logger.log(
            `DLQ reprocessing stopped due to timeout. Processed ${processedCount} messages`,
          );
        });
      }, timeoutMs);
    } catch (error) {
      this.logger.error('Failed to start DLQ reprocessing:', error);
      this.isReprocessing = false;
      if (this.reprocessingConsumer) {
        await this.reprocessingConsumer.disconnect();
        this.reprocessingConsumer = null;
      }
      throw error;
    }
  }

  async stopReprocessing(): Promise<void> {
    if (!this.isReprocessing || !this.reprocessingConsumer) {
      return;
    }

    try {
      this.isReprocessing = false;
      await this.reprocessingConsumer.disconnect();
      this.reprocessingConsumer = null;
      this.logger.log('DLQ reprocessing stopped');
    } catch (error) {
      this.logger.error('Error stopping DLQ reprocessing:', error);
    }
  }

  private async reprocessMessage(
    payload: EachMessagePayload,
    headers: DlqMessageHeaders,
  ): Promise<void> {
    const { message } = payload;
    const {
      'x-handler-id': handlerId,
      'x-original-topic': originalTopic,
      'x-retry-count': retryCountStr,
    } = headers;

    // Create DLQ reprocess span if tracing is enabled
    if (isTracingEnabled()) {
      const retryCount = parseInt(retryCountStr || '0', 10);
      const dlqSpan = createDlqSpan(
        originalTopic,
        handlerId,
        retryCount,
        'reprocess',
      );

      await executeWithSpan(dlqSpan, async () => {
        await this.reprocessMessageInternal(payload, headers);
      });
    } else {
      await this.reprocessMessageInternal(payload, headers);
    }
  }

  private async reprocessMessageInternal(
    payload: EachMessagePayload,
    headers: DlqMessageHeaders,
  ): Promise<void> {
    const { message } = payload;
    const { 'x-handler-id': handlerId, 'x-original-topic': originalTopic } =
      headers;

    try {
      // Extract original message payload from DLQ message
      const dlqPayload = message.value
        ? JSON.parse(message.value.toString())
        : null;
      const originalMessage = dlqPayload?.originalMessage || {};

      // Build retry topic name (same pattern as retry service)
      const clientId = this.options.client?.clientId || 'kafka-service';
      const retryTopicName = `${clientId}.retry`;

      // Prepare headers for retry topic with reset retry count
      const retryHeaders: Record<string, string> = {
        'x-original-topic': originalTopic,
        'x-handler-id': handlerId,
        'x-retry-count': '0', // Reset retry count for fresh retry attempt
        'x-process-after': Date.now().toString(), // Process immediately
        'x-original-partition': headers['x-original-partition'] || '0',
        'x-original-offset': headers['x-original-offset'] || '0',
        'x-original-timestamp':
          headers['x-original-timestamp'] || Date.now().toString(),
      };

      // Add correlation ID if present
      if (headers['x-correlation-id']) {
        retryHeaders['x-correlation-id'] = headers['x-correlation-id'];
      }

      // Copy any other non-DLQ headers from original message
      if (originalMessage.headers) {
        for (const [key, value] of Object.entries(originalMessage.headers)) {
          if (
            !key.startsWith('x-retry-') &&
            !key.startsWith('x-original-') &&
            !key.startsWith('x-handler-') &&
            !key.startsWith('x-dlq-') &&
            !key.startsWith('x-process-after')
          ) {
            retryHeaders[key] = value ? value.toString() : '';
          }
        }
      }

      // Send to retry topic for processing
      const messageValue = originalMessage.value
        ? JSON.parse(originalMessage.value.toString())
        : null;

      await this.producer.send(retryTopicName, {
        key: originalMessage.key ? originalMessage.key.toString() : undefined,
        value: messageValue,
        headers: retryHeaders,
      });

      this.logger.debug(
        `DLQ message sent to retry topic for reprocessing - Handler: ${handlerId}, Topic: ${retryTopicName}`,
      );
    } catch (error) {
      this.logger.error(
        `Failed to send DLQ message to retry topic for handler: ${handlerId}:`,
        error,
      );
      throw error;
    }
  }

  async storeToDlq(
    message: any,
    error: Error,
    originalTopic: string,
    handlerId: string,
    retryCount: number,
    correlationId?: string,
  ): Promise<void> {
    if (!this.options.dlq?.enabled) {
      this.logger.debug('DLQ is disabled, skipping storage');
      return;
    }

    // Create DLQ span if tracing is enabled
    if (isTracingEnabled()) {
      const dlqSpan = createDlqSpan(
        originalTopic,
        handlerId,
        retryCount,
        'store',
      );

      await executeWithSpan(dlqSpan, async () => {
        await this.storeToDlqInternal(
          message,
          error,
          originalTopic,
          handlerId,
          retryCount,
          correlationId,
        );
      });
    } else {
      await this.storeToDlqInternal(
        message,
        error,
        originalTopic,
        handlerId,
        retryCount,
        correlationId,
      );
    }
  }

  private async storeToDlqInternal(
    message: any,
    error: Error,
    originalTopic: string,
    handlerId: string,
    retryCount: number,
    correlationId?: string,
  ): Promise<void> {
    try {
      const dlqHeaders: Record<string, string> = {
        'x-original-topic': originalTopic,
        'x-handler-id': handlerId,
        'x-retry-count': retryCount.toString(),
        'x-dlq-timestamp': new Date().toISOString(),
        'x-dlq-reason': error.message,
        'x-original-partition': message.partition?.toString() || '0',
        'x-original-offset': message.offset || '0',
        'x-original-timestamp': message.timestamp || Date.now().toString(),
      };

      if (correlationId) {
        dlqHeaders['x-correlation-id'] = correlationId;
      }

      // Copy original headers (excluding our internal headers)
      if (message.headers) {
        for (const [key, value] of Object.entries(message.headers)) {
          if (
            !key.startsWith('x-retry-') &&
            !key.startsWith('x-original-') &&
            !key.startsWith('x-handler-') &&
            !key.startsWith('x-dlq-')
          ) {
            dlqHeaders[key] = value ? value.toString() : '';
          }
        }
      }

      const dlqMessage = {
        key: message.key,
        value: this.buildDlqPayload(message, error),
        headers: dlqHeaders,
      };

      await this.producer.send(this.dlqTopicName, dlqMessage);

      this.metrics.messagesStored++;
      this.logger.log('Message stored to DLQ', {
        dlqTopic: this.dlqTopicName,
        originalTopic,
        handlerId,
        errorReason: error.message,
      });
    } catch (dlqError) {
      this.logger.error('Failed to store message to DLQ', {
        originalTopic,
        handlerId,
        originalError: error.message,
        dlqError: dlqError.message,
      });
      throw dlqError;
    }
  }

  private buildDlqPayload(message: any, error: Error): string {
    const payload = {
      originalMessage: {
        value: message.value,
        headers: message.headers,
        timestamp: message.timestamp,
        offset: message.offset,
        partition: message.partition,
      },
      error: {
        message: error.message,
        name: error.name,
        stack: error.stack,
        timestamp: new Date().toISOString(),
      },
      dlqMetadata: {
        processedAt: new Date().toISOString(),
        version: '1.0.0',
      },
    };

    return JSON.stringify(payload);
  }

  private extractHeaders(message: KafkaMessage): DlqMessageHeaders | null {
    if (!message.headers) {
      return null;
    }

    const requiredHeaders: (keyof DlqMessageHeaders)[] = [
      'x-original-topic',
      'x-handler-id',
      'x-dlq-timestamp',
      'x-dlq-reason',
    ];

    const headers: Partial<DlqMessageHeaders> = {};

    for (const key of requiredHeaders) {
      const value = message.headers[key];
      if (!value) {
        this.logger.warn(`Missing required DLQ header: ${key}`);
        return null;
      }
      headers[key] = value.toString();
    }

    const optionalHeaders: (keyof DlqMessageHeaders)[] = [
      'x-retry-count',
      'x-correlation-id',
      'x-original-partition',
      'x-original-offset',
      'x-original-timestamp',
    ];

    for (const key of optionalHeaders) {
      const value = message.headers[key];
      if (value) {
        headers[key] = value.toString();
      }
    }

    return headers as DlqMessageHeaders;
  }

  getDlqTopicName(): string {
    return this.dlqTopicName;
  }

  isReprocessingActive(): boolean {
    return this.isReprocessing;
  }

  getMetrics() {
    return {
      ...this.metrics,
      isReprocessing: this.isReprocessing,
      dlqTopicName: this.dlqTopicName,
    };
  }

  resetMetrics() {
    this.metrics = {
      messagesReprocessed: 0,
      messagesSkipped: 0,
      messagesStored: 0,
      errorsEncountered: 0,
      reprocessingSessions: 0,
    };
  }

  async deleteDlqTopic(): Promise<void> {
    try {
      await this.admin.connect();
      await this.admin.deleteTopics({
        topics: [this.dlqTopicName],
        timeout: 30000,
      });
      this.logger.log(`Deleted DLQ topic: ${this.dlqTopicName}`);
    } catch (error) {
      this.logger.error(
        `Failed to delete DLQ topic: ${this.dlqTopicName}`,
        error,
      );
      throw error;
    } finally {
      await this.admin.disconnect();
    }
  }

  async getDlqTopicMetadata(): Promise<any> {
    try {
      await this.admin.connect();
      const metadata = await this.admin.fetchTopicMetadata({
        topics: [this.dlqTopicName],
      });
      return metadata.topics.find((topic) => topic.name === this.dlqTopicName);
    } catch (error) {
      this.logger.error(
        `Failed to fetch DLQ topic metadata: ${this.dlqTopicName}`,
        error,
      );
      throw error;
    } finally {
      await this.admin.disconnect();
    }
  }

  async dlqTopicExists(): Promise<boolean> {
    try {
      await this.admin.connect();
      const topics = await this.admin.listTopics();
      return topics.includes(this.dlqTopicName);
    } catch (error) {
      this.logger.error(
        `Failed to check if DLQ topic exists: ${this.dlqTopicName}`,
        error,
      );
      return false;
    } finally {
      await this.admin.disconnect();
    }
  }
}
