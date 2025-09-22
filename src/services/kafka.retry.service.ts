import {
  Injectable,
  Logger,
  Inject,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { Consumer, Kafka, Admin, ITopicConfig, EachMessagePayload, KafkaMessage } from 'kafkajs';
import {
  KAFKAJS_INSTANCE,
  KAFKA_MODULE_OPTIONS,
} from '../core/kafka.constants';
import { KafkaModuleOptions } from '../interfaces/kafka.interfaces';
import { KafkaHandlerRegistry } from './kafka.registry';
import { calculateRetryDelay } from '../utils/retry.utils';

export interface RetryMessageHeaders {
  'x-original-topic': string;
  'x-handler-id': string;
  'x-retry-count': string;
  'x-process-after': string;
  'x-correlation-id'?: string;
  'x-original-partition'?: string;
  'x-original-offset'?: string;
  'x-original-timestamp'?: string;
}

@Injectable()
export class KafkaRetryService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaRetryService.name);
  private admin: Admin;
  private consumer: Consumer;
  private retryTopicName: string;
  private isRunning = false;

  private metrics = {
    messagesProcessed: 0,
    messagesSkipped: 0,
    messagesDelayed: 0,
    errorsEncountered: 0,
  };

  constructor(
    @Inject(KAFKAJS_INSTANCE) private readonly kafka: Kafka,
    @Inject(KAFKA_MODULE_OPTIONS) private readonly options: KafkaModuleOptions,
    private readonly handlerRegistry: KafkaHandlerRegistry,
  ) {
    this.admin = this.kafka.admin();
    this.retryTopicName = this.buildRetryTopicName();

    const baseGroupId = this.options.consumer?.groupId || 'kafka-service';
    const { groupId: _, ...consumerOptions } = this.options.consumer || {};

    this.consumer = this.kafka.consumer({
      groupId: `${baseGroupId}.retry`,
      ...consumerOptions,
    });
  }

  async onModuleInit(): Promise<void> {
    await this.ensureRetryTopicExists();

    if (this.options.retry?.enabled) {
      this.logger.log('Retry is enabled, starting retry consumer...');
      await this.startRetryConsumer();
    } else {
      this.logger.log('Retry is disabled, retry service will remain inactive');
    }
  }

  async onModuleDestroy(): Promise<void> {
    await this.stopRetryConsumer();
    await this.admin.disconnect();
  }

  private buildRetryTopicName(): string {
    const clientId = this.options.client?.clientId || 'kafka-service';
    return `${clientId}.retry`;
  }

  private async ensureRetryTopicExists(): Promise<void> {
    try {
      await this.admin.connect();

      const existingTopics = await this.admin.listTopics();

      if (existingTopics.includes(this.retryTopicName)) {
        this.logger.debug(`Retry topic already exists: ${this.retryTopicName}`);
        return;
      }

      const topicConfig: ITopicConfig = {
        topic: this.retryTopicName,
        numPartitions: this.options.retry?.topicPartitions || 3,
        replicationFactor: this.options.retry?.topicReplicationFactor || 1,
        configEntries: [
          {
            name: 'cleanup.policy',
            value: 'delete',
          },
          {
            name: 'retention.ms',
            value: String(
              this.options.retry?.topicRetentionMs || 24 * 60 * 60 * 1000,
            ),
          },
          {
            name: 'segment.ms',
            value: String(this.options.retry?.topicSegmentMs || 60 * 60 * 1000),
          },
        ],
      };

      await this.admin.createTopics({
        topics: [topicConfig],
        waitForLeaders: true,
      });

      this.logger.log(`Created retry topic: ${this.retryTopicName}`);
    } catch (error) {
      this.logger.error(
        `Failed to ensure retry topic exists: ${this.retryTopicName}`,
        error,
      );
      throw error;
    } finally {
      await this.admin.disconnect();
    }
  }

  private async startRetryConsumer(): Promise<void> {
    try {
      this.logger.log(`Starting retry consumer for topic: ${this.retryTopicName}`);

      await this.consumer.connect();
      await this.consumer.subscribe({
        topic: this.retryTopicName,
        fromBeginning: true,
      });

      await this.consumer.run({
        autoCommit: false,
        eachMessage: async (payload) => {
          const { message, partition, topic } = payload;

          try {
            const headers = this.extractHeaders(message);
            if (!headers) {
              await this.consumer.commitOffsets([
                {
                  topic,
                  partition,
                  offset: (parseInt(message.offset) + 1).toString(),
                },
              ]);
              this.metrics.messagesSkipped++;
              return;
            }

            const processAfter = Math.floor(
              parseFloat(headers['x-process-after']),
            );
            const now = Date.now();

            if (now >= processAfter) {
              this.logger.log(
                `Processing ready retry message - Handler: ${headers['x-handler-id']}, Retry: ${headers['x-retry-count']}`,
              );
              await this.processReadyMessage(payload, headers);

              await this.consumer.commitOffsets([
                {
                  topic,
                  partition,
                  offset: (parseInt(message.offset) + 1).toString(),
                },
              ]);

              this.metrics.messagesProcessed++;
            } else {
              const delayRemaining = processAfter - now;
              this.logger.debug(
                `Message not ready, ${delayRemaining}ms remaining`,
              );
              this.metrics.messagesDelayed++;

              await new Promise((resolve) =>
                setTimeout(resolve, Math.min(delayRemaining, 5000)),
              );
            }
          } catch (error) {
            this.metrics.errorsEncountered++;
            this.logger.error('Error processing retry message:', error);

            await this.consumer.commitOffsets([
              {
                topic,
                partition,
                offset: (parseInt(message.offset) + 1).toString(),
              },
            ]);
          }
        },
      });

      this.isRunning = true;
      this.logger.log(`Retry consumer started for topic: ${this.retryTopicName}`);
    } catch (error) {
      this.logger.error('Failed to start retry consumer:', error);
      throw error;
    }
  }

  private async stopRetryConsumer(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    try {
      this.isRunning = false;
      await this.consumer.disconnect();
      this.logger.log('Retry consumer stopped');
    } catch (error) {
      this.logger.error('Error stopping retry consumer:', error);
    }
  }

  private async processReadyMessage(
    payload: EachMessagePayload,
    headers: RetryMessageHeaders,
  ): Promise<void> {
    const { message, partition } = payload;
    const { 'x-handler-id': handlerId, 'x-retry-count': retryCountStr } =
      headers;

    const retryCount = parseInt(retryCountStr, 10);

    try {
      const handler = this.handlerRegistry.getHandler(handlerId);
      if (!handler) {
        this.metrics.messagesSkipped++;
        this.logger.error(`Handler not found for retry message: ${handlerId}`);
        return;
      }

      await this.executeHandlerWithRetry(handler, message, retryCount);
    } catch (error) {
      this.metrics.errorsEncountered++;
      this.logger.error(
        `Error processing ready retry message: ${handlerId}`,
        error,
      );
    }
  }

  private async executeHandlerWithRetry(
    handler: any,
    message: KafkaMessage,
    currentRetryCount: number,
  ): Promise<void> {
    try {
      const payload = message.value
        ? JSON.parse(message.value.toString())
        : null;
      await this.handlerRegistry.executeHandler(handler.handlerId, payload);

      this.logger.debug(
        `Retry successful for handler: ${handler.handlerId} after ${currentRetryCount} retries`,
      );
    } catch (error) {
      this.logger.error(
        `Retry execution failed for handler: ${handler.handlerId}:`,
        error,
      );
    }
  }

  private extractHeaders(message: KafkaMessage): RetryMessageHeaders | null {
    if (!message.headers) {
      return null;
    }

    const requiredHeaders: (keyof RetryMessageHeaders)[] = [
      'x-original-topic',
      'x-handler-id',
      'x-retry-count',
      'x-process-after',
    ];

    const headers: Partial<RetryMessageHeaders> = {};

    for (const key of requiredHeaders) {
      const value = message.headers[key];
      if (!value) {
        this.logger.warn(`Missing required header: ${key}`);
        return null;
      }
      headers[key] = value.toString();
    }

    const optionalHeaders: (keyof RetryMessageHeaders)[] = [
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

    return headers as RetryMessageHeaders;
  }

  getRetryTopicName(): string {
    return this.retryTopicName;
  }

  isRetryConsumerRunning(): boolean {
    return this.isRunning;
  }

  getMetrics() {
    return {
      ...this.metrics,
      isRunning: this.isRunning,
    };
  }

  resetMetrics() {
    this.metrics = {
      messagesProcessed: 0,
      messagesSkipped: 0,
      messagesDelayed: 0,
      errorsEncountered: 0,
    };
  }

  async deleteRetryTopic(): Promise<void> {
    try {
      await this.admin.connect();
      await this.admin.deleteTopics({
        topics: [this.retryTopicName],
        timeout: 30000,
      });
      this.logger.log(`Deleted retry topic: ${this.retryTopicName}`);
    } catch (error) {
      this.logger.error(
        `Failed to delete retry topic: ${this.retryTopicName}`,
        error,
      );
      throw error;
    } finally {
      await this.admin.disconnect();
    }
  }

  async getTopicMetadata(): Promise<any> {
    try {
      await this.admin.connect();
      const metadata = await this.admin.fetchTopicMetadata({
        topics: [this.retryTopicName],
      });
      return metadata.topics.find(
        (topic) => topic.name === this.retryTopicName,
      );
    } catch (error) {
      this.logger.error(
        `Failed to fetch topic metadata: ${this.retryTopicName}`,
        error,
      );
      throw error;
    } finally {
      await this.admin.disconnect();
    }
  }

  async retryTopicExists(): Promise<boolean> {
    try {
      await this.admin.connect();
      const topics = await this.admin.listTopics();
      return topics.includes(this.retryTopicName);
    } catch (error) {
      this.logger.error(
        `Failed to check if retry topic exists: ${this.retryTopicName}`,
        error,
      );
      return false;
    } finally {
      await this.admin.disconnect();
    }
  }
}