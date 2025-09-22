import {
  Injectable,
  Logger,
  Inject,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { Consumer, Kafka, EachMessagePayload, KafkaMessage } from 'kafkajs';
import {
  KAFKAJS_INSTANCE,
  KAFKA_MODULE_OPTIONS,
} from '../core/kafka.constants';
import { KafkaModuleOptions } from '../interfaces/kafka.interfaces';
import { KafkaHandlerRegistry } from './kafka.registry';
import { KafkaRetryManager } from './kafka.retry-manager';
// Removed KafkaDelayManager - using interval-based polling instead
import { calculateRetryDelay } from '../utils/retry.utils';

export interface RetryMessageHeaders {
  'x-original-topic': string;
  'x-handler-id': string;
  'x-retry-count': string;
  'x-process-after': string; // Timestamp when message should be processed
  'x-correlation-id'?: string;
  'x-original-partition'?: string;
  'x-original-offset'?: string;
  'x-original-timestamp'?: string;
}

@Injectable()
export class KafkaRetryConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaRetryConsumer.name);
  private consumer: Consumer;
  private isRunning = false;

  // Metrics for monitoring
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
    private readonly retryManager: KafkaRetryManager,
  ) {
    // Create consumer with retry-specific groupId, excluding groupId from spread to prevent overwrite
    const baseGroupId = this.options.consumer?.groupId || 'kafka-service';
    const { groupId: _, ...consumerOptions } = this.options.consumer || {};

    this.consumer = this.kafka.consumer({
      groupId: `${baseGroupId}.retry`,
      ...consumerOptions,
    });
  }

  async onModuleInit(): Promise<void> {
    this.logger.log(
      `🔧 KafkaRetryConsumer initialized. Retry enabled: ${this.options.retry?.enabled}`,
    );

    if (this.options.retry?.enabled) {
      if (this.isRunning) {
        this.logger.log(
          'ℹ️ Retry consumer already running, skipping initialization',
        );
        return;
      }
      this.logger.log('✅ Retry is enabled, starting retry consumer...');
      await this.startRetryConsumer();
    } else {
      this.logger.log(
        'ℹ️ Retry is disabled, retry consumer will remain inactive',
      );
    }
  }

  async onModuleDestroy(): Promise<void> {
    await this.stopRetryConsumer();
  }

  private async startRetryConsumer(): Promise<void> {
    try {
      const retryTopicName = this.retryManager.getRetryTopicName();
      this.logger.log(`Starting retry consumer for topic: ${retryTopicName}`);

      await this.consumer.connect();
      await this.consumer.subscribe({
        topic: retryTopicName,
        fromBeginning: true, // Read from beginning to catch any missed retry messages
      });

      // Start continuous consumer with timestamp filtering
      await this.consumer.run({
        autoCommit: false, // Manual offset control
        eachMessage: async (payload) => {
          const { message, partition, topic } = payload;

          try {
            const headers = this.extractHeaders(message);
            if (!headers) {
              // Invalid message, commit to skip permanently
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
              // Message is ready - process it
              this.logger.log(
                `🔄 Processing ready retry message - Handler: ${headers['x-handler-id']}, Retry: ${headers['x-retry-count']}`,
              );
              await this.processReadyMessage(payload, headers);

              // Commit offset after successful processing
              await this.consumer.commitOffsets([
                {
                  topic,
                  partition,
                  offset: (parseInt(message.offset) + 1).toString(),
                },
              ]);

              this.metrics.messagesProcessed++;
            } else {
              // Message not ready - don't commit, pause briefly to avoid tight loop
              const delayRemaining = processAfter - now;
              this.logger.debug(
                `Message not ready, ${delayRemaining}ms remaining`,
              );
              this.metrics.messagesDelayed++;

              // Pause briefly to avoid tight polling loop
              await new Promise((resolve) =>
                setTimeout(resolve, Math.min(delayRemaining, 5000)),
              );
            }
          } catch (error) {
            this.metrics.errorsEncountered++;
            this.logger.error('Error processing retry message:', error);

            // Commit offset to avoid reprocessing failed message
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
      this.logger.log(`✅ Retry consumer started for topic: ${retryTopicName}`);
    } catch (error) {
      this.logger.error('❌ Failed to start retry consumer:', error);
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
    const messageId = this.createMessageId(message, partition);

    this.logger.log(
      `🚀 Processing ready retry message for handler: ${handlerId}, retry count: ${retryCount}`,
    );

    try {
      // Get the handler
      const handler = this.handlerRegistry.getHandler(handlerId);
      if (!handler) {
        this.metrics.messagesSkipped++;
        this.logger.error(`Handler not found for retry message: ${handlerId}`);
        return;
      }

      // Execute the handler
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

      // Could implement further retry logic or DLQ handling here
      // For now, we'll let the message be lost (it's already been retried)
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

    // Optional headers
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

  private createMessageId(message: KafkaMessage, partition: number): string {
    const timestamp = message.timestamp || Date.now().toString();
    const offset = message.offset || '0';
    return `${partition}-${offset}-${timestamp}`;
  }

  /**
   * Get consumer status
   */
  isRetryConsumerRunning(): boolean {
    return this.isRunning;
  }

  /**
   * Get consumer instance (for testing)
   */
  getConsumer(): Consumer {
    return this.consumer;
  }

  /**
   * Get retry consumer metrics for monitoring
   */
  getMetrics() {
    return {
      ...this.metrics,
      isRunning: this.isRunning,
    };
  }

  /**
   * Reset metrics (for testing)
   */
  resetMetrics() {
    this.metrics = {
      messagesProcessed: 0,
      messagesSkipped: 0,
      messagesDelayed: 0,
      errorsEncountered: 0,
    };
  }
}
