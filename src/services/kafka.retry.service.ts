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
import { KafkaDlqService } from './kafka.dlq.service';
import { KafkaProducerService } from '../core/kafka.producer';
import { calculateRetryDelay } from '../utils/retry.utils';
import { KafkaRetryOptions } from '../interfaces/kafka.interfaces';

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

export interface RetryContext {
  originalMessage: KafkaMessage;
  topic: string;
  attempt: number;
  error: Error;
  timestamp: Date;
  correlationId?: string;
}

export interface RetryConfiguration {
  enabled: boolean;
  maxAttempts: number;
  strategy: 'exponential' | 'linear' | 'fixed';
  initialDelay: number;
  maxDelay: number;
  multiplier: number;
}

export interface RetryDecision {
  shouldRetry: boolean;
  nextAttempt?: number;
  delay?: number;
  reason: string;
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
    private readonly dlqService: KafkaDlqService,
    private readonly producer: KafkaProducerService,
  ) {
    this.admin = this.kafka.admin();
    this.retryTopicName = this.buildRetryTopicName();

    // Use client ID as base for retry consumer group to avoid conflicts
    const baseGroupId = this.options.client?.clientId || 'kafka-service';

    // Configure consumer with polling intervals for better retry handling
    this.consumer = this.kafka.consumer({
      groupId: `${baseGroupId}.retry`,
      sessionTimeout: 30000, // 30 seconds
      heartbeatInterval: 3000, // 3 seconds heartbeat
      maxWaitTimeInMs: 2000, // Wait up to 2 seconds for new messages before returning
    });
  }

  async onModuleInit(): Promise<void> {
    // Only ensure topic exists - don't start consumer here
    // Bootstrap service will handle starting the consumer
    await this.ensureRetryTopicExists();
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
      };

      // Only add configEntries if explicitly configured
      // This allows managed Kafka services (like Redpanda Cloud) to use their defaults
      const configEntries: { name: string; value: string }[] = [];

      if (this.options.retry?.topicRetentionMs !== undefined) {
        configEntries.push({
          name: 'retention.ms',
          value: String(this.options.retry.topicRetentionMs),
        });
      }

      if (this.options.retry?.topicSegmentMs !== undefined) {
        configEntries.push({
          name: 'segment.ms',
          value: String(this.options.retry.topicSegmentMs),
        });
      }

      // Always set cleanup.policy if we have any config entries
      if (configEntries.length > 0) {
        configEntries.unshift({
          name: 'cleanup.policy',
          value: 'delete',
        });
        topicConfig.configEntries = configEntries;
      }

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

  async startRetryConsumer(): Promise<void> {
    try {
      this.logger.log(
        `Starting retry consumer for topic: ${this.retryTopicName}`,
      );

      await this.consumer.connect();
      await this.consumer.subscribe({
        topic: this.retryTopicName,
        fromBeginning: true,
      });

      // Start the polling loop with controlled intervals
      this.startPollingLoop();

      this.isRunning = true;
      this.logger.log(
        `Retry consumer started for topic: ${this.retryTopicName}`,
      );
    } catch (error) {
      this.logger.error('Failed to start retry consumer:', error);
      throw error;
    }
  }

  private async startPollingLoop(): Promise<void> {
    const pollInterval = 5000; // Poll every 5 seconds
    const batchTimeout = 2000; // Wait up to 2 seconds for messages in each poll

    // Process messages in controlled batches
    await this.consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: 1, // Process one partition at a time for better control
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        this.logger.debug(
          `Processing batch of ${batch.messages.length} messages`,
        );

        for (const message of batch.messages) {
          try {
            const headers = this.extractHeaders(message);
            if (!headers) {
              this.logger.warn('Skipping retry message with invalid headers');
              resolveOffset(message.offset);
              this.metrics.messagesSkipped++;
              continue;
            }

            const processAfter = Math.floor(
              parseFloat(headers['x-process-after']),
            );
            const now = Date.now();

            if (now >= processAfter) {
              this.logger.log(
                `Processing ready retry message - Handler: ${headers['x-handler-id']}, Retry: ${headers['x-retry-count']}`,
              );

              await this.processReadyMessage(
                message,
                batch.topic,
                batch.partition,
                headers,
              );
              resolveOffset(message.offset);
              this.metrics.messagesProcessed++;
            } else {
              const delayRemaining = processAfter - now;
              this.logger.debug(
                `Message not ready, ${delayRemaining}ms remaining - requeuing for next poll`,
              );

              // Requeue the message to retry topic with same headers
              await this.producer.send(this.retryTopicName, {
                key: message.key ? message.key.toString() : undefined,
                value: message.value,
                headers: headers as unknown as Record<string, string>,
              });

              // Resolve offset since we've handled it by requeuing
              resolveOffset(message.offset);
              this.metrics.messagesDelayed++;
              // Continue processing other messages instead of breaking
            }

            // Send heartbeat to keep the consumer alive during long processing
            await heartbeat();
          } catch (error) {
            this.metrics.errorsEncountered++;
            this.logger.error('Error processing retry message:', error);

            // Resolve offset even on error to avoid infinite retries of broken messages
            resolveOffset(message.offset);
          }
        }

        // Add controlled delay between poll cycles
        if (this.isRunning) {
          this.logger.debug(`Waiting ${pollInterval}ms before next poll cycle`);
          await new Promise((resolve) => setTimeout(resolve, pollInterval));
        }
      },
    });
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
    message: any,
    topic: string,
    partition: number,
    headers: RetryMessageHeaders,
  ): Promise<void> {
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

      await this.executeHandlerWithRetry(handler, message, retryCount, headers);
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
    headers: RetryMessageHeaders,
  ): Promise<void> {
    try {
      let payload;
      if (message.value) {
        if (Buffer.isBuffer(message.value)) {
          payload = JSON.parse(message.value.toString());
        } else if (typeof message.value === 'string') {
          payload = JSON.parse(message.value);
        } else {
          payload = message.value;
        }
      } else {
        payload = null;
      }
      await this.handlerRegistry.executeHandler(handler.handlerId, payload);

      this.logger.debug(
        `Retry successful for handler: ${handler.handlerId} after ${currentRetryCount} retries`,
      );
    } catch (error) {
      this.logger.error(
        `Retry execution failed for handler: ${handler.handlerId}:`,
        error,
      );

      // Check if this is the final retry attempt and DLQ is enabled
      const handlerConfig = this.handlerRegistry.getHandler(handler.handlerId);
      if (handlerConfig) {
        const maxRetries = handlerConfig.metadata.options?.retry?.attempts || 0;

        if (currentRetryCount >= maxRetries) {
          // Max retries exceeded - send to DLQ if enabled
          if (handlerConfig.metadata.options?.dlq?.enabled) {
            this.logger.log('Max retries exceeded, sending to DLQ', {
              handlerId: handler.handlerId,
              retryCount: currentRetryCount,
              maxRetries,
            });

            try {
              await this.dlqService.storeToDlq(
                message,
                error,
                headers['x-original-topic'],
                handler.handlerId,
                currentRetryCount,
                headers['x-correlation-id'],
              );

              this.logger.log('Message sent to centralized DLQ', {
                dlqTopic: this.dlqService.getDlqTopicName(),
                originalTopic: headers['x-original-topic'],
                handlerId: handler.handlerId,
                errorReason: error.message,
              });
            } catch (dlqError) {
              this.logger.error('Failed to send message to centralized DLQ', {
                originalTopic: headers['x-original-topic'],
                originalError: error.message,
                dlqError: dlqError.message,
              });
            }
          } else {
            this.logger.warn('Max retries exceeded but DLQ not enabled', {
              handlerId: handler.handlerId,
              retryCount: currentRetryCount,
              maxRetries,
            });
          }
        } else {
          // Retry limit not exceeded - schedule another retry
          this.logger.log('Scheduling additional retry', {
            handlerId: handler.handlerId,
            retryCount: currentRetryCount,
            nextRetryCount: currentRetryCount + 1,
            maxRetries,
          });

          try {
            await this.scheduleAdditionalRetry(
              message,
              headers,
              error,
              currentRetryCount + 1,
              handlerConfig.metadata.options,
            );
          } catch (scheduleError) {
            this.logger.error('Failed to schedule additional retry', {
              handlerId: handler.handlerId,
              originalError: error.message,
              scheduleError: scheduleError.message,
            });
          }
        }
      }
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

  /**
   * Schedule an additional retry when a retry attempt fails
   * This replicates the RetryInterceptor logic for retry attempts
   */
  private async scheduleAdditionalRetry(
    message: KafkaMessage,
    headers: RetryMessageHeaders,
    _error: Error,
    nextRetryCount: number,
    options: any,
  ): Promise<void> {
    // Calculate delay for next retry
    const delay = calculateRetryDelay({
      attempt: nextRetryCount,
      baseDelay: options.retry?.baseDelay || 1000,
      maxDelay: options.retry?.maxDelay || 30000,
      backoff: options.retry?.backoff || 'exponential',
      jitter: true,
    });

    const processAfter = Date.now() + delay;

    // Create retry headers for the next attempt
    const retryHeaders: Record<string, string> = {
      'x-original-topic': headers['x-original-topic'],
      'x-handler-id': headers['x-handler-id'],
      'x-retry-count': nextRetryCount.toString(),
      'x-process-after': processAfter.toString(),
      'x-original-partition': headers['x-original-partition'] || '0',
      'x-original-offset': headers['x-original-offset'] || message.offset,
      'x-original-timestamp':
        headers['x-original-timestamp'] ||
        message.timestamp ||
        Date.now().toString(),
    };

    if (headers['x-correlation-id']) {
      retryHeaders['x-correlation-id'] = headers['x-correlation-id'];
    }

    // Copy original headers (excluding retry headers)
    if (message.headers) {
      for (const [key, value] of Object.entries(message.headers)) {
        if (
          !key.startsWith('x-retry-') &&
          !key.startsWith('x-original-') &&
          !key.startsWith('x-dlq-')
        ) {
          retryHeaders[key] = value ? value.toString() : '';
        }
      }
    }

    // Send to retry topic for the next attempt
    const producer = this.kafka.producer();
    await producer.connect();

    try {
      await producer.send({
        topic: this.retryTopicName,
        messages: [
          {
            key: message.key,
            value: message.value,
            headers: retryHeaders,
          },
        ],
      });

      this.logger.log('Additional retry scheduled successfully', {
        nextRetryCount,
        processAfter: new Date(processAfter).toISOString(),
        delay,
      });
    } finally {
      await producer.disconnect();
    }
  }

  // ===== Utility Methods (merged from KafkaRetryLogicService) =====

  /**
   * Determine if a message should be retried based on attempt count and configuration
   */
  shouldRetry(attempt: number, config: KafkaRetryOptions): RetryDecision {
    const maxAttempts = config.attempts || 3;

    if (!config.enabled) {
      return {
        shouldRetry: false,
        reason: 'Retry is disabled in configuration',
      };
    }

    if (attempt >= maxAttempts) {
      return {
        shouldRetry: false,
        reason: `Max attempts reached (${attempt}/${maxAttempts})`,
      };
    }

    const nextAttempt = attempt + 1;
    const delay = this.calculateDelay(nextAttempt, config);

    return {
      shouldRetry: true,
      nextAttempt,
      delay,
      reason: `Retry attempt ${nextAttempt}/${maxAttempts}`,
    };
  }

  /**
   * Calculate delay for a retry attempt
   */
  calculateDelay(attempt: number, config: KafkaRetryOptions): number {
    const {
      backoff = 'exponential',
      baseDelay = 1000,
      maxDelay = 30000,
    } = config;

    return calculateRetryDelay({
      attempt,
      baseDelay,
      maxDelay,
      backoff,
      jitter: true,
    });
  }

  /**
   * Create a retry context from a failed message processing attempt
   */
  createRetryContext(
    message: KafkaMessage,
    topic: string,
    attempt: number,
    error: Error,
    correlationId?: string,
  ): RetryContext {
    return {
      originalMessage: message,
      topic,
      attempt,
      error,
      timestamp: new Date(),
      correlationId,
    };
  }

  /**
   * Get retry configuration with defaults merged from module options
   */
  getRetryConfiguration(
    overrides?: Partial<KafkaRetryOptions>,
  ): RetryConfiguration {
    const defaults = this.options.retry || {};
    const merged = { ...defaults, ...overrides };

    return {
      enabled: merged.enabled || false,
      maxAttempts: merged.attempts || 3,
      strategy: merged.backoff || 'exponential',
      initialDelay: merged.baseDelay || 1000,
      maxDelay: merged.maxDelay || 30000,
      multiplier: 2, // Fixed multiplier for exponential backoff
    };
  }

  /**
   * Extract retry count from message headers
   */
  extractRetryCount(headers: Record<string, any> = {}): number {
    const retryCount = headers['x-retry-count'];
    if (retryCount) {
      const parsed = parseInt(String(retryCount), 10);
      return isNaN(parsed) ? 0 : parsed;
    }
    return 0;
  }

  /**
   * Extract correlation ID from message headers
   */
  extractCorrelationId(headers: Record<string, any> = {}): string | undefined {
    return headers['x-correlation-id'] || headers['correlationId'];
  }

  /**
   * Check if a retry message is ready to be processed based on timestamp
   */
  isRetryReady(headers: Record<string, any> = {}): boolean {
    const processAfter = headers['x-process-after'];
    if (!processAfter) {
      // No delay specified, process immediately
      return true;
    }

    const processAfterTime = parseInt(String(processAfter), 10);
    if (isNaN(processAfterTime)) {
      this.logger.warn(
        'Invalid x-process-after header, processing immediately',
      );
      return true;
    }

    return Date.now() >= processAfterTime;
  }

  /**
   * Calculate remaining delay time for a retry message
   */
  getRemainingDelay(headers: Record<string, any> = {}): number {
    const processAfter = headers['x-process-after'];
    if (!processAfter) {
      return 0;
    }

    const processAfterTime = parseInt(String(processAfter), 10);
    if (isNaN(processAfterTime)) {
      return 0;
    }

    const remaining = processAfterTime - Date.now();
    return Math.max(remaining, 0);
  }

  /**
   * Get retry topic name for a given original topic
   */
  getRetryTopicNameForTopic(originalTopic: string): string {
    return `${originalTopic}.retry`;
  }

  /**
   * Validate retry configuration
   */
  validateRetryConfig(config: KafkaRetryOptions): {
    valid: boolean;
    errors: string[];
  } {
    const errors: string[] = [];

    if (config.enabled && config.attempts && config.attempts < 1) {
      errors.push('Retry attempts must be >= 1 when retry is enabled');
    }

    if (config.baseDelay && config.baseDelay < 0) {
      errors.push('Base delay must be >= 0');
    }

    if (config.maxDelay && config.maxDelay < 0) {
      errors.push('Max delay must be >= 0');
    }

    if (
      config.baseDelay &&
      config.maxDelay &&
      config.baseDelay > config.maxDelay
    ) {
      errors.push('Base delay cannot be greater than max delay');
    }

    return {
      valid: errors.length === 0,
      errors,
    };
  }

  /**
   * Get retry statistics for monitoring
   */
  getRetryStats(): {
    defaultConfig: RetryConfiguration;
    globalRetryTopic: string;
    metrics: any;
  } {
    return {
      defaultConfig: this.getRetryConfiguration(),
      globalRetryTopic: this.getRetryTopicName(),
      metrics: this.getMetrics(),
    };
  }
}
