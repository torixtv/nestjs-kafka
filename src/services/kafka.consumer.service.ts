import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
  Inject,
} from '@nestjs/common';
import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';

import {
  KAFKAJS_INSTANCE,
  KAFKA_MODULE_OPTIONS,
} from '../core/kafka.constants';
import { KafkaModuleOptions } from '../interfaces/kafka.interfaces';
import { KafkaHandlerRegistry, RegisteredHandler } from './kafka.registry';
import { calculateRetryDelay } from '../utils/retry.utils';
import { KafkaRetryService } from './kafka.retry.service';
import { KafkaDlqService } from './kafka.dlq.service';
import { KafkaProducerService } from '../core/kafka.producer';

@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumerService.name);
  private consumer: Consumer;
  private isConnected = false;

  constructor(
    @Inject(KAFKAJS_INSTANCE) private readonly kafka: Kafka,
    @Inject(KAFKA_MODULE_OPTIONS) private readonly options: KafkaModuleOptions,
    private readonly handlerRegistry: KafkaHandlerRegistry,
    private readonly retryService: KafkaRetryService,
    private readonly dlqService: KafkaDlqService,
    private readonly producer: KafkaProducerService,
  ) {
    if (!this.options.consumer) {
      throw new Error('Consumer configuration is required');
    }

    this.consumer = this.kafka.consumer(this.options.consumer);
  }

  async onModuleInit(): Promise<void> {
    // Don't auto-start consumer - let bootstrap service handle this
    this.logger.log('Consumer service initialized');
  }

  async onModuleDestroy(): Promise<void> {
    if (this.isConnected) {
      await this.disconnect();
    }
  }

  async connect(): Promise<void> {
    if (this.isConnected) {
      return;
    }

    try {
      await this.consumer.connect();
      this.isConnected = true;
      this.logger.log('Kafka consumer connected successfully');
    } catch (error) {
      this.logger.error('Failed to connect Kafka consumer', error.stack);
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    if (!this.isConnected) {
      return;
    }

    try {
      await this.consumer.disconnect();
      this.isConnected = false;
      this.logger.log('Kafka consumer disconnected');
    } catch (error) {
      this.logger.error('Failed to disconnect Kafka consumer', error.stack);
    }
  }

  async subscribe(): Promise<void> {
    if (!this.isConnected) {
      await this.connect();
    }

    if (!this.options.subscriptions) {
      this.logger.warn('No subscriptions configured');
      return;
    }

    const { topics, fromBeginning } = this.options.subscriptions;

    if (!topics || topics.length === 0) {
      this.logger.warn('No topics configured for subscription');
      return;
    }

    for (const topic of topics) {
      await this.consumer.subscribe({
        topic,
        fromBeginning: fromBeginning ?? false,
      });
      this.logger.log(`Subscribed to topic: ${topic}`);
    }
  }

  async startConsumer(): Promise<void> {
    await this.consumer.run({
      eachMessage: async (payload: EachMessagePayload) => {
        await this.handleMessage(payload);
      },
    });
    this.logger.log('Consumer started and processing messages');
  }

  private async handleMessage(payload: EachMessagePayload): Promise<void> {
    const { topic, partition, message } = payload;

    try {
      const handler = this.handlerRegistry.getHandlerByTopic(topic);

      if (!handler) {
        this.logger.warn(`No handler found for topic: ${topic}`);
        return;
      }

      // Parse message
      const parsedMessage = {
        key: message.key?.toString(),
        value: this.parseMessageValue(message.value),
        headers: this.parseHeaders(message.headers),
        timestamp: message.timestamp,
        partition,
        offset: message.offset,
      };

      // Get retry/DLQ configuration from handler metadata
      const retryOptions = handler.metadata?.options?.retry;
      const dlqOptions = handler.metadata?.options?.dlq;

      try {
        // Execute handler - pass only the value to match handler expectations
        await handler.method.call(handler.instance, parsedMessage.value);
        this.logger.debug(`Successfully processed message from ${topic}:${partition}:${message.offset}`);
      } catch (error) {
        // Handle error with retry logic
        await this.handleMessageError(
          error,
          message,
          topic,
          partition,
          handler,
          retryOptions,
          dlqOptions,
        );
      }
    } catch (error) {
      this.logger.error(
        `Failed to process message from ${topic}:${partition}:${message.offset}`,
        error.stack
      );
      // Don't throw - message is handled
    }
  }

  private async handleMessageError(
    error: Error,
    message: any,
    topic: string,
    partition: number,
    handler: RegisteredHandler,
    retryOptions: any,
    dlqOptions: any,
  ): Promise<void> {
    const retryCount = this.getRetryCountFromHeaders(message.headers);

    this.logger.warn(`Message processing failed`, {
      topic,
      partition,
      offset: message.offset,
      retryCount,
      error: error.message,
    });

    if (retryOptions?.enabled) {
      if (retryCount < retryOptions.attempts) {
        // Calculate delay
        const delay = calculateRetryDelay({
          attempt: retryCount + 1,
          baseDelay: retryOptions.baseDelay,
          maxDelay: retryOptions.maxDelay,
          backoff: retryOptions.backoff,
          jitter: true,
        });

        // Schedule retry
        await this.scheduleRetry(
          message,
          topic,
          partition,
          handler.handlerId,
          retryCount + 1,
          delay,
        );

        this.logger.log(`Scheduled retry ${retryCount + 1} with ${delay}ms delay`);
      } else if (dlqOptions?.enabled) {
        // Max retries exceeded - send to DLQ
        await this.dlqService.storeToDlq(
          message,
          error,
          topic,
          handler.handlerId,
          retryCount,
          this.getCorrelationId(message.headers),
        );

        this.logger.error(`Max retries exceeded, sent to DLQ`, {
          topic,
          handlerId: handler.handlerId,
          retryCount,
        });
      }
    } else {
      // No retry configured, just log
      this.logger.error(`Handler failed without retry`, error.stack);
    }
  }

  private async scheduleRetry(
    message: any,
    originalTopic: string,
    partition: number,
    handlerId: string,
    retryCount: number,
    delayMs: number,
  ): Promise<void> {
    const retryTopicName = this.retryService.getRetryTopicName();
    const processAfter = Date.now() + delayMs;

    const retryHeaders: Record<string, string> = {
      'x-original-topic': originalTopic,
      'x-handler-id': handlerId,
      'x-retry-count': retryCount.toString(),
      'x-process-after': processAfter.toString(),
      'x-original-partition': partition.toString(),
      'x-original-offset': message.offset || '0',
      'x-original-timestamp': message.timestamp || Date.now().toString(),
    };

    // Preserve correlation ID if exists
    const correlationId = this.getCorrelationId(message.headers);
    if (correlationId) {
      retryHeaders['x-correlation-id'] = correlationId;
    }

    // Copy original headers (excluding retry headers)
    if (message.headers) {
      for (const [key, value] of Object.entries(message.headers)) {
        if (!key.startsWith('x-retry-') &&
            !key.startsWith('x-original-') &&
            !key.startsWith('x-handler-')) {
          retryHeaders[key] = value ? value.toString() : '';
        }
      }
    }

    await this.producer.send(retryTopicName, {
      key: message.key,
      value: message.value,
      headers: retryHeaders,
    });
  }

  private parseMessageValue(value: Buffer | null): any {
    if (!value) return null;

    try {
      return JSON.parse(value.toString());
    } catch {
      return value.toString();
    }
  }

  private parseHeaders(headers: any): Record<string, string> {
    if (!headers) return {};

    const parsed: Record<string, string> = {};
    for (const [key, value] of Object.entries(headers)) {
      if (Buffer.isBuffer(value)) {
        parsed[key] = value.toString();
      } else {
        parsed[key] = String(value);
      }
    }
    return parsed;
  }

  private getRetryCountFromHeaders(headers: any): number {
    if (!headers || !headers['x-retry-count']) {
      return 0;
    }
    const value = headers['x-retry-count'];
    const count = parseInt(Buffer.isBuffer(value) ? value.toString() : value, 10);
    return isNaN(count) ? 0 : count;
  }

  private getCorrelationId(headers: any): string | undefined {
    if (!headers || !headers['x-correlation-id']) {
      return undefined;
    }
    const value = headers['x-correlation-id'];
    return Buffer.isBuffer(value) ? value.toString() : value;
  }
}