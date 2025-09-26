import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
  Inject,
} from '@nestjs/common';
import { Kafka, Producer, ProducerRecord, Message } from 'kafkajs';

import { KAFKAJS_INSTANCE } from './kafka.constants';
import { getOrGenerateCorrelationId } from '../utils/tracing.utils';

export interface MessagePayload {
  key?: string;
  value: any;
  headers?: Record<string, string>;
  partition?: number;
  timestamp?: string;
  /** Optional correlation ID for tracing. If not provided, one will be auto-generated. */
  correlationId?: string;
}

@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaProducerService.name);
  private producer: Producer;
  private isConnected = false;

  constructor(@Inject(KAFKAJS_INSTANCE) private readonly kafka: Kafka) {
    this.producer = this.kafka.producer();
  }

  async onModuleInit(): Promise<void> {
    // Initialize producer connection during module startup
    try {
      await this.connect();
    } catch (error) {
      this.logger.error(
        'Failed to initialize producer during module init',
        error,
      );
      // Don't throw if not required - allow startup to continue
    }
  }

  async connect(): Promise<void> {
    if (this.isConnected) {
      return;
    }

    try {
      await this.producer.connect();
      this.isConnected = true;
      this.logger.log('Kafka producer connected successfully');
    } catch (error) {
      this.logger.error('Failed to connect Kafka producer', error.stack);
      throw error;
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (this.isConnected) {
      await this.producer.disconnect();
      this.isConnected = false;
      this.logger.log('Kafka producer disconnected');
    }
  }

  async send(topic: string, message: MessagePayload): Promise<void> {
    await this.sendBatch(topic, [message]);
  }

  async sendBatch(topic: string, messages: MessagePayload[]): Promise<void> {
    if (!this.isConnected) {
      await this.connect();
    }

    const kafkaMessages: Message[] = messages.map((msg) => {
      const enhancedHeaders = { ...msg.headers };

      // Auto-generate or use provided correlation ID
      const correlationId = getOrGenerateCorrelationId(
        msg.correlationId,
        msg.headers,
      );

      // Always inject correlation ID into headers
      enhancedHeaders['x-correlation-id'] = correlationId;

      return {
        key: msg.key,
        value: this.serializeValue(msg.value),
        headers: enhancedHeaders,
        partition: msg.partition,
        timestamp: msg.timestamp,
      };
    });

    const record: ProducerRecord = {
      topic,
      messages: kafkaMessages,
    };

    try {
      this.logger.debug(
        `Sending ${messages.length} message(s) to topic: ${topic}`,
      );

      const result = await this.producer.send(record);

      this.logger.log(
        `Successfully sent ${messages.length} message(s) to topic: ${topic}`,
        {
          topic,
          messageCount: messages.length,
          partitions: result.map((r) => r.partition),
        },
      );
    } catch (error) {
      this.logger.error(
        `Failed to send message(s) to topic: ${topic}`,
        error.stack,
        { topic, messageCount: messages.length },
      );
      throw error;
    }
  }

  async sendTransaction(
    records: Array<{ topic: string; messages: MessagePayload[] }>,
  ): Promise<void> {
    if (!this.isConnected) {
      await this.connect();
    }

    const transaction = await this.producer.transaction();

    try {
      for (const record of records) {
        const kafkaMessages: Message[] = record.messages.map((msg) => {
          const enhancedHeaders = { ...msg.headers };

          // Auto-generate or use provided correlation ID
          const correlationId = getOrGenerateCorrelationId(
            msg.correlationId,
            msg.headers,
          );

          // Always inject correlation ID into headers
          enhancedHeaders['x-correlation-id'] = correlationId;

          return {
            key: msg.key,
            value: this.serializeValue(msg.value),
            headers: enhancedHeaders,
            partition: msg.partition,
            timestamp: msg.timestamp,
          };
        });

        await transaction.send({
          topic: record.topic,
          messages: kafkaMessages,
        });
      }

      await transaction.commit();

      this.logger.log(
        `Successfully sent transactional messages to ${records.length} topic(s)`,
        { topics: records.map((r) => r.topic) },
      );
    } catch (error) {
      await transaction.abort();
      this.logger.error('Failed to send transactional messages', error.stack);
      throw error;
    }
  }

  private serializeValue(value: any): string {
    if (typeof value === 'string') {
      return value;
    }
    return JSON.stringify(value);
  }

  async isReady(): Promise<boolean> {
    return this.isConnected;
  }
}
