import { Injectable, Logger, Inject } from '@nestjs/common';
import { KafkaPlugin } from '../../interfaces/kafka.interfaces';
import { KafkaProducerService } from '../../core/kafka.producer';

export interface DLQPluginOptions {
  topicPrefix?: string;
  includeErrorDetails?: boolean;
  includeOriginalMessage?: boolean;
  customDlqTopic?: string;
}

@Injectable()
export class DLQPlugin implements KafkaPlugin {
  readonly name = 'dlq';
  private readonly logger = new Logger(DLQPlugin.name);

  constructor(
    private readonly producer: KafkaProducerService,
    private readonly options: DLQPluginOptions = {},
  ) {}

  async initialize(): Promise<void> {
    this.logger.log('DLQ Plugin initialized', {
      topicPrefix: this.options.topicPrefix || 'dlq',
      includeErrorDetails: this.options.includeErrorDetails ?? true,
      includeOriginalMessage: this.options.includeOriginalMessage ?? true,
    });
  }

  async handleFailure(message: any, error: Error): Promise<void> {
    try {
      const dlqTopic =
        this.options.customDlqTopic || this.buildDlqTopicName(message);

      const dlqMessage = {
        key: message.key,
        value: this.buildDlqPayload(message, error),
        headers: {
          ...message.headers,
          'x-dlq-timestamp': new Date().toISOString(),
          'x-dlq-reason': error.message,
          'x-original-topic': message.topic || 'unknown',
        },
      };

      await this.producer.send(dlqTopic, dlqMessage);

      this.logger.log('Message sent to DLQ', {
        dlqTopic,
        originalTopic: message.topic,
        messageId: message.key || 'unknown',
        errorReason: error.message,
      });
    } catch (dlqError) {
      this.logger.error('Failed to send message to DLQ', {
        originalError: error.message,
        dlqError: dlqError.message,
      });
      // Don't throw - we don't want DLQ failures to crash the application
    }
  }

  private buildDlqTopicName(message: any): string {
    const prefix = this.options.topicPrefix || 'dlq';
    const originalTopic = message.topic || 'unknown';
    return `${prefix}.${originalTopic}`;
  }

  private buildDlqPayload(message: any, error: Error): any {
    const payload: any = {};

    if (this.options.includeOriginalMessage !== false) {
      payload.originalMessage = {
        value: message.value,
        headers: message.headers,
        timestamp: message.timestamp,
        offset: message.offset,
        partition: message.partition,
      };
    }

    if (this.options.includeErrorDetails !== false) {
      payload.error = {
        message: error.message,
        name: error.name,
        stack: error.stack,
        timestamp: new Date().toISOString(),
      };
    }

    payload.dlqMetadata = {
      processedAt: new Date().toISOString(),
      plugin: 'dlq',
      version: '1.0.0',
    };

    return payload;
  }
}
