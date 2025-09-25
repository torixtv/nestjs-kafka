import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
  Logger,
  Inject,
} from '@nestjs/common';
import { Observable, throwError } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { Reflector } from '@nestjs/core';
import { KafkaContext } from '@nestjs/microservices';

import { EVENT_HANDLER_METADATA } from '../core/kafka.constants';
import {
  calculateRetryDelay,
  getRetryCountFromHeaders,
  getCorrelationIdFromHeaders,
  createMessageId,
} from '../utils/retry.utils';
import { KafkaProducerService } from '../core/kafka.producer';
import { KafkaRetryService } from '../services/kafka.retry.service';
import { KafkaDlqService } from '../services/kafka.dlq.service';
import { KafkaHandlerRegistry } from '../services/kafka.registry';

@Injectable()
export class RetryInterceptor implements NestInterceptor {
  private readonly logger = new Logger(RetryInterceptor.name);

  constructor(
    private readonly reflector: Reflector,
    private readonly producer: KafkaProducerService,
    private readonly retryService: KafkaRetryService,
    private readonly dlqService: KafkaDlqService,
    private readonly handlerRegistry: KafkaHandlerRegistry,
  ) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    // Only apply to RPC/microservice contexts (Kafka messages)
    if (context.getType() !== 'rpc') {
      return next.handle();
    }

    const handler = context.getHandler();
    const target = context.getClass();

    // Get retry options from metadata
    const handlerMetadata = this.reflector.get(EVENT_HANDLER_METADATA, handler);

    if (!handlerMetadata?.options?.retry?.enabled) {
      // No retry configuration, just execute normally
      return next.handle();
    }

    this.logger.debug('RetryInterceptor activated for handler', {
      pattern: handlerMetadata.pattern,
      retryEnabled: handlerMetadata.options.retry.enabled,
      dlqEnabled: handlerMetadata.options.dlq.enabled,
    });

    return this.executeWithRetry(context, next, handlerMetadata);
  }

  private executeWithRetry(
    context: ExecutionContext,
    next: CallHandler,
    handlerMetadata: any,
  ): Observable<any> {
    const kafkaContext = context.switchToRpc().getContext<KafkaContext>();
    const message = kafkaContext.getMessage();
    const partition = kafkaContext.getPartition();
    const topic = kafkaContext.getTopic();

    const retryOptions = handlerMetadata.options.retry;
    const dlqOptions = handlerMetadata.options.dlq;

    const retryCount = getRetryCountFromHeaders(message.headers);
    const correlationId = getCorrelationIdFromHeaders(message.headers);
    const messageId = createMessageId(message, partition);

    this.logger.debug('Processing message with retry interceptor', {
      messageId,
      topic,
      partition,
      offset: message.offset,
      retryCount,
      maxRetries: retryOptions.attempts,
      correlationId,
    });

    return next.handle().pipe(
      catchError(async (error) => {
        this.logger.warn('Message processing failed', {
          messageId,
          topic,
          retryCount,
          maxRetries: retryOptions.attempts,
          error: error.message,
        });

        // Check if we should retry
        if (retryCount < retryOptions.attempts) {
          // Calculate delay for next retry
          const delay = calculateRetryDelay({
            attempt: retryCount + 1,
            baseDelay: retryOptions.baseDelay,
            maxDelay: retryOptions.maxDelay,
            backoff: retryOptions.backoff,
            jitter: true,
          });

          this.logger.log('Scheduling retry', {
            messageId,
            topic,
            retryCount: retryCount + 1,
            maxRetries: retryOptions.attempts,
            delayMs: delay,
          });

          // Publish to retry topic with proper headers
          await this.publishToRetryTopic(
            message,
            topic,
            partition,
            retryCount + 1,
            delay,
            correlationId || null,
            handlerMetadata,
          );

          // Don't re-throw - we've handled the retry by publishing to retry topic
          return;
        } else {
          // Max retries exceeded
          this.logger.error('Max retries exceeded, processing DLQ', {
            messageId,
            topic,
            retryCount,
            maxRetries: retryOptions.attempts,
          });

          // Handle DLQ if enabled
          if (dlqOptions?.enabled) {
            await this.handleDLQ(
              message,
              error,
              topic,
              handlerMetadata,
              retryCount,
              correlationId,
            );
          }

          // Don't re-throw - message is now dead lettered or discarded
          return;
        }
      }),
    );
  }

  private async publishToRetryTopic(
    message: any,
    originalTopic: string,
    partition: number,
    retryCount: number,
    delayMs: number,
    correlationId: string | null,
    handlerMetadata: any,
  ): Promise<void> {
    try {
      const retryTopicName = this.retryService.getRetryTopicName();
      const handlerId = this.createHandlerId(handlerMetadata);

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

      if (correlationId) {
        retryHeaders['x-correlation-id'] = correlationId;
      }

      // Copy original headers (excluding our retry headers)
      if (message.headers) {
        for (const [key, value] of Object.entries(message.headers)) {
          if (
            !key.startsWith('x-retry-') &&
            !key.startsWith('x-original-') &&
            !key.startsWith('x-handler-')
          ) {
            retryHeaders[key] = value ? value.toString() : '';
          }
        }
      }

      await this.producer.send(retryTopicName, {
        key: message.key,
        value: message.value,
        headers: retryHeaders,
      });

      this.logger.debug('Published message to retry topic', {
        retryTopicName,
        originalTopic,
        handlerId,
        retryCount,
      });
    } catch (error) {
      this.logger.error('Failed to publish to retry topic', {
        originalTopic,
        retryCount,
        error: error.message,
      });
      throw error;
    }
  }

  private createHandlerId(handlerMetadata: any): string {
    // Get the handler ID from the registry based on the pattern
    const handlers = this.handlerRegistry.getHandlersForPattern(
      handlerMetadata.pattern,
    );
    if (handlers.length > 0) {
      return handlers[0].handlerId;
    }

    // Fallback: create a synthetic ID
    return `unknown.${handlerMetadata.pattern}`;
  }

  private async handleDLQ(
    message: any,
    error: Error,
    originalTopic: string,
    handlerMetadata: any,
    retryCount: number,
    correlationId?: string | null,
  ): Promise<void> {
    try {
      const handlerId = this.createHandlerId(handlerMetadata);

      await this.dlqService.storeToDlq(
        message,
        error,
        originalTopic,
        handlerId,
        retryCount,
        correlationId || undefined,
      );

      this.logger.log('Message sent to centralized DLQ', {
        dlqTopic: this.dlqService.getDlqTopicName(),
        originalTopic,
        handlerId,
        errorReason: error.message,
      });
    } catch (dlqError) {
      this.logger.error('Failed to send message to centralized DLQ', {
        originalTopic,
        originalError: error.message,
        dlqError: dlqError.message,
      });
    }
  }
}
