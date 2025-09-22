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

import { EVENT_HANDLER_METADATA, KAFKA_PLUGINS } from '../core/kafka.constants';
import { KafkaPlugin } from '../interfaces/kafka.interfaces';
import {
  calculateRetryDelay,
  getRetryCountFromHeaders,
  getCorrelationIdFromHeaders,
  createMessageId,
} from '../utils/retry.utils';
import { KafkaProducerService } from '../core/kafka.producer';
import { KafkaRetryManager } from '../services/kafka.retry-manager';
import { KafkaHandlerRegistry } from '../services/kafka.registry';

@Injectable()
export class RetryInterceptor implements NestInterceptor {
  private readonly logger = new Logger(RetryInterceptor.name);

  constructor(
    private readonly reflector: Reflector,
    private readonly producer: KafkaProducerService,
    private readonly retryManager: KafkaRetryManager,
    private readonly handlerRegistry: KafkaHandlerRegistry,
    @Inject(KAFKA_PLUGINS) private readonly plugins: KafkaPlugin[] = [],
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
      return this.executeWithPlugins(context, next);
    }

    return this.executeWithRetry(context, next, handlerMetadata);
  }

  private executeWithPlugins(
    context: ExecutionContext,
    next: CallHandler,
  ): Observable<any> {
    return next.handle().pipe(
      catchError(async (error) => {
        // Run failure plugins
        await this.runPluginHooks('onFailure', context, error);
        throw error;
      }),
    );
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

        // Run failure plugins
        await this.runPluginHooks('onFailure', context, error);

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
            await this.handleDLQ(message, error, dlqOptions, {
              messageId,
              topic,
              partition,
              retryCount,
            });
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
      const retryTopicName = this.retryManager.getRetryTopicName();
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
    dlqOptions: any,
    context: any,
  ): Promise<void> {
    try {
      // Find DLQ plugin if available
      const dlqPlugin = this.plugins.find((p) => p.name === 'dlq');
      if (dlqPlugin && 'handleFailure' in dlqPlugin) {
        await (dlqPlugin as any).handleFailure(message, error);
      } else {
        this.logger.warn('DLQ enabled but no DLQ plugin available', {
          messageId: context.messageId,
          topic: context.topic,
        });
      }
    } catch (dlqError) {
      this.logger.error('Failed to handle DLQ', {
        messageId: context.messageId,
        originalError: error.message,
        dlqError: dlqError.message,
      });
    }
  }

  private async runPluginHooks(
    hookName: keyof KafkaPlugin,
    context: ExecutionContext,
    ...args: any[]
  ): Promise<void> {
    for (const plugin of this.plugins) {
      try {
        if (typeof plugin[hookName] === 'function') {
          await (plugin[hookName] as Function)(...args);
        }
      } catch (error) {
        this.logger.warn(
          `Plugin ${plugin.name} hook ${hookName} failed`,
          error.message,
        );
      }
    }
  }
}
