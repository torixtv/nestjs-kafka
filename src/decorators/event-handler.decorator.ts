import { SetMetadata, applyDecorators } from '@nestjs/common';
import { EventPattern } from '@nestjs/microservices';

import { EVENT_HANDLER_METADATA } from '../core/kafka.constants';
import { EventHandlerOptions } from '../interfaces/kafka.interfaces';

/**
 * Simplified event handler decorator that only sets metadata.
 * All retry and DLQ logic is handled by interceptors, not by wrapping methods.
 */
export function EventHandler(
  pattern: string,
  options: EventHandlerOptions = {},
): MethodDecorator {
  const handlerMetadata = {
    pattern,
    options: {
      retry: {
        enabled: options.retry?.enabled ?? false,
        attempts:
          options.retry?.attempts !== undefined ? options.retry.attempts : 3,
        backoff:
          options.retry?.backoff !== undefined
            ? options.retry.backoff
            : 'exponential',
        maxDelay: options.retry?.maxDelay ?? 30000,
        baseDelay: options.retry?.baseDelay ?? 1000,
      },
      dlq: {
        enabled: options.dlq?.enabled ?? false,
        topic: options.dlq?.topic,
      },
    },
  };

  return applyDecorators(
    EventPattern(pattern),
    SetMetadata(EVENT_HANDLER_METADATA, handlerMetadata),
  );
}

/**
 * Simple event pattern decorator for cases where no retry/DLQ is needed
 */
export function SimpleEventHandler(pattern: string): MethodDecorator {
  return EventHandler(pattern, {
    retry: { enabled: false },
    dlq: { enabled: false },
  });
}
