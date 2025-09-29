# Plan: Integrate Retry Interceptor into Main Consumer Service

## Problem Analysis

The retry mechanism is not working in the new single-instance architecture because:

1. **RetryInterceptor expects NestJS RPC context** (`context.getType() === 'rpc'`) from microservices pattern
2. **KafkaConsumerService directly calls handler methods** without any interceptor pipeline
3. **Handlers throw errors but they're caught** in `handleMessage()` without retry logic being applied

## Current Architecture Issue

```
Before (Dual-Instance):
NestJS Microservice → RetryInterceptor → Handler → Error → Retry/DLQ

Now (Single-Instance):
KafkaConsumerService → Handler → Error → ❌ No Retry Logic
```

## Solution Overview

Integrate retry/DLQ logic directly into the KafkaConsumerService's message handling flow, adapting the interceptor pattern to work with our single-instance architecture.

## Implementation Steps

### Step 1: Modify KafkaConsumerService (src/services/kafka.consumer.service.ts)

Add dependencies and enhance message handling:

```typescript
constructor(
  @Inject(KAFKAJS_INSTANCE) private readonly kafka: Kafka,
  @Inject(KAFKA_MODULE_OPTIONS) private readonly options: KafkaModuleOptions,
  private readonly handlerRegistry: KafkaHandlerRegistry,
  private readonly retryService: KafkaRetryService,  // Add
  private readonly dlqService: KafkaDlqService,      // Add
  private readonly producer: KafkaProducerService,   // Add
)
```

### Step 2: Enhance handleMessage Method

Replace current `handleMessage()` with retry-aware implementation:

```typescript
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
      // Execute handler
      await handler.method.call(handler.instance, parsedMessage);
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
```

### Step 3: Implement Error Handling with Retry Logic

```typescript
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
```

### Step 4: Add Retry Scheduling Method

```typescript
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
```

### Step 5: Add Helper Methods

```typescript
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
```

### Step 6: Import Required Utilities

Add to imports:
```typescript
import { calculateRetryDelay } from '../utils/retry.utils';
import { KafkaRetryService } from './kafka.retry.service';
import { KafkaDlqService } from './kafka.dlq.service';
import { KafkaProducerService } from '../core/kafka.producer';
```

## Testing Plan

1. **Test Immediate Success**
   - Send message to `example.immediate.success`
   - Verify processed without retry

2. **Test Retry Success**
   - Send message to `example.retry.success` with `shouldFail: true`
   - Verify fails initially
   - Verify retry scheduled with delay
   - Verify succeeds on retry attempt

3. **Test DLQ Routing**
   - Send message to `example.dlq.test` with `action: "fail"`
   - Verify all retry attempts fail
   - Verify message sent to DLQ after max retries

4. **Test DLQ Reprocessing**
   - Trigger DLQ reprocessing endpoint
   - Verify messages retrieved from DLQ
   - Verify reprocessing attempts

## Benefits

- ✅ Maintains single Kafka instance architecture
- ✅ Reuses existing retry/DLQ services
- ✅ Preserves handler metadata configuration
- ✅ Works with current @EventHandler decorators
- ✅ No changes needed to handler implementations
- ✅ Consistent with retry service behavior

## Files to Modify

1. `src/services/kafka.consumer.service.ts` - Main implementation
2. No other files need changes - all retry/DLQ services remain as-is

## Metrics to Track

- Messages processed successfully
- Messages retried (by retry count)
- Messages sent to DLQ
- Retry delays applied
- Handler execution times