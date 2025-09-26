import {
  trace,
  context,
  propagation,
  SpanKind,
  SpanStatusCode,
  Span,
  Context,
} from '@opentelemetry/api';

const TRACER_NAME = '@torix/nestjs-kafka';

export interface KafkaSpanAttributes {
  'messaging.system': string;
  'messaging.destination': string;
  'messaging.destination_kind': string;
  'messaging.kafka.partition'?: number;
  'messaging.kafka.offset'?: number;
  'messaging.kafka.consumer_group'?: string;
  'messaging.message_id'?: string;
  'messaging.kafka.retry_count'?: number;
  'kafka.handler_id'?: string;
}

/**
 * Check if OpenTelemetry API is available
 */
export function isTracingEnabled(): boolean {
  try {
    return trace.getTracer !== undefined;
  } catch {
    return false;
  }
}

/**
 * Get the OpenTelemetry tracer
 */
export function getTracer() {
  if (!isTracingEnabled()) {
    // Return a no-op tracer if tracing is not available
    return {
      startSpan: () => ({
        setStatus: () => {},
        setAttributes: () => {},
        end: () => {},
      }),
    } as any;
  }
  return trace.getTracer(TRACER_NAME);
}

/**
 * Extract OpenTelemetry context from Kafka message headers
 */
export function extractContextFromHeaders(
  headers: Record<string, any> = {},
): Context {
  if (!isTracingEnabled()) {
    return context.active();
  }

  const headerCarrier: Record<string, string> = {};

  // Convert headers to string format expected by propagation API
  Object.entries(headers).forEach(([key, value]) => {
    if (value !== null && value !== undefined) {
      headerCarrier[key] = String(value);
    }
  });

  return propagation.extract(context.active(), headerCarrier);
}

/**
 * Inject OpenTelemetry context into Kafka message headers
 */
export function injectContextIntoHeaders(
  headers: Record<string, string> = {},
  ctx?: Context,
): Record<string, string> {
  if (!isTracingEnabled()) {
    return headers;
  }

  const activeContext = ctx || context.active();
  const headerCarrier: Record<string, string> = { ...headers };

  propagation.inject(activeContext, headerCarrier);

  return headerCarrier;
}

/**
 * Create a span for Kafka message production
 */
export function createProducerSpan(
  topic: string,
  key?: string,
  partition?: number,
): Span {
  const tracer = getTracer();

  const attributes: KafkaSpanAttributes = {
    'messaging.system': 'kafka',
    'messaging.destination': topic,
    'messaging.destination_kind': 'topic',
  };

  if (partition !== undefined) {
    attributes['messaging.kafka.partition'] = partition;
  }

  if (key) {
    attributes['messaging.message_id'] = key;
  }

  return tracer.startSpan(`${topic} send`, {
    kind: SpanKind.PRODUCER,
    attributes,
  });
}

/**
 * Create a span for Kafka message consumption
 */
export function createConsumerSpan(
  topic: string,
  handlerId: string,
  message: any,
  partition: number,
  consumerGroup?: string,
): Span {
  const tracer = getTracer();

  const attributes: KafkaSpanAttributes = {
    'messaging.system': 'kafka',
    'messaging.destination': topic,
    'messaging.destination_kind': 'topic',
    'messaging.kafka.partition': partition,
    'kafka.handler_id': handlerId,
  };

  if (message.offset !== undefined) {
    attributes['messaging.kafka.offset'] = parseInt(message.offset, 10);
  }

  if (message.key) {
    attributes['messaging.message_id'] = message.key;
  }

  if (consumerGroup) {
    attributes['messaging.kafka.consumer_group'] = consumerGroup;
  }

  // Add retry count if present
  const retryCount = parseInt(message.headers?.['x-retry-count'] || '0', 10);
  if (retryCount > 0) {
    attributes['messaging.kafka.retry_count'] = retryCount;
  }

  return tracer.startSpan(`${topic} receive`, {
    kind: SpanKind.CONSUMER,
    attributes,
  });
}

/**
 * Create a span for retry operations
 */
export function createRetrySpan(
  topic: string,
  handlerId: string,
  retryCount: number,
  delayMs: number,
): Span {
  const tracer = getTracer();

  const attributes = {
    'messaging.system': 'kafka',
    'messaging.destination': topic,
    'messaging.destination_kind': 'topic',
    'kafka.handler_id': handlerId,
    'messaging.kafka.retry_count': retryCount,
    'kafka.retry.delay_ms': delayMs,
    'kafka.operation': 'retry',
  };

  return tracer.startSpan(`${topic} retry`, {
    kind: SpanKind.INTERNAL,
    attributes,
  });
}

/**
 * Create a span for DLQ operations
 */
export function createDlqSpan(
  topic: string,
  handlerId: string,
  retryCount: number,
  operation: 'store' | 'reprocess',
): Span {
  const tracer = getTracer();

  const attributes = {
    'messaging.system': 'kafka',
    'messaging.destination': topic,
    'messaging.destination_kind': 'topic',
    'kafka.handler_id': handlerId,
    'messaging.kafka.retry_count': retryCount,
    'kafka.operation': `dlq_${operation}`,
  };

  return tracer.startSpan(`${topic} dlq_${operation}`, {
    kind: SpanKind.INTERNAL,
    attributes,
  });
}

/**
 * Get correlation ID from current span baggage or headers
 */
export function getCorrelationId(
  headers?: Record<string, any>,
): string | undefined {
  // First try to get from headers (existing functionality)
  if (headers) {
    const correlationId =
      headers['x-correlation-id'] || headers['correlationId'];
    if (correlationId) {
      return String(correlationId);
    }
  }

  // TODO: In future versions, we could extract from span baggage
  // This would require OpenTelemetry baggage API support

  return undefined;
}

/**
 * Set span status based on error
 */
export function setSpanError(span: Span, error: Error): void {
  span.setStatus({
    code: SpanStatusCode.ERROR,
    message: error.message,
  });

  span.setAttributes({
    error: true,
    'error.name': error.name,
    'error.message': error.message,
  });

  if (error.stack) {
    span.setAttributes({
      'error.stack': error.stack,
    });
  }
}

/**
 * Execute a function within a span context
 */
export async function executeWithSpan<T>(
  span: Span,
  fn: () => Promise<T> | T,
): Promise<T> {
  if (!isTracingEnabled()) {
    return await fn();
  }

  return context.with(trace.setSpan(context.active(), span), async () => {
    try {
      const result = await fn();
      span.setStatus({ code: SpanStatusCode.OK });
      return result;
    } catch (error) {
      setSpanError(span, error as Error);
      throw error;
    } finally {
      span.end();
    }
  });
}

/**
 * Execute a function within a span context (sync version for RxJS)
 */
export function executeWithSpanSync<T>(span: Span, fn: () => T): T {
  if (!isTracingEnabled()) {
    return fn();
  }

  return context.with(trace.setSpan(context.active(), span), () => {
    try {
      const result = fn();
      span.setStatus({ code: SpanStatusCode.OK });
      return result;
    } catch (error) {
      setSpanError(span, error as Error);
      throw error;
    } finally {
      span.end();
    }
  });
}

/**
 * Execute a function within an extracted context
 */
export function executeWithExtractedContext<T>(
  headers: Record<string, any>,
  fn: () => T,
): T {
  if (!isTracingEnabled()) {
    return fn();
  }

  const extractedContext = extractContextFromHeaders(headers);
  return context.with(extractedContext, fn);
}
