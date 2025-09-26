import {
  trace,
  SpanKind,
  SpanStatusCode,
  Span,
} from '@opentelemetry/api';
import { randomBytes } from 'crypto';

const TRACER_NAME = '@torix/nestjs-kafka';

/**
 * Get the OpenTelemetry tracer, returns undefined if not available
 */
function getTracer() {
  try {
    return trace.getTracer(TRACER_NAME);
  } catch {
    return undefined;
  }
}

/**
 * Create a span for retry operations
 */
export function createRetrySpan(
  topic: string,
  handlerId: string,
  retryCount: number,
  delayMs: number,
): Span | undefined {
  const tracer = getTracer();
  if (!tracer) return undefined;

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
): Span | undefined {
  const tracer = getTracer();
  if (!tracer) return undefined;

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
 * Generate a new correlation ID
 */
function generateCorrelationId(): string {
  return `corr-${randomBytes(8).toString('hex')}-${Date.now()}`;
}

/**
 * Get correlation ID from current tracing context
 * This is a placeholder for future baggage implementation
 */
function getCorrelationIdFromContext(): string | undefined {
  // Future enhancement: extract from OpenTelemetry baggage
  // For now, we rely on header propagation
  return undefined;
}

/**
 * Get correlation ID from headers (legacy support)
 */
export function getCorrelationId(
  headers?: Record<string, any>,
): string | undefined {
  if (headers) {
    const correlationId =
      headers['x-correlation-id'] || headers['correlationId'];
    if (correlationId) {
      return String(correlationId);
    }
  }
  return undefined;
}

/**
 * Get or generate correlation ID from multiple sources with precedence:
 * 1. Explicit correlationId parameter (highest priority)
 * 2. Headers (for backward compatibility)
 * 3. OpenTelemetry context (future enhancement)
 * 4. Auto-generate new ID (fallback)
 */
export function getOrGenerateCorrelationId(
  explicitId?: string,
  headers?: Record<string, any>,
): string {
  // 1. Use explicit ID if provided
  if (explicitId) {
    return explicitId;
  }

  // 2. Check headers for backward compatibility
  const headerCorrelationId = getCorrelationId(headers);
  if (headerCorrelationId) {
    return headerCorrelationId;
  }

  // 3. Check OpenTelemetry context (future enhancement)
  const contextCorrelationId = getCorrelationIdFromContext();
  if (contextCorrelationId) {
    return contextCorrelationId;
  }

  // 4. Generate new correlation ID
  return generateCorrelationId();
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
 * Execute a function within a span context with error handling
 */
export async function executeWithSpan<T>(
  span: Span | undefined,
  fn: () => Promise<T> | T,
): Promise<T> {
  if (!span) {
    return await fn();
  }

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
}
