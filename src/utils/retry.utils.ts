export interface RetryCalculationOptions {
  attempt: number;
  baseDelay: number;
  maxDelay: number;
  backoff: 'linear' | 'exponential';
  jitter?: boolean;
}

/**
 * Calculate retry delay based on attempt number and configuration
 */
export function calculateRetryDelay(options: RetryCalculationOptions): number {
  const { attempt, baseDelay, maxDelay, backoff, jitter = false } = options;

  let delay: number;

  switch (backoff) {
    case 'linear':
      delay = baseDelay * attempt;
      break;
    case 'exponential':
    default:
      delay = Math.min(baseDelay * Math.pow(2, attempt - 1), maxDelay);
      break;
  }

  // Add jitter to prevent thundering herd
  if (jitter) {
    const jitterAmount = delay * 0.1; // 10% jitter
    delay += (Math.random() - 0.5) * 2 * jitterAmount;
  }

  return Math.max(delay, 0);
}

/**
 * Extract retry count from Kafka message headers
 */
export function getRetryCountFromHeaders(
  headers: Record<string, any> = {},
): number {
  const retryCount = headers['x-retry-count'];
  if (retryCount) {
    const parsed = parseInt(String(retryCount), 10);
    return isNaN(parsed) ? 0 : parsed;
  }
  return 0;
}

/**
 * Extract correlation ID from Kafka message headers
 */
export function getCorrelationIdFromHeaders(
  headers: Record<string, any> = {},
): string | undefined {
  return headers['x-correlation-id'] || headers['correlationId'];
}

/**
 * Create message ID for tracking
 */
export function createMessageId(message: any, partition: number): string {
  return `${partition}-${message.offset}-${message.timestamp || Date.now()}`;
}
