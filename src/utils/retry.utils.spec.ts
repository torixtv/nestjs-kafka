import {
  calculateRetryDelay,
  getRetryCountFromHeaders,
  getCorrelationIdFromHeaders,
  createMessageId,
  RetryCalculationOptions,
} from './retry.utils';

describe('RetryUtils', () => {
  describe('calculateRetryDelay', () => {
    it('should calculate exponential backoff delay', () => {
      const options: RetryCalculationOptions = {
        attempt: 1,
        baseDelay: 1000,
        maxDelay: 30000,
        backoff: 'exponential',
      };

      const delay = calculateRetryDelay(options);
      expect(delay).toBe(1000); // 1000 * 2^(1-1) = 1000
    });

    it('should calculate exponential backoff for multiple attempts', () => {
      const baseOptions: Omit<RetryCalculationOptions, 'attempt'> = {
        baseDelay: 1000,
        maxDelay: 30000,
        backoff: 'exponential',
      };

      expect(calculateRetryDelay({ ...baseOptions, attempt: 1 })).toBe(1000); // 2^0
      expect(calculateRetryDelay({ ...baseOptions, attempt: 2 })).toBe(2000); // 2^1
      expect(calculateRetryDelay({ ...baseOptions, attempt: 3 })).toBe(4000); // 2^2
      expect(calculateRetryDelay({ ...baseOptions, attempt: 4 })).toBe(8000); // 2^3
    });

    it('should respect maxDelay for exponential backoff', () => {
      const options: RetryCalculationOptions = {
        attempt: 10,
        baseDelay: 1000,
        maxDelay: 10000,
        backoff: 'exponential',
      };

      const delay = calculateRetryDelay(options);
      expect(delay).toBe(10000); // Should be capped at maxDelay
    });

    it('should calculate linear backoff delay', () => {
      const baseOptions: Omit<RetryCalculationOptions, 'attempt'> = {
        baseDelay: 1000,
        maxDelay: 30000,
        backoff: 'linear',
      };

      expect(calculateRetryDelay({ ...baseOptions, attempt: 1 })).toBe(1000); // 1000 * 1
      expect(calculateRetryDelay({ ...baseOptions, attempt: 2 })).toBe(2000); // 1000 * 2
      expect(calculateRetryDelay({ ...baseOptions, attempt: 3 })).toBe(3000); // 1000 * 3
      expect(calculateRetryDelay({ ...baseOptions, attempt: 5 })).toBe(5000); // 1000 * 5
    });

    it('should add jitter when enabled', () => {
      const options: RetryCalculationOptions = {
        attempt: 1,
        baseDelay: 1000,
        maxDelay: 30000,
        backoff: 'exponential',
        jitter: true,
      };

      // Run multiple times to test jitter variance
      const delays = Array.from({ length: 10 }, () => calculateRetryDelay(options));

      // All delays should be positive
      delays.forEach(delay => expect(delay).toBeGreaterThanOrEqual(0));

      // Should have some variance (not all the same due to jitter)
      const baseDelay = calculateRetryDelay({ ...options, jitter: false });
      const hasVariance = delays.some(delay => Math.abs(delay - baseDelay) > 0);
      expect(hasVariance).toBe(true);
    });

    it('should not allow negative delays', () => {
      const options: RetryCalculationOptions = {
        attempt: 1,
        baseDelay: 0,
        maxDelay: 1000,
        backoff: 'linear',
        jitter: true,
      };

      const delay = calculateRetryDelay(options);
      expect(delay).toBeGreaterThanOrEqual(0);
    });
  });

  describe('getRetryCountFromHeaders', () => {
    it('should return 0 for undefined headers', () => {
      const count = getRetryCountFromHeaders();
      expect(count).toBe(0);
    });

    it('should return 0 for empty headers', () => {
      const count = getRetryCountFromHeaders({});
      expect(count).toBe(0);
    });

    it('should extract valid retry count', () => {
      const headers = { 'x-retry-count': '3' };
      const count = getRetryCountFromHeaders(headers);
      expect(count).toBe(3);
    });

    it('should handle numeric retry count', () => {
      const headers = { 'x-retry-count': 5 };
      const count = getRetryCountFromHeaders(headers);
      expect(count).toBe(5);
    });

    it('should return 0 for invalid retry count', () => {
      const headers = { 'x-retry-count': 'invalid' };
      const count = getRetryCountFromHeaders(headers);
      expect(count).toBe(0);
    });

    it('should return 0 for missing retry count header', () => {
      const headers = { 'other-header': 'value' };
      const count = getRetryCountFromHeaders(headers);
      expect(count).toBe(0);
    });
  });

  describe('getCorrelationIdFromHeaders', () => {
    it('should return undefined for undefined headers', () => {
      const correlationId = getCorrelationIdFromHeaders();
      expect(correlationId).toBeUndefined();
    });

    it('should return undefined for empty headers', () => {
      const correlationId = getCorrelationIdFromHeaders({});
      expect(correlationId).toBeUndefined();
    });

    it('should extract x-correlation-id', () => {
      const headers = { 'x-correlation-id': 'test-correlation-id' };
      const correlationId = getCorrelationIdFromHeaders(headers);
      expect(correlationId).toBe('test-correlation-id');
    });

    it('should extract correlationId', () => {
      const headers = { correlationId: 'test-correlation-id' };
      const correlationId = getCorrelationIdFromHeaders(headers);
      expect(correlationId).toBe('test-correlation-id');
    });

    it('should prefer x-correlation-id over correlationId', () => {
      const headers = {
        'x-correlation-id': 'preferred-id',
        correlationId: 'fallback-id',
      };
      const correlationId = getCorrelationIdFromHeaders(headers);
      expect(correlationId).toBe('preferred-id');
    });

    it('should return undefined for missing correlation headers', () => {
      const headers = { 'other-header': 'value' };
      const correlationId = getCorrelationIdFromHeaders(headers);
      expect(correlationId).toBeUndefined();
    });
  });

  describe('createMessageId', () => {
    it('should create a valid message ID', () => {
      const message = {
        offset: '12345',
        timestamp: '1640995200000',
      };
      const partition = 0;

      const messageId = createMessageId(message, partition);
      expect(messageId).toBe('0-12345-1640995200000');
    });

    it('should handle missing timestamp', () => {
      const message = {
        offset: '12345',
      };
      const partition = 2;

      const messageId = createMessageId(message, partition);
      expect(messageId).toMatch(/^2-12345-\d+$/);
    });

    it('should include partition number', () => {
      const message = {
        offset: '98765',
        timestamp: '1640995200000',
      };
      const partition = 5;

      const messageId = createMessageId(message, partition);
      expect(messageId).toBe('5-98765-1640995200000');
    });

    it('should generate unique IDs for different messages', () => {
      const message1 = { offset: '1', timestamp: '1000' };
      const message2 = { offset: '2', timestamp: '2000' };

      const id1 = createMessageId(message1, 0);
      const id2 = createMessageId(message2, 0);

      expect(id1).not.toBe(id2);
    });

    it('should generate unique IDs for same message on different partitions', () => {
      const message = { offset: '1', timestamp: '1000' };

      const id1 = createMessageId(message, 0);
      const id2 = createMessageId(message, 1);

      expect(id1).not.toBe(id2);
      expect(id1).toBe('0-1-1000');
      expect(id2).toBe('1-1-1000');
    });
  });
});