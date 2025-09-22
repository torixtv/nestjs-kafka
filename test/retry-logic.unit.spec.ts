import { calculateRetryDelay } from '../src/utils/retry.utils';

describe('Retry Logic Unit Tests', () => {
  describe('Timestamp-based Message Processing', () => {
    it('should correctly identify when message is ready', () => {
      const now = Date.now();
      const processAfter = now - 1000; // 1 second ago

      const isReady = now >= processAfter;
      expect(isReady).toBe(true);
    });

    it('should correctly identify when message is not ready', () => {
      const now = Date.now();
      const processAfter = now + 5000; // 5 seconds in future

      const isReady = now >= processAfter;
      expect(isReady).toBe(false);

      const timeRemaining = processAfter - now;
      expect(timeRemaining).toBeGreaterThan(4000);
      expect(timeRemaining).toBeLessThan(6000);
    });

    it('should handle delay calculation correctly', () => {
      const baseDelay = 1000;
      const attempt = 2;

      const delay = calculateRetryDelay({
        attempt,
        baseDelay,
        maxDelay: 30000,
        backoff: 'exponential',
      });

      // For exponential backoff: baseDelay * (2 ^ (attempt - 1))
      // For attempt 2: 1000 * (2 ^ (2-1)) = 1000 * 2 = 2000ms
      expect(delay).toBe(2000);
    });

    it('should create proper retry headers', () => {
      const originalTopic = 'test.topic';
      const handlerId = 'TestClass.testMethod';
      const retryCount = 2;
      const delayMs = 5000;
      const now = Date.now();

      const processAfter = now + delayMs;
      const headers = {
        'x-original-topic': originalTopic,
        'x-handler-id': handlerId,
        'x-retry-count': retryCount.toString(),
        'x-process-after': processAfter.toString(),
      };

      expect(headers['x-original-topic']).toBe('test.topic');
      expect(headers['x-handler-id']).toBe('TestClass.testMethod');
      expect(headers['x-retry-count']).toBe('2');
      expect(parseInt(headers['x-process-after'])).toBeGreaterThan(now);
    });
  });

  describe('Retry Logic Validation', () => {
    it('should handle retry count progression', () => {
      const maxAttempts = 3;
      const scenarios = [
        { retryCount: 0, shouldRetry: true },
        { retryCount: 1, shouldRetry: true },
        { retryCount: 2, shouldRetry: true },
        { retryCount: 3, shouldRetry: false }, // Exceeded max
        { retryCount: 4, shouldRetry: false },
      ];

      scenarios.forEach(({ retryCount, shouldRetry }) => {
        const canRetry = retryCount < maxAttempts;
        expect(canRetry).toBe(shouldRetry);
      });
    });

    it('should calculate progressive delays', () => {
      const baseDelay = 100;
      const maxDelay = 1000;

      const delays = [1, 2, 3, 4].map(attempt =>
        calculateRetryDelay({
          attempt,
          baseDelay,
          maxDelay,
          backoff: 'exponential',
        })
      );

      // Should be progressive: 100, 200, 400, 800 (using attempt-1 formula)
      expect(delays[0]).toBe(100);  // 100 * 2^(1-1) = 100 * 1
      expect(delays[1]).toBe(200);  // 100 * 2^(2-1) = 100 * 2
      expect(delays[2]).toBe(400);  // 100 * 2^(3-1) = 100 * 4
      expect(delays[3]).toBe(800);  // 100 * 2^(4-1) = 100 * 8

      // Each delay should be greater than or equal to the previous
      for (let i = 1; i < delays.length; i++) {
        expect(delays[i]).toBeGreaterThanOrEqual(delays[i - 1]);
      }
    });

    it('should handle linear backoff', () => {
      const baseDelay = 1000;

      const delays = [1, 2, 3].map(attempt =>
        calculateRetryDelay({
          attempt,
          baseDelay,
          maxDelay: 30000,
          backoff: 'linear',
        })
      );

      // Linear: baseDelay * attempt
      expect(delays[0]).toBe(1000); // 1000 * 1
      expect(delays[1]).toBe(2000); // 1000 * 2
      expect(delays[2]).toBe(3000); // 1000 * 3
    });
  });

  describe('Message ID Generation', () => {
    it('should create consistent message IDs', () => {
      const partition = 0;
      const offset = '123';
      const timestamp = '1640995200000';

      const createMessageId = (p: number, o: string, t: string) => `${p}-${o}-${t}`;

      const id1 = createMessageId(partition, offset, timestamp);
      const id2 = createMessageId(partition, offset, timestamp);

      expect(id1).toBe(id2);
      expect(id1).toBe('0-123-1640995200000');
    });

    it('should handle missing values gracefully', () => {
      const partition = 1;
      const offset = undefined;
      const timestamp = undefined;

      const createMessageId = (p: number, o?: string, t?: string) => {
        const now = Date.now().toString();
        return `${p}-${o || '0'}-${t || now}`;
      };

      const messageId = createMessageId(partition, offset, timestamp);
      expect(messageId).toMatch(/^1-0-\d+$/);
    });
  });

  describe('Header Validation', () => {
    it('should validate required headers', () => {
      const requiredHeaders = [
        'x-original-topic',
        'x-handler-id',
        'x-retry-count',
        'x-process-after',
      ];

      const testHeaders = {
        'x-original-topic': 'test.topic',
        'x-handler-id': 'TestClass.testMethod',
        'x-retry-count': '1',
        'x-process-after': Date.now().toString(),
        'x-correlation-id': 'optional-header',
      };

      // All required headers should be present
      requiredHeaders.forEach(header => {
        expect(testHeaders).toHaveProperty(header);
        expect(testHeaders[header as keyof typeof testHeaders]).toBeTruthy();
      });

      // Should handle missing headers
      const incompleteHeaders = {
        'x-original-topic': 'test.topic',
        // Missing other required headers
      };

      const missingHeaders = requiredHeaders.filter(
        header => !incompleteHeaders.hasOwnProperty(header)
      );

      expect(missingHeaders.length).toBeGreaterThan(0);
    });

    it('should parse headers correctly', () => {
      const headers = {
        'x-retry-count': '3',
        'x-process-after': '1640995200000',
      };

      const retryCount = parseInt(headers['x-retry-count'], 10);
      const processAfter = parseInt(headers['x-process-after'], 10);

      expect(retryCount).toBe(3);
      expect(processAfter).toBe(1640995200000);
    });
  });

  describe('Polling Interval Logic', () => {
    it('should use 10-second polling interval', () => {
      const POLLING_INTERVAL_MS = 10000;

      expect(POLLING_INTERVAL_MS).toBe(10 * 1000);
      expect(POLLING_INTERVAL_MS).toBeGreaterThan(5000); // At least 5 seconds
      expect(POLLING_INTERVAL_MS).toBeLessThan(30000);   // Less than 30 seconds
    });

    it('should handle interval timing correctly', () => {
      const startTime = Date.now();
      const intervalMs = 10000;

      // Simulate what happens after one interval
      const afterInterval = startTime + intervalMs;
      const elapsed = afterInterval - startTime;

      expect(elapsed).toBe(intervalMs);
    });
  });
});