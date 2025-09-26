// Integration test setup - no mocking for real Kafka connections
import { Test } from '@nestjs/testing';

// Increase timeout for integration tests
jest.setTimeout(60000);

// Mock console methods to reduce noise in tests
global.console = {
  ...console,
  log: jest.fn(),
  debug: jest.fn(),
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
};

// Add global test utilities
declare global {
  namespace jest {
    interface Matchers<R> {
      toBeValidKafkaMessage(): R;
    }
  }
}

expect.extend({
  toBeValidKafkaMessage(received) {
    const isValid =
      received &&
      typeof received === 'object' &&
      'value' in received;

    return {
      message: () =>
        `expected ${received} to be a valid Kafka message`,
      pass: isValid,
    };
  },
});

export {};