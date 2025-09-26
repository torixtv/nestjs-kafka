// Global test setup
import { Test } from '@nestjs/testing';

// Increase timeout for integration tests
jest.setTimeout(30000);

// Mock KafkaJS to prevent real connections in unit tests
jest.mock('kafkajs', () => {
  const mockProducer = {
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    send: jest.fn().mockResolvedValue([]),
    sendBatch: jest.fn().mockResolvedValue([]),
    transaction: jest.fn().mockResolvedValue({
      send: jest.fn().mockResolvedValue([]),
      commit: jest.fn().mockResolvedValue(undefined),
      abort: jest.fn().mockResolvedValue(undefined),
    }),
    isIdempotent: jest.fn().mockReturnValue(false),
  };

  const mockConsumer = {
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    subscribe: jest.fn().mockResolvedValue(undefined),
    run: jest.fn().mockResolvedValue(undefined),
    commitOffsets: jest.fn().mockResolvedValue(undefined),
    stop: jest.fn().mockResolvedValue(undefined),
    pause: jest.fn().mockResolvedValue(undefined),
    resume: jest.fn().mockResolvedValue(undefined),
    seek: jest.fn().mockResolvedValue(undefined),
    describeGroup: jest.fn().mockResolvedValue({}),
  };

  const mockAdmin = {
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    createTopics: jest.fn().mockResolvedValue(true),
    deleteTopics: jest.fn().mockResolvedValue(undefined),
    listTopics: jest.fn().mockResolvedValue([]),
    fetchTopicMetadata: jest.fn().mockResolvedValue({}),
    describeConfigs: jest.fn().mockResolvedValue({}),
    alterConfigs: jest.fn().mockResolvedValue(undefined),
  };

  const mockKafka = {
    producer: jest.fn().mockReturnValue(mockProducer),
    consumer: jest.fn().mockReturnValue(mockConsumer),
    admin: jest.fn().mockReturnValue(mockAdmin),
    logger: jest.fn().mockReturnValue({
      info: jest.fn(),
      error: jest.fn(),
      warn: jest.fn(),
      debug: jest.fn(),
    }),
  };

  return {
    Kafka: jest.fn().mockImplementation(() => mockKafka),
    logLevel: {
      ERROR: 1,
      WARN: 2,
      INFO: 4,
      DEBUG: 5,
    },
    CompressionTypes: {
      None: 0,
      GZIP: 1,
      Snappy: 2,
      LZ4: 3,
      ZSTD: 4,
    },
  };
});

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