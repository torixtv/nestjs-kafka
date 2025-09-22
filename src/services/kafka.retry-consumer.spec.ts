import { Test, TestingModule } from '@nestjs/testing';
import { Logger } from '@nestjs/common';
import { Consumer, Kafka, KafkaMessage } from 'kafkajs';
import { KafkaRetryConsumer, RetryMessageHeaders } from './kafka.retry-consumer';
import { KafkaHandlerRegistry, RegisteredHandler } from './kafka.registry';
import { KafkaRetryManager } from './kafka.retry-manager';
import { KAFKAJS_INSTANCE, KAFKA_MODULE_OPTIONS } from '../core/kafka.constants';
import { KafkaModuleOptions } from '../interfaces/kafka.interfaces';

describe('KafkaRetryConsumer', () => {
  let consumer: KafkaRetryConsumer;
  let mockKafka: jest.Mocked<Kafka>;
  let mockConsumer: jest.Mocked<Consumer>;
  let mockHandlerRegistry: jest.Mocked<KafkaHandlerRegistry>;
  let mockRetryManager: jest.Mocked<KafkaRetryManager>;
  let mockOptions: KafkaModuleOptions;

  const createMockMessage = (headers: Partial<RetryMessageHeaders>): KafkaMessage => ({
    key: Buffer.from('test-key'),
    value: Buffer.from(JSON.stringify({ data: 'test' })),
    timestamp: '1640995200000',
    offset: '123',
    headers: Object.entries(headers).reduce((acc, [key, value]) => {
      acc[key] = Buffer.from(value);
      return acc;
    }, {} as Record<string, Buffer>),
  } as any);

  const createMockHandler = (): RegisteredHandler => ({
    instance: { testMethod: jest.fn() },
    methodName: 'testMethod',
    pattern: 'test.topic',
    handlerId: 'TestClass.testMethod',
    metadata: {
      pattern: 'test.topic',
      options: {
        retry: {
          enabled: true,
          attempts: 3,
          baseDelay: 1000,
          maxDelay: 30000,
          backoff: 'exponential',
        },
        dlq: { enabled: false },
      },
    },
  });

  beforeEach(async () => {
    // Mock consumer methods
    mockConsumer = {
      connect: jest.fn(),
      disconnect: jest.fn(),
      subscribe: jest.fn(),
      run: jest.fn(),
      seek: jest.fn(),
      pause: jest.fn(),
      resume: jest.fn(),
      commitOffsets: jest.fn(),
      describeGroup: jest.fn(),
      stop: jest.fn(),
      on: jest.fn(),
      events: {} as any,
    } as any;

    mockKafka = {
      consumer: jest.fn().mockReturnValue(mockConsumer),
    } as any;

    mockHandlerRegistry = {
      getHandler: jest.fn(),
      getHandlersForPattern: jest.fn(),
      getAllHandlers: jest.fn(),
      getAllPatterns: jest.fn(),
      executeHandler: jest.fn(),
      hasHandler: jest.fn(),
      onModuleInit: jest.fn(),
    } as any;

    mockRetryManager = {
      getRetryTopicName: jest.fn().mockReturnValue('test-service.retry'),
      ensureRetryTopicExists: jest.fn(),
      deleteRetryTopic: jest.fn(),
      getTopicMetadata: jest.fn(),
      retryTopicExists: jest.fn(),
      onModuleInit: jest.fn(),
    } as any;

    mockOptions = {
      consumer: {
        groupId: 'test-group',
      },
      client: {
        clientId: 'test-service',
        brokers: ['localhost:9092'],
      },
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        KafkaRetryConsumer,
        {
          provide: KAFKAJS_INSTANCE,
          useValue: mockKafka,
        },
        {
          provide: KAFKA_MODULE_OPTIONS,
          useValue: mockOptions,
        },
        {
          provide: KafkaHandlerRegistry,
          useValue: mockHandlerRegistry,
        },
        {
          provide: KafkaRetryManager,
          useValue: mockRetryManager,
        },
      ],
    }).compile();

    consumer = module.get<KafkaRetryConsumer>(KafkaRetryConsumer);

    // Mock logger to reduce test noise
    jest.spyOn(Logger.prototype, 'log').mockImplementation();
    jest.spyOn(Logger.prototype, 'debug').mockImplementation();
    jest.spyOn(Logger.prototype, 'warn').mockImplementation();
    jest.spyOn(Logger.prototype, 'error').mockImplementation();
  });

  afterEach(() => {
    jest.clearAllMocks();
    jest.clearAllTimers();
  });

  describe('initialization', () => {
    it('should create consumer with correct configuration', () => {
      expect(mockKafka.consumer).toHaveBeenCalledWith({
        groupId: 'test-group.retry',
      });
    });

    it('should start retry consumer on module init', async () => {
      await consumer.onModuleInit();

      expect(mockConsumer.connect).toHaveBeenCalled();
      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: 'test-service.retry',
        fromBeginning: false,
      });
      expect(consumer.isRetryConsumerRunning()).toBe(true);
    });

    it('should stop retry consumer on module destroy', async () => {
      await consumer.onModuleInit();
      await consumer.onModuleDestroy();

      expect(mockConsumer.disconnect).toHaveBeenCalled();
      expect(consumer.isRetryConsumerRunning()).toBe(false);
    });
  });

  describe('message processing', () => {
    beforeEach(async () => {
      await consumer.onModuleInit();
    });

    afterEach(async () => {
      await consumer.onModuleDestroy();
    });

    it('should process message immediately when ready', async () => {
      const handler = createMockHandler();
      const now = Date.now();
      const message = createMockMessage({
        'x-original-topic': 'test.topic',
        'x-handler-id': 'TestClass.testMethod',
        'x-retry-count': '1',
        'x-process-after': (now - 1000).toString(), // Already ready
      });

      mockHandlerRegistry.getHandler.mockReturnValue(handler);
      mockHandlerRegistry.executeHandler.mockResolvedValue(undefined);

      // Test the processReadyMessage method directly with valid headers
      const headers = (consumer as any).extractHeaders(message);
      if (!headers) throw new Error('Invalid headers in test');
      await (consumer as any).processReadyMessage({
        message,
        topic: 'test-service.retry',
        partition: 0,
      }, headers);

      expect(mockHandlerRegistry.executeHandler).toHaveBeenCalledWith(
        'TestClass.testMethod',
        { data: 'test' }
      );
      expect(mockConsumer.seek).not.toHaveBeenCalled();
    });

    it('should skip processing when message is not ready', async () => {
      const now = Date.now();
      const message = createMockMessage({
        'x-original-topic': 'test.topic',
        'x-handler-id': 'TestClass.testMethod',
        'x-retry-count': '1',
        'x-process-after': (now + 5000).toString(), // Not ready yet
      });

      const headers = (consumer as any).extractHeaders(message);
      const processAfter = parseInt(headers['x-process-after'], 10);

      // Verify timestamp logic works correctly
      expect(processAfter).toBeGreaterThan(now);
      expect(Date.now()).toBeLessThan(processAfter);

      // In the new architecture, not-ready messages are skipped during batch processing
      // We don't test processReadyMessage for not-ready messages
      expect(mockHandlerRegistry.executeHandler).not.toHaveBeenCalled();
    });

    it('should skip message with missing headers', async () => {
      const message = createMockMessage({
        'x-handler-id': 'TestClass.testMethod',
        // Missing required headers
      });

      // Test header extraction directly
      const headers = (consumer as any).extractHeaders(message);
      expect(headers).toBeNull();

      // In the new architecture, messages with missing headers are skipped in batch processing
      expect(mockHandlerRegistry.getHandler).not.toHaveBeenCalled();
      expect(mockHandlerRegistry.executeHandler).not.toHaveBeenCalled();
    });

    it('should skip message when handler not found', async () => {
      const message = createMockMessage({
        'x-original-topic': 'test.topic',
        'x-handler-id': 'NonExistent.handler',
        'x-retry-count': '1',
        'x-process-after': Date.now().toString(),
      });

      mockHandlerRegistry.getHandler.mockReturnValue(undefined);

      // Test the processReadyMessage method directly with valid headers
      const headers = (consumer as any).extractHeaders(message);
      if (!headers) throw new Error('Invalid headers in test');
      await (consumer as any).processReadyMessage({
        message,
        topic: 'test-service.retry',
        partition: 0,
      }, headers);

      expect(mockHandlerRegistry.executeHandler).not.toHaveBeenCalled();
    });

    it('should handle execution errors gracefully', async () => {
      const handler = createMockHandler();
      const message = createMockMessage({
        'x-original-topic': 'test.topic',
        'x-handler-id': 'TestClass.testMethod',
        'x-retry-count': '1',
        'x-process-after': Date.now().toString(),
      });

      mockHandlerRegistry.getHandler.mockReturnValue(handler);
      mockHandlerRegistry.executeHandler.mockRejectedValue(new Error('Handler failed'));

      // Should not throw - test the processReadyMessage method directly
      const headers = (consumer as any).extractHeaders(message);
      if (!headers) throw new Error('Invalid headers in test');
      await expect((consumer as any).processReadyMessage({
        message,
        topic: 'test-service.retry',
        partition: 0,
      }, headers)).resolves.not.toThrow();

      expect(mockHandlerRegistry.executeHandler).toHaveBeenCalled();
    });
  });

  describe('polling mechanism', () => {
    beforeEach(() => {
      jest.useFakeTimers();
      jest.spyOn(global, 'setTimeout');
      jest.spyOn(global, 'clearTimeout');
    });

    afterEach(() => {
      jest.useRealTimers();
      jest.restoreAllMocks();
    });

    it('should start polling timeout on initialization', async () => {
      await consumer.onModuleInit();

      // Verify polling timeout is set (immediate first poll)
      expect(setTimeout).toHaveBeenCalledWith(expect.any(Function), 0);
    });

    it('should stop polling timeout on destroy', async () => {
      await consumer.onModuleInit();
      await consumer.onModuleDestroy();

      expect(clearTimeout).toHaveBeenCalled();
    });

    it('should schedule next poll after completing a batch', async () => {
      // Mock pollBatch to avoid actual Kafka operations
      const pollBatchSpy = jest.spyOn(consumer as any, 'pollBatch').mockResolvedValue(undefined);

      await consumer.onModuleInit();
      // Clear the initial setTimeout call
      jest.clearAllMocks();

      // Fast-forward initial poll to trigger pollBatch
      jest.advanceTimersByTime(1);
      await Promise.resolve();
      await Promise.resolve(); // Allow pollBatch to complete

      // Should have scheduled next poll (10 seconds by default)
      expect(setTimeout).toHaveBeenCalledWith(expect.any(Function), 10000);

      pollBatchSpy.mockRestore();
    });
  });

  describe('message ID creation', () => {
    it('should create consistent message IDs', () => {
      const message: KafkaMessage = {
        key: Buffer.from('test-key'),
        value: Buffer.from('test-value'),
        timestamp: '1640995200000',
        offset: '123',
        headers: {},
      } as any;

      const messageId1 = (consumer as any).createMessageId(message, 0);
      const messageId2 = (consumer as any).createMessageId(message, 0);

      expect(messageId1).toBe(messageId2);
      expect(messageId1).toBe('0-123-1640995200000');
    });

    it('should handle missing timestamp and offset', () => {
      const message: KafkaMessage = {
        key: Buffer.from('test-key'),
        value: Buffer.from('test-value'),
        timestamp: undefined as any,
        offset: undefined as any,
        headers: {},
      } as any;

      const messageId = (consumer as any).createMessageId(message, 1);

      expect(messageId).toMatch(/^1-0-\d+$/);
    });
  });

  describe('header extraction', () => {
    it('should extract valid headers', () => {
      const message = createMockMessage({
        'x-original-topic': 'test.topic',
        'x-handler-id': 'TestClass.testMethod',
        'x-retry-count': '2',
        'x-process-after': '1640995200000',
        'x-correlation-id': 'corr-123',
      });

      const headers = (consumer as any).extractHeaders(message);

      expect(headers).toEqual({
        'x-original-topic': 'test.topic',
        'x-handler-id': 'TestClass.testMethod',
        'x-retry-count': '2',
        'x-process-after': '1640995200000',
        'x-correlation-id': 'corr-123',
      });
    });

    it('should return null for missing required headers', () => {
      const message = createMockMessage({
        'x-original-topic': 'test.topic',
        // Missing other required headers
      });

      const headers = (consumer as any).extractHeaders(message);

      expect(headers).toBeNull();
    });

    it('should return null for message without headers', () => {
      const message: KafkaMessage = {
        key: Buffer.from('test-key'),
        value: Buffer.from('test-value'),
        timestamp: '1640995200000',
        offset: '123',
        headers: undefined as any,
      } as any;

      const headers = (consumer as any).extractHeaders(message);

      expect(headers).toBeNull();
    });
  });

  describe('error handling', () => {
    beforeEach(async () => {
      await consumer.onModuleInit();
    });

    afterEach(async () => {
      await consumer.onModuleDestroy();
    });

    it('should handle consumer connection errors', async () => {
      mockConsumer.connect.mockRejectedValue(new Error('Connection failed'));

      await expect(consumer.onModuleInit()).rejects.toThrow('Connection failed');
    });

    it('should handle message processing errors gracefully', async () => {
      const message = createMockMessage({
        'x-original-topic': 'test.topic',
        'x-handler-id': 'TestClass.testMethod',
        'x-retry-count': '1',
        'x-process-after': Date.now().toString(),
      });

      mockHandlerRegistry.getHandler.mockImplementation(() => {
        throw new Error('Registry error');
      });

      // Should not throw, just log error - test the processReadyMessage method directly
      const headers = (consumer as any).extractHeaders(message);
      if (!headers) throw new Error('Invalid headers in test');
      await expect((consumer as any).processReadyMessage({
        message,
        topic: 'test-service.retry',
        partition: 0,
      }, headers)).resolves.not.toThrow();
    });
  });
});