import { Test, TestingModule } from '@nestjs/testing';
import { ExecutionContext, CallHandler } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { KafkaContext } from '@nestjs/microservices';
import { of, throwError } from 'rxjs';
import { RetryInterceptor } from './retry.interceptor';
import { EVENT_HANDLER_METADATA, KAFKA_PLUGINS } from '../core/kafka.constants';
import { KafkaPlugin } from '../interfaces/kafka.interfaces';
import { KafkaProducerService } from '../core/kafka.producer';
import { KafkaRetryManager } from '../services/kafka.retry-manager';
import { KafkaHandlerRegistry } from '../services/kafka.registry';

describe('RetryInterceptor', () => {
  let interceptor: RetryInterceptor;
  let reflector: Reflector;
  let mockPlugins: jest.Mocked<KafkaPlugin>[];

  beforeEach(async () => {
    mockPlugins = [
      {
        name: 'test-plugin',
        onFailure: jest.fn(),
      },
      {
        name: 'dlq-plugin',
        onFailure: jest.fn(),
      },
    ];

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        RetryInterceptor,
        {
          provide: Reflector,
          useValue: {
            get: jest.fn(),
          },
        },
        {
          provide: KafkaProducerService,
          useValue: {
            send: jest.fn(),
            sendBatch: jest.fn(),
            sendTransaction: jest.fn(),
            connect: jest.fn(),
            onModuleDestroy: jest.fn(),
            isReady: jest.fn(),
          },
        },
        {
          provide: KafkaRetryManager,
          useValue: {
            getRetryTopicName: jest.fn().mockReturnValue('test-client.retry'),
            ensureRetryTopicExists: jest.fn(),
            deleteRetryTopic: jest.fn(),
            getTopicMetadata: jest.fn(),
            retryTopicExists: jest.fn(),
          },
        },
        {
          provide: KafkaHandlerRegistry,
          useValue: {
            getHandler: jest.fn(),
            getHandlersForPattern: jest.fn().mockReturnValue([{
              handlerId: 'TestClass.testHandler',
              pattern: 'test-topic',
              instance: {},
              methodName: 'testHandler',
              metadata: {},
            }]),
            getAllHandlers: jest.fn(),
            getAllPatterns: jest.fn(),
            executeHandler: jest.fn(),
            hasHandler: jest.fn(),
          },
        },
        {
          provide: KAFKA_PLUGINS,
          useValue: mockPlugins,
        },
      ],
    }).compile();

    interceptor = module.get<RetryInterceptor>(RetryInterceptor);
    reflector = module.get<Reflector>(Reflector);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  const createMockExecutionContext = (contextType: string = 'rpc', metadata?: any): ExecutionContext => {
    const mockKafkaContext = {
      getMessage: jest.fn().mockReturnValue({
        headers: {},
        offset: '123',
        timestamp: '1640995200000',
      }),
      getPartition: jest.fn().mockReturnValue(0),
      getTopic: jest.fn().mockReturnValue('test-topic'),
    } as any;

    return {
      getType: jest.fn().mockReturnValue(contextType),
      getHandler: jest.fn().mockReturnValue(function testHandler() {}),
      getClass: jest.fn().mockReturnValue({ name: 'TestClass' }),
      switchToRpc: jest.fn().mockReturnValue({
        getContext: jest.fn().mockReturnValue(mockKafkaContext),
      }),
    } as any;
  };

  const createMockCallHandler = (shouldSucceed: boolean = true): CallHandler => {
    return {
      handle: jest.fn().mockReturnValue(
        shouldSucceed ? of('success') : throwError(new Error('Processing failed'))
      ),
    } as any;
  };

  describe('intercept', () => {
    it('should skip non-RPC contexts', () => {
      const context = createMockExecutionContext('http');
      const next = createMockCallHandler();

      const result$ = interceptor.intercept(context, next);

      expect(next.handle).toHaveBeenCalled();
      expect(reflector.get).not.toHaveBeenCalled();
    });

    it('should execute normally when retry is not enabled', () => {
      const context = createMockExecutionContext('rpc');
      const next = createMockCallHandler();

      (reflector.get as jest.Mock).mockReturnValue(null); // No metadata

      const result$ = interceptor.intercept(context, next);

      expect(next.handle).toHaveBeenCalled();
      expect(reflector.get).toHaveBeenCalledWith(EVENT_HANDLER_METADATA, expect.any(Function));
    });

    it('should execute normally when retry is disabled in metadata', () => {
      const context = createMockExecutionContext('rpc');
      const next = createMockCallHandler();

      const metadata = {
        options: {
          retry: { enabled: false },
        },
      };

      (reflector.get as jest.Mock).mockReturnValue(metadata);

      const result$ = interceptor.intercept(context, next);

      expect(next.handle).toHaveBeenCalled();
    });
  });

  describe('retry logic', () => {
    it('should handle successful execution without retry', (done) => {
      const context = createMockExecutionContext('rpc');
      const next = createMockCallHandler(true);

      const metadata = {
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
      };

      (reflector.get as jest.Mock).mockReturnValue(metadata);

      const result$ = interceptor.intercept(context, next);

      result$.subscribe({
        next: (value) => {
          expect(value).toBe('success');
          expect(next.handle).toHaveBeenCalledTimes(1);
          done();
        },
        error: done,
      });
    });

    it('should handle failure and trigger plugins', (done) => {
      const context = createMockExecutionContext('rpc');
      const next = createMockCallHandler(false);

      // Mock message with no retry count (first attempt)
      const mockKafkaContext = context.switchToRpc().getContext();
      mockKafkaContext.getMessage.mockReturnValue({
        headers: {},
        offset: '123',
        timestamp: '1640995200000',
      });

      const metadata = {
        options: {
          retry: {
            enabled: true,
            attempts: 3,
            baseDelay: 100,
            maxDelay: 1000,
            backoff: 'exponential',
          },
          dlq: { enabled: false },
        },
      };

      (reflector.get as jest.Mock).mockReturnValue(metadata);

      const result$ = interceptor.intercept(context, next);

      result$.subscribe({
        next: () => {
          // Should succeed (return) - interceptor handles retry by publishing to retry topic
          expect(mockPlugins[0].onFailure).toHaveBeenCalled();
          expect(mockPlugins[1].onFailure).toHaveBeenCalled();
          done();
        },
        error: (error) => {
          done(new Error(`Should not have failed: ${error.message}`));
        },
      });
    });

    it('should extract retry count from headers', (done) => {
      const context = createMockExecutionContext('rpc');
      const next = createMockCallHandler(false);

      // Mock message with retry count in headers
      const mockKafkaContext = context.switchToRpc().getContext();
      mockKafkaContext.getMessage.mockReturnValue({
        headers: { 'x-retry-count': '2' },
        offset: '123',
        timestamp: '1640995200000',
      });

      const metadata = {
        options: {
          retry: {
            enabled: true,
            attempts: 3,
            baseDelay: 100,
            maxDelay: 1000,
            backoff: 'exponential',
          },
          dlq: { enabled: false },
        },
      };

      (reflector.get as jest.Mock).mockReturnValue(metadata);

      const result$ = interceptor.intercept(context, next);

      result$.subscribe({
        next: () => {
          // Should succeed (return) - interceptor handles retry by publishing to retry topic
          // Should still retry since 2 < 3
          done();
        },
        error: (error) => {
          done(new Error(`Should not have failed: ${error.message}`));
        },
      });
    });

    it('should trigger DLQ when max retries exceeded', (done) => {
      const context = createMockExecutionContext('rpc');
      const next = createMockCallHandler(false);

      // Mock message with retry count exceeding max attempts
      const mockKafkaContext = context.switchToRpc().getContext();
      mockKafkaContext.getMessage.mockReturnValue({
        headers: { 'x-retry-count': '3' }, // Exceeds max attempts
        offset: '123',
        timestamp: '1640995200000',
      });

      const metadata = {
        options: {
          retry: {
            enabled: true,
            attempts: 3,
            baseDelay: 100,
            maxDelay: 1000,
            backoff: 'exponential',
          },
          dlq: { enabled: true },
        },
      };

      (reflector.get as jest.Mock).mockReturnValue(metadata);

      // Add a DLQ plugin
      const dlqPlugin = {
        name: 'dlq',
        handleFailure: jest.fn().mockResolvedValue(undefined),
      };
      mockPlugins.push(dlqPlugin as any);

      const result$ = interceptor.intercept(context, next);

      result$.subscribe({
        next: (value) => {
          // Should succeed (not throw) when message is sent to DLQ
          expect(dlqPlugin.handleFailure).toHaveBeenCalled();
          done();
        },
        error: done,
      });
    });

    it('should extract correlation ID from headers', () => {
      const context = createMockExecutionContext('rpc');
      const next = createMockCallHandler(true);

      const mockKafkaContext = context.switchToRpc().getContext();
      mockKafkaContext.getMessage.mockReturnValue({
        headers: { 'x-correlation-id': 'test-correlation-123' },
        offset: '123',
        timestamp: '1640995200000',
      });

      const metadata = {
        options: {
          retry: {
            enabled: true,
            attempts: 3,
            baseDelay: 100,
            maxDelay: 1000,
            backoff: 'exponential',
          },
          dlq: { enabled: false },
        },
      };

      (reflector.get as jest.Mock).mockReturnValue(metadata);

      interceptor.intercept(context, next);

      // This test mainly ensures the correlation ID extraction doesn't throw
      expect(mockKafkaContext.getMessage).toHaveBeenCalled();
    });
  });

  describe('plugin integration', () => {
    it('should call all plugin failure hooks', (done) => {
      const context = createMockExecutionContext('rpc');
      const next = createMockCallHandler(false);

      const metadata = {
        options: {
          retry: { enabled: false },
          dlq: { enabled: false },
        },
      };

      (reflector.get as jest.Mock).mockReturnValue(metadata);

      const result$ = interceptor.intercept(context, next);

      result$.subscribe({
        next: () => {
          done(new Error('Should have failed'));
        },
        error: (error) => {
          expect(mockPlugins[0].onFailure).toHaveBeenCalledWith(error);
          expect(mockPlugins[1].onFailure).toHaveBeenCalledWith(error);
          done();
        },
      });
    });

    it('should handle plugin failures gracefully', (done) => {
      const context = createMockExecutionContext('rpc');
      const next = createMockCallHandler(false);

      // Make one plugin fail
      mockPlugins[0].onFailure = jest.fn().mockRejectedValue(new Error('Plugin failed'));

      const metadata = {
        options: {
          retry: { enabled: false },
          dlq: { enabled: false },
        },
      };

      (reflector.get as jest.Mock).mockReturnValue(metadata);

      const result$ = interceptor.intercept(context, next);

      result$.subscribe({
        next: () => {
          done(new Error('Should have failed'));
        },
        error: (error) => {
          expect(error.message).toBe('Processing failed'); // Original error, not plugin error
          expect(mockPlugins[1].onFailure).toHaveBeenCalled(); // Other plugins should still run
          done();
        },
      });
    });

    it('should find and use DLQ plugin for dead lettering', (done) => {
      const context = createMockExecutionContext('rpc');
      const next = createMockCallHandler(false);

      const dlqPlugin = {
        name: 'dlq',
        handleFailure: jest.fn().mockResolvedValue(undefined),
      };
      mockPlugins.push(dlqPlugin as any);

      const mockKafkaContext = context.switchToRpc().getContext();
      mockKafkaContext.getMessage.mockReturnValue({
        headers: { 'x-retry-count': '3' }, // Max retries exceeded
        offset: '123',
        timestamp: '1640995200000',
      });

      const metadata = {
        options: {
          retry: {
            enabled: true,
            attempts: 3,
          },
          dlq: { enabled: true },
        },
      };

      (reflector.get as jest.Mock).mockReturnValue(metadata);

      const result$ = interceptor.intercept(context, next);

      result$.subscribe({
        next: () => {
          expect(dlqPlugin.handleFailure).toHaveBeenCalled();
          done();
        },
        error: done,
      });
    });
  });

  describe('message ID creation', () => {
    it('should create consistent message IDs', () => {
      const context = createMockExecutionContext('rpc');
      const next = createMockCallHandler(true);

      const mockKafkaContext = context.switchToRpc().getContext();
      mockKafkaContext.getMessage.mockReturnValue({
        headers: {},
        offset: '456',
        timestamp: '1640995300000',
      });
      mockKafkaContext.getPartition.mockReturnValue(2);

      const metadata = {
        options: {
          retry: {
            enabled: true,
            attempts: 3,
          },
        },
      };

      (reflector.get as jest.Mock).mockReturnValue(metadata);

      interceptor.intercept(context, next);

      // The message ID should be created from partition-offset-timestamp
      // This test verifies the flow doesn't break during ID creation
      expect(mockKafkaContext.getPartition).toHaveBeenCalled();
      expect(mockKafkaContext.getMessage).toHaveBeenCalled();
    });
  });
});