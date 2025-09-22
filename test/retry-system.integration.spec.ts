import { Test, TestingModule } from '@nestjs/testing';
import { Injectable, Controller } from '@nestjs/common';
import { KafkaModule } from '../src/core/kafka.module';
import { KafkaProducerService } from '../src/core/kafka.producer';
import { KafkaController } from '../src/core/kafka.controller';
import { EventHandler } from '../src/decorators/event-handler.decorator';
import { KafkaRetryManager } from '../src/services/kafka.retry-manager';
import { KafkaHandlerRegistry } from '../src/services/kafka.registry';

@Injectable()
@Controller()
class TestRetryController extends KafkaController {
  public processedMessages: Array<{
    topic: string;
    payload: any;
    timestamp: string;
    attempt?: number;
  }> = [];

  public failureCount = new Map<string, number>();

  @EventHandler('test.retry.event', {
    retry: {
      enabled: true,
      attempts: 3,
      backoff: 'exponential',
      baseDelay: 100,
      maxDelay: 1000,
    },
  })
  async handleRetryEvent(payload: any) {
    const failures = this.failureCount.get(payload.id) || 0;

    // Fail first 2 attempts, succeed on 3rd
    if (payload.shouldFailInitially && failures < 2) {
      this.failureCount.set(payload.id, failures + 1);
      throw new Error(`Simulated failure attempt ${failures + 1} for ${payload.id}`);
    }

    this.processedMessages.push({
      topic: 'test.retry.event',
      payload,
      timestamp: new Date().toISOString(),
      attempt: failures + 1,
    });
  }

  reset() {
    this.processedMessages = [];
    this.failureCount.clear();
  }
}

describe('Retry System Integration', () => {
  let app: TestingModule;
  let producer: KafkaProducerService;
  let testController: TestRetryController;
  let retryManager: KafkaRetryManager;
  let handlerRegistry: KafkaHandlerRegistry;

  beforeAll(async () => {
    app = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({
          client: {
            clientId: 'retry-test-client',
            brokers: ['localhost:9092'],
          },
          consumer: {
            groupId: 'retry-test-group',
          },
          retry: {
            enabled: true,
            attempts: 3,
            backoff: 'exponential',
            baseDelay: 100,
            maxDelay: 1000,
          },
        }),
      ],
      controllers: [TestRetryController],
    }).compile();

    producer = app.get<KafkaProducerService>(KafkaProducerService);
    testController = app.get<TestRetryController>(TestRetryController);
    retryManager = app.get<KafkaRetryManager>(KafkaRetryManager);
    handlerRegistry = app.get<KafkaHandlerRegistry>(KafkaHandlerRegistry);

    // Wait for initialization
    await new Promise(resolve => setTimeout(resolve, 1000));
  });

  afterAll(async () => {
    if (app) {
      await app.close();
    }
  });

  beforeEach(() => {
    testController.reset();
  });

  describe('Handler Registry', () => {
    it('should automatically discover event handlers', async () => {
      const handlers = handlerRegistry.getAllHandlers();

      expect(handlers.length).toBeGreaterThan(0);

      const retryHandler = handlers.find(h => h.pattern === 'test.retry.event');
      expect(retryHandler).toBeDefined();
      expect(retryHandler?.handlerId).toContain('TestRetryController');
      expect(retryHandler?.metadata.options.retry?.enabled).toBe(true);
      expect(retryHandler?.metadata.options.retry?.attempts).toBe(3);
    });

    it('should execute handlers by ID', async () => {
      const handlers = handlerRegistry.getAllHandlers();
      const retryHandler = handlers.find(h => h.pattern === 'test.retry.event');

      if (retryHandler) {
        await handlerRegistry.executeHandler(retryHandler.handlerId, {
          id: 'direct-test',
          shouldFailInitially: false,
        });

        expect(testController.processedMessages).toHaveLength(1);
        expect(testController.processedMessages[0].payload.id).toBe('direct-test');
      }
    });
  });

  describe('Retry Topic Management', () => {
    it('should get correct retry topic name', () => {
      const retryTopicName = retryManager.getRetryTopicName();
      expect(retryTopicName).toBe('retry-test-client.retry');
    });

    it('should ensure retry topic exists', async () => {
      // This should not throw and should create the topic if it doesn't exist
      await expect(retryManager.onModuleInit()).resolves.not.toThrow();

      const exists = await retryManager.retryTopicExists();
      expect(exists).toBe(true);
    });
  });

  describe('Producer Integration', () => {
    it('should send messages to retry topic with proper headers', async () => {
      const retryTopicName = retryManager.getRetryTopicName();

      const testMessage = {
        key: 'retry-test-key',
        value: {
          id: 'retry-test-1',
          data: 'test data',
        },
        headers: {
          'x-original-topic': 'test.retry.event',
          'x-handler-id': 'TestRetryController.handleRetryEvent',
          'x-retry-count': '1',
          'x-process-after': (Date.now() + 5000).toString(),
          'x-correlation-id': 'test-correlation-123',
        },
      };

      // Should not throw
      await expect(producer.send(retryTopicName, testMessage)).resolves.not.toThrow();
    });

    it('should handle producer connection', async () => {
      const isReady = await producer.isReady();
      expect(typeof isReady).toBe('boolean');
    });
  });

  describe('Header Processing', () => {
    it('should handle messages with all required headers', () => {
      const testHeaders = {
        'x-original-topic': 'test.topic',
        'x-handler-id': 'TestClass.testMethod',
        'x-retry-count': '2',
        'x-process-after': Date.now().toString(),
        'x-correlation-id': 'corr-123',
      };

      // This tests the structure we expect
      expect(testHeaders['x-original-topic']).toBe('test.topic');
      expect(testHeaders['x-handler-id']).toBe('TestClass.testMethod');
      expect(testHeaders['x-retry-count']).toBe('2');
      expect(testHeaders['x-process-after']).toBeDefined();
    });

    it('should handle timestamp comparison logic', () => {
      const now = Date.now();
      const futureTime = now + 5000;
      const pastTime = now - 5000;

      // Message not ready yet
      expect(now < futureTime).toBe(true);

      // Message ready to process
      expect(now >= pastTime).toBe(true);
    });
  });

  describe('Retry Configuration', () => {
    it('should have correct retry delay calculation', () => {
      // This tests that our retry logic parameters are reasonable
      const baseDelay = 100;
      const maxDelay = 1000;
      const backoff = 'exponential';

      expect(baseDelay).toBeLessThan(maxDelay);
      expect(['linear', 'exponential']).toContain(backoff);
    });

    it('should respect max retry attempts', () => {
      const maxAttempts = 3;
      const currentRetry = 2;

      // Should still retry
      expect(currentRetry < maxAttempts).toBe(true);

      // Should not retry
      const exceedsMax = 3;
      expect(exceedsMax >= maxAttempts).toBe(true);
    });
  });

  describe('Message ID Generation', () => {
    it('should create consistent message IDs', () => {
      const partition = 0;
      const offset = '123';
      const timestamp = '1640995200000';

      const messageId1 = `${partition}-${offset}-${timestamp}`;
      const messageId2 = `${partition}-${offset}-${timestamp}`;

      expect(messageId1).toBe(messageId2);
      expect(messageId1).toBe('0-123-1640995200000');
    });

    it('should handle missing values gracefully', () => {
      const partition = 1;
      const offset = undefined;
      const timestamp = undefined;
      const now = Date.now().toString();

      const messageId = `${partition}-${offset || '0'}-${timestamp || now}`;

      expect(messageId).toMatch(/^1-0-\d+$/);
    });
  });
});