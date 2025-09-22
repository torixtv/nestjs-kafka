import { Test, TestingModule } from '@nestjs/testing';
import { Injectable, Controller } from '@nestjs/common';
import { KafkaModule } from '../../src/core/kafka.module';
import { KafkaProducerService } from '../../src/core/kafka.producer';
import { KafkaController } from '../../src/core/kafka.controller';
import { EventHandler } from '../../src/decorators/event-handler.decorator';
import { KafkaRetryManager } from '../../src/services/kafka.retry-manager';
import { KafkaRetryConsumer } from '../../src/services/kafka.retry-consumer';
import { KafkaHandlerRegistry } from '../../src/services/kafka.registry';
import { KafkaTestHelper } from '../helpers/kafka-test.helper';
import { registerKafkaMicroservice, startKafkaMicroservice } from '../../src/core/kafka.bootstrap';
import { KafkaBootstrapService } from '../../src/services/kafka.bootstrap.service';
import { INestApplication } from '@nestjs/common';

// Test controller that fails predictably for retry testing
@Injectable()
@Controller()
class RetryTestController extends KafkaController {
  public processedMessages: Array<{
    payload: any;
    timestamp: string;
    attempt: number;
    processingTime: number;
  }> = [];

  public failures: Array<{
    payload: any;
    timestamp: string;
    attempt: number;
    error: string;
  }> = [];

  private failureCounts = new Map<string, number>();
  private startTime = Date.now();

  @EventHandler('test.retry.message', {
    retry: {
      enabled: true,
      attempts: 3,
      baseDelay: 2000, // 2 seconds for faster testing
      maxDelay: 10000,
      backoff: 'exponential',
    },
  })
  async handleRetryMessage(payload: any) {
    const attempt = this.getAttemptCount(payload.id);
    const processingTime = Date.now() - this.startTime;

    // Fail first 2 attempts, succeed on 3rd
    if (payload.shouldFail && attempt < 2) {
      this.failures.push({
        payload,
        timestamp: new Date().toISOString(),
        attempt: attempt + 1,
        error: `Simulated failure ${attempt + 1}`,
      });

      this.incrementAttemptCount(payload.id);
      throw new Error(`Simulated failure ${attempt + 1} for ${payload.id}`);
    }

    // Success
    this.processedMessages.push({
      payload,
      timestamp: new Date().toISOString(),
      attempt: attempt + 1,
      processingTime,
    });
  }

  @EventHandler('test.immediate.success', {
    retry: {
      enabled: true,
      attempts: 3,
      baseDelay: 1000,
      maxDelay: 5000,
      backoff: 'linear',
    },
  })
  async handleImmediateSuccess(payload: any) {
    const processingTime = Date.now() - this.startTime;
    this.processedMessages.push({
      payload,
      timestamp: new Date().toISOString(),
      attempt: 1,
      processingTime,
    });
  }

  @EventHandler('test.always.fail', {
    retry: {
      enabled: true,
      attempts: 2, // Low attempts for faster test
      baseDelay: 1000,
      maxDelay: 3000,
      backoff: 'linear',
    },
  })
  async handleAlwaysFail(payload: any) {
    const attempt = this.getAttemptCount(payload.id);
    this.failures.push({
      payload,
      timestamp: new Date().toISOString(),
      attempt: attempt + 1,
      error: `Always fails attempt ${attempt + 1}`,
    });

    this.incrementAttemptCount(payload.id);
    throw new Error(`Always fails for ${payload.id}`);
  }

  private getAttemptCount(id: string): number {
    return this.failureCounts.get(id) || 0;
  }

  private incrementAttemptCount(id: string): void {
    const current = this.getAttemptCount(id);
    this.failureCounts.set(id, current + 1);
  }

  reset() {
    this.processedMessages = [];
    this.failures = [];
    this.failureCounts.clear();
    this.startTime = Date.now();
  }
}

describe('Retry Flow Integration (Real Kafka)', () => {
  let app: INestApplication;
  let module: TestingModule;
  let producer: KafkaProducerService;
  let retryManager: KafkaRetryManager;
  let retryConsumer: KafkaRetryConsumer;
  let testController: RetryTestController;
  let testHelper: KafkaTestHelper;
  let testTopics: string[];
  let handlerRegistry: KafkaHandlerRegistry;
  let bootstrapService: KafkaBootstrapService;

  beforeAll(async () => {
    testHelper = new KafkaTestHelper({
      brokers: ['localhost:9092'],
      clientId: 'retry-flow-test',
      groupId: 'retry-flow-test-group',
    });

    // Wait for RedPanda to be available
    await testHelper.waitForKafka();
    await testHelper.connect();
  });

  afterAll(async () => {
    await testHelper.disconnect();
  });

  beforeEach(async () => {
    console.error('🔧 DEBUG: beforeEach starting...'); // Force output with console.error
    const clientId = `retry-flow-test-${Date.now()}`;
    testTopics = [
      'test.retry.message',
      'test.immediate.success',
      'test.always.fail',
    ];

    console.log('🚀 Starting test setup with clientId:', clientId);
    console.error('DEBUG: Test setup starting - this should appear!'); // Using console.error to force output

    // Debug the topic setup
    console.log('Test topics:', testTopics);
    console.log('Retry topic will be:', `${clientId}.retry`);

    module = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({
          client: {
            clientId,
            brokers: ['localhost:9092'],
          },
          consumer: {
            groupId: `${clientId}-group`,
          },
          retry: {
            enabled: true,
            attempts: 3,
            baseDelay: 2000,
            maxDelay: 10000,
            backoff: 'exponential',
          },
        }),
      ],
      controllers: [RetryTestController],
    }).compile();

    app = module.createNestApplication();

    producer = module.get<KafkaProducerService>(KafkaProducerService);
    retryManager = module.get<KafkaRetryManager>(KafkaRetryManager);
    retryConsumer = module.get<KafkaRetryConsumer>(KafkaRetryConsumer);
    testController = module.get<RetryTestController>(RetryTestController);
    handlerRegistry = module.get<KafkaHandlerRegistry>(KafkaHandlerRegistry);
    bootstrapService = module.get<KafkaBootstrapService>(KafkaBootstrapService);


    // Create topics
    await testHelper.createTopics([
      ...testTopics,
      retryManager.getRetryTopicName(),
    ]);

    // Register and start Kafka microservice
    // IMPORTANT: Exclude retry topic from main microservice subscription
    // The retry consumer should be the ONLY consumer for the retry topic
    await registerKafkaMicroservice(app, {
      overrides: {
        subscribe: {
          topics: testTopics, // Only subscribe to test topics, NOT retry topic
          fromBeginning: false,
        },
      },
    });

    await app.init();

    // Start the microservice (now includes automatic bootstrap service initialization)
    console.log('🔧 Debug: About to start microservice...');
    let serviceStatus: any;
    try {
      await startKafkaMicroservice(app);
      console.log('🔧 Debug: Microservice started, checking bootstrap service...');

      // Wait for bootstrap service to complete initialization
      console.log('🔧 Debug: Waiting for bootstrap service initialization...');
      await bootstrapService.waitForInitialization(10000);
      console.log('🔧 Debug: Bootstrap service wait completed');

      // Verify all services are properly initialized
      serviceStatus = bootstrapService.getServiceStatus();
      console.log('🔧 Service status after initialization:', serviceStatus);
    } catch (error) {
      console.error('🔧 Debug: Error during startup:', error);
      throw error;
    }

    if (!serviceStatus.initialized) {
      throw new Error('Bootstrap service failed to initialize Kafka services');
    }

    if (!serviceStatus.retryConsumerRunning) {
      throw new Error('Retry consumer failed to start');
    }

    if (serviceStatus.handlerCount === 0) {
      throw new Error('No handlers were discovered by the registry');
    }

    // Reset test state
    testController.reset();
  });

  afterEach(async () => {
    if (app) {
      await app.close();
    }
    if (module) {
      await module.close();
    }

    // Clean up topics
    await testHelper.deleteTopics([
      ...testTopics,
      retryManager.getRetryTopicName(),
    ]);
  });

  describe('Successful Processing', () => {
    it('should process messages immediately without retry when no failure occurs', async () => {
      const testData = {
        id: 'success-test-1',
        data: 'test data',
        timestamp: new Date().toISOString(),
      };

      await producer.send('test.immediate.success', {
        key: testData.id,
        value: testData,
      });

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 3000));

      expect(testController.processedMessages).toHaveLength(1);
      expect(testController.processedMessages[0]).toMatchObject({
        payload: testData,
        attempt: 1,
      });
      expect(testController.failures).toHaveLength(0);
    });
  });

  describe('Retry Flow with Delays', () => {
    it('should retry failed messages with exponential backoff delays', async () => {
      console.error('🔧 DEBUG: Test starting...'); // Force output
      const testData = {
        id: 'retry-test-1',
        shouldFail: true,
        data: 'test retry data',
        timestamp: new Date().toISOString(),
      };

      const startTime = Date.now();

      await producer.send('test.retry.message', {
        key: testData.id,
        value: testData,
      });

      // Wait for all retries to complete (2 failures + 1 success)
      // With 2s base delay and exponential backoff: 2s, 4s delays
      // Plus polling intervals and processing time
      await new Promise(resolve => setTimeout(resolve, 15000));

      // Debug: Check what we have so far
      console.log('Debug - failures:', testController.failures.length);
      console.log('Debug - successes:', testController.processedMessages.length);
      console.log('Debug - retry consumer metrics:', retryConsumer.getMetrics());


      const endTime = Date.now();
      const totalTime = endTime - startTime;

      // Should have 2 failures and 1 success
      expect(testController.failures).toHaveLength(2);
      expect(testController.processedMessages).toHaveLength(1);

      // Verify failure attempts
      expect(testController.failures[0].attempt).toBe(1);
      expect(testController.failures[1].attempt).toBe(2);

      // Verify successful processing on 3rd attempt
      expect(testController.processedMessages[0]).toMatchObject({
        payload: testData,
        attempt: 3,
      });

      // Verify timing - should have delays between attempts
      // At minimum: 2s (first retry) + 4s (second retry) = 6s + processing time
      expect(totalTime).toBeGreaterThan(6000);
    }, 30000);

    it('should handle multiple concurrent retry messages', async () => {
      const messages = [
        { id: 'concurrent-1', shouldFail: true, data: 'data-1' },
        { id: 'concurrent-2', shouldFail: true, data: 'data-2' },
        { id: 'concurrent-3', shouldFail: true, data: 'data-3' },
      ];

      // Send all messages at once
      await Promise.all(
        messages.map(msg =>
          producer.send('test.retry.message', {
            key: msg.id,
            value: msg,
          })
        )
      );

      // Wait for all retries to complete
      await new Promise(resolve => setTimeout(resolve, 20000));

      // Each message should have 2 failures and 1 success
      expect(testController.failures).toHaveLength(6); // 2 failures × 3 messages
      expect(testController.processedMessages).toHaveLength(3); // 1 success × 3 messages

      // Verify all messages were eventually processed
      const processedIds = testController.processedMessages.map(m => m.payload.id);
      expect(processedIds).toEqual(expect.arrayContaining(['concurrent-1', 'concurrent-2', 'concurrent-3']));
    }, 35000);
  });

  describe('Retry Topic and Consumer', () => {
    it('should create retry topic and consumer correctly', async () => {
      const retryTopicName = retryManager.getRetryTopicName();
      expect(retryTopicName).toMatch(/retry$/);

      // Verify retry topic exists
      const topicExists = await retryManager.retryTopicExists();
      expect(topicExists).toBe(true);

      // Verify retry consumer is running
      expect(retryConsumer.isRetryConsumerRunning()).toBe(true);
    });

    it('should send retry messages with correct headers', async () => {
      const testData = {
        id: 'header-test-1',
        shouldFail: true,
        data: 'header test',
      };

      await producer.send('test.retry.message', {
        key: testData.id,
        value: testData,
      });

      // Wait for first failure and retry message to be sent
      await new Promise(resolve => setTimeout(resolve, 5000));

      // Capture retry messages from retry topic
      const retryMessages = await testHelper.waitForMessages(
        retryManager.getRetryTopicName(),
        {
          expectedCount: 1,
          timeout: 10000,
          fromBeginning: true,
          includeHeaders: true,
        }
      );

      expect(retryMessages).toHaveLength(1);

      // Verify retry message has correct headers
      const retryMessage = retryMessages[0];
      expect(retryMessage.headers).toHaveProperty('x-original-topic');
      expect(retryMessage.headers).toHaveProperty('x-handler-id');
      expect(retryMessage.headers).toHaveProperty('x-retry-count');
      expect(retryMessage.headers).toHaveProperty('x-process-after');

      // Verify header values
      expect(retryMessage.headers['x-original-topic']).toBe('test.retry.message');
      expect(retryMessage.headers['x-retry-count']).toBe('1');

      // Process-after should be in the future
      const processAfter = parseInt(retryMessage.headers['x-process-after']);
      expect(processAfter).toBeGreaterThan(Date.now() - 10000); // Allow for processing time
    }, 20000);
  });

  describe('Max Retries Exceeded', () => {
    it('should stop retrying after max attempts exceeded', async () => {
      const testData = {
        id: 'max-retries-test-1',
        data: 'always fail test',
        timestamp: new Date().toISOString(),
      };

      await producer.send('test.always.fail', {
        key: testData.id,
        value: testData,
      });

      // Wait for all retries to complete (should be 2 attempts total)
      await new Promise(resolve => setTimeout(resolve, 10000));

      // Should have 2 failures and no success
      expect(testController.failures).toHaveLength(2);
      expect(testController.processedMessages).toHaveLength(0);

      // Verify attempts
      expect(testController.failures[0].attempt).toBe(1);
      expect(testController.failures[1].attempt).toBe(2);
    }, 15000);
  });

  describe('Polling Mechanism', () => {
    it('should process messages when timestamps are ready', async () => {
      const testData = {
        id: 'timing-test-1',
        shouldFail: true,
        data: 'timing test',
      };

      const startTime = Date.now();

      await producer.send('test.retry.message', {
        key: testData.id,
        value: testData,
      });

      // Wait for processing to complete
      await new Promise(resolve => setTimeout(resolve, 12000));

      // Verify that retries happened with proper timing
      expect(testController.failures).toHaveLength(2);
      expect(testController.processedMessages).toHaveLength(1);

      // The successful message should have been processed after delays
      const successMessage = testController.processedMessages[0];
      const processingTime = successMessage.processingTime;

      // Should take at least the delay times: 2s + 4s = 6s
      expect(processingTime).toBeGreaterThan(6000);
    }, 20000);
  });
});