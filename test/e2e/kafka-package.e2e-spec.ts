import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { Injectable, Controller } from '@nestjs/common';
import { KafkaModule } from '../../src/core/kafka.module';
import { KafkaProducerService } from '../../src/core/kafka.producer';
import { KafkaController } from '../../src/core/kafka.controller';
import { EventHandler, SimpleEventHandler } from '../../src/decorators/event-handler.decorator';
import { DLQPlugin } from '../../src/plugins/dlq/dlq.plugin';
import { registerKafkaMicroservice, startKafkaMicroservice } from '../../src/core/kafka.bootstrap';
import { KafkaTestHelper } from '../helpers/kafka-test.helper';

// E2E Test Service
@Injectable()
@Controller()
class E2ETestController extends KafkaController {
  public processedMessages: Array<{
    topic: string;
    payload: any;
    timestamp: string;
    attempt?: number;
  }> = [];

  public failureCount = new Map<string, number>();

  @SimpleEventHandler('e2e.simple.event')
  async handleSimpleEvent(payload: any) {
    this.processedMessages.push({
      topic: 'e2e.simple.event',
      payload,
      timestamp: new Date().toISOString(),
    });
  }

  @EventHandler('e2e.retry.success', {
    retry: {
      enabled: true,
      attempts: 3,
      backoff: 'exponential',
      baseDelay: 100,
      maxDelay: 1000,
    },
  })
  async handleRetrySuccessEvent(payload: any) {
    const failures = this.failureCount.get(payload.id) || 0;

    // Fail first 2 attempts, succeed on 3rd
    if (payload.shouldFailInitially && failures < 2) {
      this.failureCount.set(payload.id, failures + 1);
      throw new Error(`Simulated failure attempt ${failures + 1} for ${payload.id}`);
    }

    this.processedMessages.push({
      topic: 'e2e.retry.success',
      payload,
      timestamp: new Date().toISOString(),
      attempt: failures + 1,
    });
  }

  @EventHandler('e2e.dlq.event', {
    retry: {
      enabled: true,
      attempts: 2,
      baseDelay: 50,
    },
    dlq: {
      enabled: true,
    },
  })
  async handleDlqEvent(payload: any) {
    if (payload.alwaysFail) {
      throw new Error(`DLQ test failure for ${payload.id}`);
    }

    this.processedMessages.push({
      topic: 'e2e.dlq.event',
      payload,
      timestamp: new Date().toISOString(),
    });
  }

  reset() {
    this.processedMessages = [];
    this.failureCount.clear();
  }
}

describe('Kafka Package (E2E)', () => {
  let app: INestApplication;
  let module: TestingModule;
  let producer: KafkaProducerService;
  let testController: E2ETestController;
  let testHelper: KafkaTestHelper;
  let dlqPlugin: DLQPlugin;

  const testTopics = [
    'e2e.simple.event',
    'e2e.retry.success',
    'e2e.dlq.event',
    'e2e.dlq.e2e.dlq.event', // DLQ topic
  ];

  beforeAll(async () => {
    testHelper = new KafkaTestHelper(KafkaTestHelper.getDefaultConfig());
    await testHelper.waitForKafka();
    await testHelper.connect();
  });

  afterAll(async () => {
    await testHelper.disconnect();
  });

  beforeEach(async () => {
    // Create a placeholder producer for DLQ plugin (will be replaced after module creation)
    dlqPlugin = new DLQPlugin({} as any, {
      topicPrefix: 'e2e.dlq',
      includeErrorDetails: true,
      includeOriginalMessage: true,
    });

    module = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({
          client: {
            clientId: 'e2e-test-client',
            brokers: ['localhost:9092'],
          },
          consumer: {
            groupId: 'e2e-test-group',
          },
          subscriptions: {
            topics: testTopics,
            fromBeginning: true,
          },
          retry: {
            enabled: true,
            attempts: 3,
            backoff: 'exponential',
            baseDelay: 100,
            maxDelay: 1000,
          },
          dlq: {
            enabled: true,
          },
          plugins: [dlqPlugin],
        }),
      ],
      controllers: [E2ETestController],
    }).compile();

    app = module.createNestApplication();

    producer = module.get<KafkaProducerService>(KafkaProducerService);
    testController = module.get<E2ETestController>(E2ETestController);

    // Inject the actual producer into the DLQ plugin
    (dlqPlugin as any).producer = producer;

    await testHelper.createTopics(testTopics);

    await registerKafkaMicroservice(app);
    await app.init();
    await startKafkaMicroservice(app);

    testController.reset();

    // Wait a moment for the microservice to be ready
    await new Promise(resolve => setTimeout(resolve, 1000));
  });

  afterEach(async () => {
    if (app) {
      await app.close();
    }
    if (module) {
      await module.close();
    }
    await testHelper.deleteTopics(testTopics);
  });

  describe('Complete Package Functionality', () => {
    it('should demonstrate full package capabilities', async () => {
      // Test 1: Simple event handling (no retry, no DLQ)
      await producer.send('e2e.simple.event', {
        key: 'simple-1',
        value: {
          id: 'simple-1',
          message: 'This is a simple event',
          timestamp: new Date().toISOString(),
        },
      });

      // Test 2: Successful retry scenario
      await producer.send('e2e.retry.success', {
        key: 'retry-1',
        value: {
          id: 'retry-1',
          shouldFailInitially: true,
          message: 'This will fail twice then succeed',
        },
      });

      // Test 3: DLQ scenario (always fails)
      await producer.send('e2e.dlq.event', {
        key: 'dlq-1',
        value: {
          id: 'dlq-1',
          alwaysFail: true,
          message: 'This will go to DLQ',
        },
      });

      // Test 4: Successful DLQ handler (doesn't fail)
      await producer.send('e2e.dlq.event', {
        key: 'dlq-success-1',
        value: {
          id: 'dlq-success-1',
          alwaysFail: false,
          message: 'This will succeed',
        },
      });

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 5000));

      // Verify simple event was processed
      const simpleEvents = testController.processedMessages.filter(
        m => m.topic === 'e2e.simple.event'
      );
      expect(simpleEvents).toHaveLength(1);
      expect(simpleEvents[0].payload.id).toBe('simple-1');

      // Verify retry event eventually succeeded
      const retryEvents = testController.processedMessages.filter(
        m => m.topic === 'e2e.retry.success'
      );
      expect(retryEvents).toHaveLength(1);
      expect(retryEvents[0].payload.id).toBe('retry-1');
      expect(retryEvents[0].attempt).toBe(3); // Should succeed on 3rd attempt

      // Verify successful DLQ event was processed
      const successfulDlqEvents = testController.processedMessages.filter(
        m => m.topic === 'e2e.dlq.event' && m.payload.id === 'dlq-success-1'
      );
      expect(successfulDlqEvents).toHaveLength(1);

      // Verify DLQ message was sent for failed event
      const dlqMessages = await testHelper.waitForMessages('e2e.dlq.e2e.dlq.event', {
        expectedCount: 1,
        timeout: 10000,
      });

      expect(dlqMessages).toHaveLength(1);
      expect(dlqMessages[0].originalMessage.value.id).toBe('dlq-1');
      expect(dlqMessages[0].error.message).toContain('DLQ test failure');
      expect(dlqMessages[0].dlqMetadata.plugin).toBe('dlq');
    });

    it('should handle batch message processing', async () => {
      const batchMessages = Array.from({ length: 10 }, (_, i) => ({
        key: `batch-${i}`,
        value: {
          id: `batch-${i}`,
          batchIndex: i,
          message: `Batch message ${i}`,
        },
      }));

      await producer.sendBatch('e2e.simple.event', batchMessages);

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 3000));

      const batchEvents = testController.processedMessages.filter(
        m => m.topic === 'e2e.simple.event' && m.payload.id?.startsWith('batch-')
      );

      expect(batchEvents).toHaveLength(10);

      // Verify all messages were processed
      const processedIds = batchEvents.map(e => e.payload.id).sort();
      const expectedIds = Array.from({ length: 10 }, (_, i) => `batch-${i}`);
      expect(processedIds).toEqual(expectedIds);
    });

    it('should handle concurrent message processing', async () => {
      const concurrentMessages = Array.from({ length: 20 }, (_, i) => ({
        topic: 'e2e.simple.event',
        key: `concurrent-${i}`,
        value: {
          id: `concurrent-${i}`,
          message: `Concurrent message ${i}`,
        },
      }));

      const startTime = Date.now();

      // Send all messages concurrently
      await Promise.all(
        concurrentMessages.map(msg =>
          producer.send(msg.topic, {
            key: msg.key,
            value: msg.value,
          })
        )
      );

      // Wait for processing
      let attempts = 0;
      while (
        testController.processedMessages.filter(
          m => m.payload.id?.startsWith('concurrent-')
        ).length < 20 &&
        attempts < 30
      ) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        attempts++;
      }

      const processingTime = Date.now() - startTime;

      const concurrentEvents = testController.processedMessages.filter(
        m => m.payload.id?.startsWith('concurrent-')
      );

      expect(concurrentEvents).toHaveLength(20);
      expect(processingTime).toBeLessThan(15000); // Should complete within 15 seconds
    });

    it('should handle transactional messaging', async () => {
      const transactionRecords = [
        {
          topic: 'e2e.simple.event',
          messages: [
            {
              key: 'txn-1',
              value: { id: 'txn-1', type: 'order', action: 'created' },
            },
            {
              key: 'txn-2',
              value: { id: 'txn-2', type: 'inventory', action: 'reserved' },
            },
          ],
        },
        {
          topic: 'e2e.dlq.event',
          messages: [
            {
              key: 'txn-3',
              value: { id: 'txn-3', type: 'payment', alwaysFail: false },
            },
          ],
        },
      ];

      await producer.sendTransaction(transactionRecords);

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 3000));

      // Verify all transactional messages were processed
      const txnMessages = testController.processedMessages.filter(
        m => m.payload.id?.startsWith('txn-')
      );

      expect(txnMessages).toHaveLength(3);

      const messageTypes = txnMessages.map(m => m.payload.type).sort();
      expect(messageTypes).toEqual(['inventory', 'order', 'payment']);
    });
  });

  describe('Error Handling and Resilience', () => {
    it('should recover from producer connection issues', async () => {
      // Force disconnect the producer
      await producer.onModuleDestroy();

      // Try to send a message (should auto-reconnect)
      await producer.send('e2e.simple.event', {
        key: 'reconnect-test',
        value: {
          id: 'reconnect-test',
          message: 'Testing reconnection',
        },
      });

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      const reconnectEvents = testController.processedMessages.filter(
        m => m.payload.id === 'reconnect-test'
      );

      expect(reconnectEvents).toHaveLength(1);
    });

    it('should handle malformed messages gracefully', async () => {
      // Send a message with unexpected structure
      await producer.send('e2e.simple.event', {
        key: 'malformed',
        value: null, // This might cause issues
      });

      // Send a valid message after
      await producer.send('e2e.simple.event', {
        key: 'valid-after-malformed',
        value: {
          id: 'valid-after-malformed',
          message: 'This should still work',
        },
      });

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      // The valid message should still be processed
      const validEvents = testController.processedMessages.filter(
        m => m.payload?.id === 'valid-after-malformed'
      );

      expect(validEvents).toHaveLength(1);
    });
  });

  describe('Performance and Scalability', () => {
    it('should handle high message throughput', async () => {
      const messageCount = 100;
      const messages = Array.from({ length: messageCount }, (_, i) => ({
        key: `perf-${i}`,
        value: {
          id: `perf-${i}`,
          index: i,
          payload: 'x'.repeat(500), // 500 byte payload
        },
      }));

      const startTime = Date.now();

      // Send messages in batches of 10
      for (let i = 0; i < messages.length; i += 10) {
        const batch = messages.slice(i, i + 10);
        await producer.sendBatch('e2e.simple.event', batch);
      }

      const sendTime = Date.now() - startTime;

      // Wait for all messages to be processed
      let processedCount = 0;
      let attempts = 0;
      while (processedCount < messageCount && attempts < 60) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        processedCount = testController.processedMessages.filter(
          m => m.payload.id?.startsWith('perf-')
        ).length;
        attempts++;
      }

      const totalTime = Date.now() - startTime;

      expect(processedCount).toBe(messageCount);
      expect(sendTime).toBeLessThan(10000); // Send should complete within 10 seconds
      expect(totalTime).toBeLessThan(30000); // Total processing within 30 seconds

      console.log(`Performance test: ${messageCount} messages processed in ${totalTime}ms`);
    });
  });

  describe('Configuration Flexibility', () => {
    it('should work with minimal configuration', async () => {
      // This test uses the existing module which already has minimal config
      expect(producer).toBeInstanceOf(KafkaProducerService);
      expect(testController).toBeInstanceOf(E2ETestController);

      const isReady = await producer.isReady();
      expect(isReady).toBe(true);
    });

    it('should handle plugin customization', async () => {
      // The DLQ plugin is already configured with custom options
      expect(dlqPlugin.name).toBe('dlq');

      // Test that the plugin is working by triggering a DLQ scenario
      await producer.send('e2e.dlq.event', {
        key: 'plugin-test',
        value: {
          id: 'plugin-test',
          alwaysFail: true,
          message: 'Testing plugin customization',
        },
      });

      await new Promise(resolve => setTimeout(resolve, 3000));

      // Verify DLQ message has custom prefix
      const dlqMessages = await testHelper.waitForMessages('e2e.dlq.e2e.dlq.event', {
        expectedCount: 1,
        timeout: 5000,
      });

      expect(dlqMessages[0].dlqMetadata.plugin).toBe('dlq');
    });
  });
});