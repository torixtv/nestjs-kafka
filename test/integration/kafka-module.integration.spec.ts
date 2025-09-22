import { Test, TestingModule } from '@nestjs/testing';
import { Injectable, Controller } from '@nestjs/common';
import { KafkaModule } from '../../src/core/kafka.module';
import { KafkaProducerService } from '../../src/core/kafka.producer';
import { KafkaController } from '../../src/core/kafka.controller';
import { EventHandler } from '../../src/decorators/event-handler.decorator';
import { DLQPlugin } from '../../src/plugins/dlq/dlq.plugin';
import { KafkaTestHelper } from '../helpers/kafka-test.helper';
import { registerKafkaMicroservice, startKafkaMicroservice } from '../../src/core/kafka.bootstrap';
import { INestApplication } from '@nestjs/common';

// Test controllers for integration testing
@Injectable()
@Controller()
class TestEventController extends KafkaController {
  public receivedMessages: any[] = [];
  public errors: Error[] = [];

  @EventHandler('test.order.created')
  async handleOrderCreated(payload: any) {
    console.log('Received order created event:', payload);
    this.receivedMessages.push({ event: 'order.created', payload });
  }

  @EventHandler('test.user.registered', {
    retry: {
      enabled: true,
      attempts: 3,
      backoff: 'exponential',
    },
  })
  async handleUserRegistered(payload: any) {
    console.log('Received user registered event:', payload);

    // Simulate failure for testing retry logic
    if (payload.shouldFail && this.getAttemptCount(payload.id) < 2) {
      this.errors.push(new Error(`Simulated failure for user ${payload.id}`));
      throw new Error(`Processing failed for user ${payload.id}`);
    }

    this.receivedMessages.push({ event: 'user.registered', payload });
  }

  @EventHandler('test.payment.failed', {
    dlq: {
      enabled: true,
      topic: 'test.dlq.payments',
    },
  })
  async handlePaymentFailed(payload: any) {
    console.log('Received payment failed event:', payload);

    // Always fail to test DLQ
    if (payload.alwaysFail) {
      throw new Error('Payment processing always fails');
    }

    this.receivedMessages.push({ event: 'payment.failed', payload });
  }

  private getAttemptCount(id: string): number {
    return this.errors.filter(err => err.message.includes(id)).length;
  }

  reset() {
    this.receivedMessages = [];
    this.errors = [];
  }
}

describe('KafkaModule (Integration)', () => {
  let app: INestApplication;
  let module: TestingModule;
  let producer: KafkaProducerService;
  let testController: TestEventController;
  let testHelper: KafkaTestHelper;
  let testTopics: string[];

  beforeAll(async () => {
    testHelper = new KafkaTestHelper(KafkaTestHelper.getDefaultConfig());
    await testHelper.waitForKafka();
    await testHelper.connect();
  });

  afterAll(async () => {
    await testHelper.disconnect();
  });

  beforeEach(async () => {
    testTopics = [
      'test.order.created',
      'test.user.registered',
      'test.payment.failed',
      'test.dlq.payments'
    ];

    const dlqPlugin = new DLQPlugin(
      // We'll inject the producer after module creation
      {} as any,
      {
        topicPrefix: 'test.dlq',
        includeErrorDetails: true,
        includeOriginalMessage: true,
      }
    );

    module = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({
          client: {
            clientId: 'integration-test-full',
            brokers: ['localhost:9092'],
          },
          consumer: {
            groupId: 'integration-test-full-group',
          },
          subscriptions: {
            topics: testTopics,
            fromBeginning: true,
          },
          retry: {
            enabled: true,
            attempts: 3,
            backoff: 'exponential',
            baseDelay: 100, // Fast retry for testing
            maxDelay: 1000,
          },
          dlq: {
            enabled: true,
          },
          plugins: [dlqPlugin],
        }),
      ],
      controllers: [TestEventController],
    }).compile();

    // Initialize the NestJS application
    app = module.createNestApplication();

    // Get services
    producer = module.get<KafkaProducerService>(KafkaProducerService);
    testController = module.get<TestEventController>(TestEventController);

    // Inject producer into DLQ plugin
    (dlqPlugin as any).producer = producer;

    // Create topics
    await testHelper.createTopics(testTopics);

    // Register and start Kafka microservice
    await registerKafkaMicroservice(app, {
      overrides: {
        subscribe: {
          topics: testTopics,
          fromBeginning: true,
        },
      },
    });

    await app.init();
    await startKafkaMicroservice(app);

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
    await testHelper.deleteTopics(testTopics);
  });

  describe('end-to-end message flow', () => {
    it('should send and receive messages successfully', async () => {
      const orderData = {
        orderId: 'order-123',
        customerId: 'customer-456',
        amount: 199.99,
        timestamp: new Date().toISOString(),
      };

      // Send message
      await producer.send('test.order.created', {
        key: orderData.orderId,
        value: orderData,
      });

      // Wait for message processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Verify message was received
      expect(testController.receivedMessages).toHaveLength(1);
      expect(testController.receivedMessages[0]).toEqual({
        event: 'order.created',
        payload: orderData,
      });
    });

    it('should handle multiple message types', async () => {
      const messages = [
        {
          topic: 'test.order.created',
          data: { orderId: 'order-001', amount: 99.99 },
        },
        {
          topic: 'test.user.registered',
          data: { userId: 'user-001', email: 'test@example.com' },
        },
      ];

      // Send messages
      for (const msg of messages) {
        await producer.send(msg.topic, {
          key: msg.data.orderId || msg.data.userId,
          value: msg.data,
        });
      }

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 3000));

      // Verify both messages were received
      expect(testController.receivedMessages).toHaveLength(2);

      const events = testController.receivedMessages.map(m => m.event);
      expect(events).toContain('order.created');
      expect(events).toContain('user.registered');
    });

    it('should process messages in parallel', async () => {
      const messageCount = 10;
      const messages = Array.from({ length: messageCount }, (_, i) => ({
        orderId: `parallel-order-${i}`,
        amount: 100 + i,
        timestamp: new Date().toISOString(),
      }));

      const startTime = Date.now();

      // Send messages in parallel
      await Promise.all(
        messages.map(msg =>
          producer.send('test.order.created', {
            key: msg.orderId,
            value: msg,
          })
        )
      );

      // Wait for all messages to be processed
      let attempts = 0;
      while (testController.receivedMessages.length < messageCount && attempts < 30) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        attempts++;
      }

      const processingTime = Date.now() - startTime;

      expect(testController.receivedMessages).toHaveLength(messageCount);
      expect(processingTime).toBeLessThan(15000); // Should complete within 15 seconds
    });
  });

  describe('retry mechanism', () => {
    it('should retry failed messages according to configuration', async () => {
      const userData = {
        id: 'retry-user-001',
        email: 'retry@example.com',
        shouldFail: true, // This will cause initial failures
      };

      await producer.send('test.user.registered', {
        key: userData.id,
        value: userData,
      });

      // Wait for retries to complete
      await new Promise(resolve => setTimeout(resolve, 5000));

      // Should eventually succeed after retries
      expect(testController.receivedMessages.length).toBeGreaterThan(0);
      expect(testController.errors.length).toBeGreaterThan(0); // Should have some failures

      // Verify the message was eventually processed
      const processedMessage = testController.receivedMessages.find(
        m => m.event === 'user.registered' && m.payload.id === userData.id
      );
      expect(processedMessage).toBeDefined();
    });
  });

  describe('dead letter queue', () => {
    it('should send messages to DLQ after max retries exceeded', async () => {
      const paymentData = {
        paymentId: 'payment-failed-001',
        amount: 500.00,
        alwaysFail: true, // This will always fail
      };

      await producer.send('test.payment.failed', {
        key: paymentData.paymentId,
        value: paymentData,
      });

      // Wait for processing and DLQ
      await new Promise(resolve => setTimeout(resolve, 3000));

      // Verify DLQ message was sent
      const dlqMessages = await testHelper.waitForMessages('test.dlq.payments', {
        expectedCount: 1,
        timeout: 5000,
      });

      expect(dlqMessages).toHaveLength(1);

      const dlqMessage = dlqMessages[0];
      expect(dlqMessage.originalMessage).toBeDefined();
      expect(dlqMessage.error).toBeDefined();
      expect(dlqMessage.dlqMetadata).toBeDefined();
    });
  });

  describe('bootstrap functions', () => {
    it('should handle microservice startup gracefully', async () => {
      // This test verifies that the bootstrap functions work correctly
      // The setup in beforeEach already tests this, but let's verify explicitly

      expect(app).toBeDefined();

      // Verify services are available
      expect(producer).toBeInstanceOf(KafkaProducerService);
      expect(testController).toBeInstanceOf(TestEventController);

      // Verify producer is ready
      const isReady = await producer.isReady();
      expect(isReady).toBe(true);
    });

    it('should handle connection failures gracefully when requireBroker is false', async () => {
      // Create a module with invalid broker and requireBroker: false
      const faultyModule = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            client: {
              clientId: 'faulty-test',
              brokers: ['localhost:19092'], // Non-existent broker
            },
            consumer: {
              groupId: 'faulty-test-group',
            },
            requireBroker: false,
          }),
        ],
        controllers: [TestEventController],
      }).compile();

      const faultyApp = faultyModule.createNestApplication();

      // This should not throw even with invalid broker
      await registerKafkaMicroservice(faultyApp);
      await faultyApp.init();

      // Should handle startup gracefully
      await expect(startKafkaMicroservice(faultyApp)).resolves.not.toThrow();

      await faultyApp.close();
      await faultyModule.close();
    });
  });

  describe('configuration', () => {
    it('should work with async configuration', async () => {
      const asyncModule = await Test.createTestingModule({
        imports: [
          KafkaModule.forRootAsync({
            useFactory: () => ({
              client: {
                clientId: 'async-test',
                brokers: ['localhost:9092'],
              },
              consumer: {
                groupId: 'async-test-group',
              },
            }),
          }),
        ],
      }).compile();

      const asyncProducer = asyncModule.get<KafkaProducerService>(KafkaProducerService);
      expect(asyncProducer).toBeInstanceOf(KafkaProducerService);

      await asyncModule.close();
    });
  });
});