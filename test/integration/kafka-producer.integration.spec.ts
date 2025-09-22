import { Test, TestingModule } from '@nestjs/testing';
import { KafkaModule } from '../../src/core/kafka.module';
import { KafkaProducerService } from '../../src/core/kafka.producer';
import { KafkaTestHelper } from '../helpers/kafka-test.helper';

describe('KafkaProducerService (Integration)', () => {
  let module: TestingModule;
  let producer: KafkaProducerService;
  let testHelper: KafkaTestHelper;
  let testTopic: string;

  beforeAll(async () => {
    testHelper = new KafkaTestHelper(KafkaTestHelper.getDefaultConfig());

    // Wait for Kafka to be available
    await testHelper.waitForKafka();
    await testHelper.connect();
  });

  afterAll(async () => {
    await testHelper.disconnect();
  });

  beforeEach(async () => {
    testTopic = testHelper.generateTestTopic('producer-integration');

    module = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({
          client: {
            clientId: 'integration-test-producer',
            brokers: ['localhost:9092'],
          },
          consumer: {
            groupId: 'integration-test-group',
          },
        }),
      ],
    }).compile();

    producer = module.get<KafkaProducerService>(KafkaProducerService);
    await testHelper.createTopics([testTopic]);
  });

  afterEach(async () => {
    if (module) {
      await module.close();
    }
    await testHelper.deleteTopics([testTopic]);
  });

  describe('send', () => {
    it('should send a message to Kafka and be consumed successfully', async () => {
      const testMessage = {
        orderId: '12345',
        customerId: 'customer-1',
        amount: 99.99,
        timestamp: new Date().toISOString(),
      };

      // Send message
      await producer.send(testTopic, {
        key: testMessage.orderId,
        value: testMessage,
        headers: {
          'content-type': 'application/json',
          'source': 'integration-test',
        },
      });

      // Wait for and verify message
      const receivedMessages = await testHelper.waitForMessages(testTopic, {
        expectedCount: 1,
        timeout: 10000,
      });

      expect(receivedMessages).toHaveLength(1);
      expect(receivedMessages[0]).toEqual(testMessage);
    });

    it('should send multiple messages in sequence', async () => {
      const messages = [
        { id: '1', data: 'first message' },
        { id: '2', data: 'second message' },
        { id: '3', data: 'third message' },
      ];

      // Send messages sequentially
      for (const msg of messages) {
        await producer.send(testTopic, {
          key: msg.id,
          value: msg,
        });
      }

      // Wait for all messages
      const receivedMessages = await testHelper.waitForMessages(testTopic, {
        expectedCount: 3,
        timeout: 15000,
      });

      expect(receivedMessages).toHaveLength(3);
      expect(receivedMessages).toEqual(expect.arrayContaining(messages));
    });

    it('should handle string values without JSON serialization', async () => {
      const stringMessage = 'This is a plain string message';

      await producer.send(testTopic, {
        key: 'string-test',
        value: stringMessage,
      });

      // For string messages, we need to handle them differently in the test helper
      const consumer = await testHelper.createConsumer();

      const receivedMessage = await new Promise<string>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Timeout waiting for string message'));
        }, 10000);

        consumer.subscribe({ topic: testTopic, fromBeginning: true });
        consumer.run({
          eachMessage: async ({ message }) => {
            clearTimeout(timeout);
            resolve(message.value?.toString() || '');
          },
        });
      });

      expect(receivedMessage).toBe(stringMessage);
    });
  });

  describe('sendBatch', () => {
    it('should send multiple messages in a single batch', async () => {
      const messages = [
        { key: 'batch-1', value: { order: 'order-1', status: 'pending' } },
        { key: 'batch-2', value: { order: 'order-2', status: 'processing' } },
        { key: 'batch-3', value: { order: 'order-3', status: 'completed' } },
      ];

      await producer.sendBatch(testTopic, messages);

      const receivedMessages = await testHelper.waitForMessages(testTopic, {
        expectedCount: 3,
        timeout: 15000,
      });

      expect(receivedMessages).toHaveLength(3);

      // Verify all messages were received (order might not be preserved)
      const receivedValues = receivedMessages.map(msg => msg);
      expect(receivedValues).toEqual(expect.arrayContaining([
        { order: 'order-1', status: 'pending' },
        { order: 'order-2', status: 'processing' },
        { order: 'order-3', status: 'completed' },
      ]));
    });

    it('should handle large batch efficiently', async () => {
      const batchSize = 100;
      const messages = Array.from({ length: batchSize }, (_, i) => ({
        key: `large-batch-${i}`,
        value: {
          id: i,
          data: `Message number ${i}`,
          timestamp: new Date().toISOString(),
        },
      }));

      const startTime = Date.now();
      await producer.sendBatch(testTopic, messages);
      const sendDuration = Date.now() - startTime;

      // Should complete within reasonable time (less than 5 seconds for 100 messages)
      expect(sendDuration).toBeLessThan(5000);

      const receivedMessages = await testHelper.waitForMessages(testTopic, {
        expectedCount: batchSize,
        timeout: 30000,
      });

      expect(receivedMessages).toHaveLength(batchSize);
    });
  });

  describe('sendTransaction', () => {
    it('should send transactional messages to multiple topics', async () => {
      const secondTopic = testHelper.generateTestTopic('producer-integration-txn');
      await testHelper.createTopics([secondTopic]);

      const transactionRecords = [
        {
          topic: testTopic,
          messages: [
            { key: 'txn-1', value: { event: 'order-created', orderId: 'txn-order-1' } },
            { key: 'txn-2', value: { event: 'order-created', orderId: 'txn-order-2' } },
          ],
        },
        {
          topic: secondTopic,
          messages: [
            { key: 'txn-1', value: { event: 'inventory-updated', orderId: 'txn-order-1' } },
            { key: 'txn-2', value: { event: 'inventory-updated', orderId: 'txn-order-2' } },
          ],
        },
      ];

      await producer.sendTransaction(transactionRecords);

      // Verify messages in both topics
      const [firstTopicMessages, secondTopicMessages] = await Promise.all([
        testHelper.waitForMessages(testTopic, { expectedCount: 2, timeout: 10000 }),
        testHelper.waitForMessages(secondTopic, { expectedCount: 2, timeout: 10000 }),
      ]);

      expect(firstTopicMessages).toHaveLength(2);
      expect(secondTopicMessages).toHaveLength(2);

      // Verify message content
      expect(firstTopicMessages).toEqual(expect.arrayContaining([
        { event: 'order-created', orderId: 'txn-order-1' },
        { event: 'order-created', orderId: 'txn-order-2' },
      ]));

      expect(secondTopicMessages).toEqual(expect.arrayContaining([
        { event: 'inventory-updated', orderId: 'txn-order-1' },
        { event: 'inventory-updated', orderId: 'txn-order-2' },
      ]));

      // Cleanup
      await testHelper.deleteTopics([secondTopic]);
    });
  });

  describe('error handling', () => {
    it('should handle invalid topic names gracefully', async () => {
      const invalidTopic = 'invalid-topic-with-very-long-name-that-exceeds-kafka-limits-and-contains-invalid-characters-!@#$%^&*()';

      await expect(
        producer.send(invalidTopic, { value: 'test' })
      ).rejects.toThrow();
    });

    it('should handle connection loss gracefully', async () => {
      // This test simulates network issues by attempting to send to a non-existent broker
      const isolatedModule = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            client: {
              clientId: 'isolated-test-producer',
              brokers: ['localhost:19092'], // Non-existent broker
            },
          }),
        ],
      }).compile();

      const isolatedProducer = isolatedModule.get<KafkaProducerService>(KafkaProducerService);

      await expect(
        isolatedProducer.send(testTopic, { value: 'test' })
      ).rejects.toThrow();

      await isolatedModule.close();
    });
  });

  describe('performance', () => {
    it('should maintain consistent performance under load', async () => {
      const messageCount = 50;
      const messages = Array.from({ length: messageCount }, (_, i) => ({
        key: `perf-test-${i}`,
        value: {
          id: i,
          payload: 'x'.repeat(1000), // 1KB message
          timestamp: new Date().toISOString(),
        },
      }));

      const startTime = Date.now();

      // Send messages in parallel
      await Promise.all(
        messages.map(msg => producer.send(testTopic, msg))
      );

      const sendDuration = Date.now() - startTime;

      // Should complete within reasonable time
      expect(sendDuration).toBeLessThan(10000); // 10 seconds max for 50 messages

      // Verify all messages were sent
      const receivedMessages = await testHelper.waitForMessages(testTopic, {
        expectedCount: messageCount,
        timeout: 20000,
      });

      expect(receivedMessages).toHaveLength(messageCount);
    });
  });
});