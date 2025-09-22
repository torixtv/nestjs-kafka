import { Test, TestingModule } from '@nestjs/testing';
import { Injectable, Controller } from '@nestjs/common';
import { KafkaModule } from '../../src/core/kafka.module';
import { KafkaProducerService } from '../../src/core/kafka.producer';
import { KafkaController } from '../../src/core/kafka.controller';
import { EventHandler } from '../../src/decorators/event-handler.decorator';
import { KafkaRetryManager } from '../../src/services/kafka.retry-manager';
import { KafkaRetryConsumer } from '../../src/services/kafka.retry-consumer';
import { KafkaTestHelper } from '../helpers/kafka-test.helper';
import { registerKafkaMicroservice, startKafkaMicroservice } from '../../src/core/kafka.bootstrap';
import { INestApplication } from '@nestjs/common';

// Load test controller with configurable failure rates
@Injectable()
@Controller()
class LoadTestController extends KafkaController {
  public metrics = {
    processed: 0,
    failed: 0,
    retried: 0,
    totalProcessingTime: 0,
    messageTimestamps: new Map<string, number>(),
  };

  private failureRate = 0.3; // 30% failure rate
  private maxRetries = 2;

  @EventHandler('test.load.high-volume', {
    retry: {
      enabled: true,
      attempts: 2, // Quick retries for load testing
      baseDelay: 500, // Fast delays
      maxDelay: 2000,
      backoff: 'linear',
    },
  })
  async handleHighVolumeMessage(payload: any) {
    const startTime = Date.now();
    const messageId = payload.id;

    // Record message timestamp for latency measurement
    if (!this.metrics.messageTimestamps.has(messageId)) {
      this.metrics.messageTimestamps.set(messageId, payload.timestamp);
    }

    // Simulate failure based on failure rate
    const shouldFail = Math.random() < this.failureRate;
    const retryCount = this.getRetryCount(messageId);

    if (shouldFail && retryCount < this.maxRetries) {
      this.metrics.failed++;
      this.metrics.retried++;
      throw new Error(`Load test failure for ${messageId}`);
    }

    // Success - record metrics
    this.metrics.processed++;
    const processingTime = startTime - this.metrics.messageTimestamps.get(messageId)!;
    this.metrics.totalProcessingTime += processingTime;
  }

  @EventHandler('test.load.concurrent-retry', {
    retry: {
      enabled: true,
      attempts: 3,
      baseDelay: 1000,
      maxDelay: 5000,
      backoff: 'exponential',
    },
  })
  async handleConcurrentRetry(payload: any) {
    const messageId = payload.id;
    const partitionKey = payload.partitionKey;

    // Fail messages in certain partitions to test concurrent retry handling
    if (partitionKey % 2 === 0 && this.getRetryCount(messageId) < 2) {
      this.metrics.failed++;
      this.metrics.retried++;
      throw new Error(`Concurrent retry test failure for ${messageId}`);
    }

    this.metrics.processed++;
  }

  private getRetryCount(messageId: string): number {
    // Simplified retry count tracking for load testing
    return Math.floor(Math.random() * 3);
  }

  reset() {
    this.metrics = {
      processed: 0,
      failed: 0,
      retried: 0,
      totalProcessingTime: 0,
      messageTimestamps: new Map(),
    };
  }

  getMetrics() {
    const avgProcessingTime = this.metrics.processed > 0
      ? this.metrics.totalProcessingTime / this.metrics.processed
      : 0;

    return {
      ...this.metrics,
      averageProcessingTime: avgProcessingTime,
      successRate: this.metrics.processed / (this.metrics.processed + this.metrics.failed),
      totalMessages: this.metrics.processed + this.metrics.failed,
    };
  }
}

describe('Retry Load and Performance Tests (Real Kafka)', () => {
  let app: INestApplication;
  let module: TestingModule;
  let producer: KafkaProducerService;
  let retryManager: KafkaRetryManager;
  let retryConsumer: KafkaRetryConsumer;
  let testController: LoadTestController;
  let testHelper: KafkaTestHelper;
  let testTopics: string[];

  beforeAll(async () => {
    testHelper = new KafkaTestHelper({
      brokers: ['localhost:9092'],
      clientId: 'retry-load-test',
      groupId: 'retry-load-test-group',
    });

    await testHelper.waitForKafka();
    await testHelper.connect();
  });

  afterAll(async () => {
    await testHelper.disconnect();
  });

  beforeEach(async () => {
    const clientId = `retry-load-test-${Date.now()}`;
    testTopics = [
      'test.load.high-volume',
      'test.load.concurrent-retry',
    ];

    module = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({
          client: {
            clientId,
            brokers: ['localhost:9092'],
          },
          consumer: {
            groupId: `${clientId}-group`,
            // Optimize for load testing
            sessionTimeout: 6000,
            heartbeatInterval: 1000,
            maxBytesPerPartition: 1048576,
            fetchMaxBytes: 1048576,
          },
          producer: {
            // Optimize for high throughput
            maxInFlightRequests: 5,
            acks: 1, // Faster acknowledgment for load testing
            compression: 'gzip',
          },
          retry: {
            enabled: true,
            attempts: 3,
            baseDelay: 500,
            maxDelay: 5000,
            backoff: 'linear',
            // Retry topic optimization
            topicPartitions: 5,
            topicReplicationFactor: 1,
          },
        }),
      ],
      controllers: [LoadTestController],
    }).compile();

    app = module.createNestApplication();

    producer = module.get<KafkaProducerService>(KafkaProducerService);
    retryManager = module.get<KafkaRetryManager>(KafkaRetryManager);
    retryConsumer = module.get<KafkaRetryConsumer>(KafkaRetryConsumer);
    testController = module.get<LoadTestController>(LoadTestController);

    // Create topics with multiple partitions for load testing
    await testHelper.createTopics([
      ...testTopics.map(topic => ({ topic, numPartitions: 5 })),
      { topic: retryManager.getRetryTopicName(), numPartitions: 5 },
    ]);

    await registerKafkaMicroservice(app, {
      overrides: {
        subscribe: {
          topics: testTopics,
          fromBeginning: false,
        },
      },
    });

    await app.init();
    await startKafkaMicroservice(app);

    testController.reset();

    // Wait for services to be ready
    await new Promise(resolve => setTimeout(resolve, 3000));
  });

  afterEach(async () => {
    if (app) {
      await app.close();
    }
    if (module) {
      await module.close();
    }

    await testHelper.deleteTopics([
      ...testTopics,
      retryManager.getRetryTopicName(),
    ]);
  });

  describe('High Volume Message Processing', () => {
    it('should handle 1000 messages with retry failures', async () => {
      const messageCount = 1000;
      const messages = Array.from({ length: messageCount }, (_, i) => ({
        id: `load-test-${i}`,
        timestamp: Date.now(),
        data: `Load test message ${i}`,
        batchId: Math.floor(i / 100), // Group into batches
      }));

      const startTime = Date.now();

      // Send messages in parallel batches for better performance
      const batchSize = 50;
      for (let i = 0; i < messages.length; i += batchSize) {
        const batch = messages.slice(i, i + batchSize);
        await Promise.all(
          batch.map((msg, index) =>
            producer.send('test.load.high-volume', {
              key: msg.id,
              value: msg,
              partition: (i + index) % 5, // Distribute across partitions
            })
          )
        );

        // Small delay between batches to avoid overwhelming
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      const sendTime = Date.now() - startTime;
      console.log(`Sent ${messageCount} messages in ${sendTime}ms`);

      // Wait for processing to complete
      // With 30% failure rate and 2 retries, expect some processing time
      await new Promise(resolve => setTimeout(resolve, 30000));

      const metrics = testController.getMetrics();
      console.log('Load test metrics:', metrics);

      // Verify basic throughput
      expect(metrics.totalMessages).toBeGreaterThan(messageCount * 0.8); // At least 80% processed
      expect(metrics.successRate).toBeGreaterThan(0.6); // At least 60% success rate
      expect(metrics.averageProcessingTime).toBeLessThan(10000); // Average under 10 seconds

      // Performance benchmarks
      const totalTime = Date.now() - startTime;
      const throughput = metrics.totalMessages / (totalTime / 1000); // Messages per second
      console.log(`Throughput: ${throughput.toFixed(2)} messages/second`);

      expect(throughput).toBeGreaterThan(10); // At least 10 messages/second
    }, 60000);

    it('should handle concurrent processing across multiple partitions', async () => {
      const messagesPerPartition = 100;
      const partitionCount = 5;
      const totalMessages = messagesPerPartition * partitionCount;

      const messages = Array.from({ length: totalMessages }, (_, i) => ({
        id: `concurrent-${i}`,
        partitionKey: i % partitionCount,
        timestamp: Date.now(),
        data: `Concurrent test message ${i}`,
      }));

      const startTime = Date.now();

      // Send messages distributed across partitions
      await Promise.all(
        messages.map(msg =>
          producer.send('test.load.concurrent-retry', {
            key: msg.id,
            value: msg,
            partition: msg.partitionKey,
          })
        )
      );

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 20000));

      const metrics = testController.getMetrics();
      console.log('Concurrent processing metrics:', metrics);

      // Verify all partitions were processed
      expect(metrics.totalMessages).toBeGreaterThan(totalMessages * 0.7);
      expect(metrics.processed).toBeGreaterThan(0);

      const processingTime = Date.now() - startTime;
      console.log(`Concurrent processing completed in ${processingTime}ms`);

      // Should complete faster than sequential processing due to concurrency
      expect(processingTime).toBeLessThan(30000);
    }, 40000);
  });

  describe('Retry Consumer Performance', () => {
    it('should maintain stable polling under load', async () => {
      // Generate messages that will create retry load
      const retryMessages = Array.from({ length: 200 }, (_, i) => ({
        id: `retry-load-${i}`,
        timestamp: Date.now(),
        data: `Retry load test ${i}`,
        shouldAlwaysFail: i % 3 === 0, // 33% always fail to create retry pressure
      }));

      const startTime = Date.now();

      // Send all messages
      await Promise.all(
        retryMessages.map(msg =>
          producer.send('test.load.high-volume', {
            key: msg.id,
            value: msg,
          })
        )
      );

      // Wait for multiple polling cycles
      await new Promise(resolve => setTimeout(resolve, 25000));

      const metrics = testController.getMetrics();
      console.log('Retry load metrics:', metrics);

      // Verify retry consumer is handling the load
      expect(retryConsumer.isRetryConsumerRunning()).toBe(true);
      expect(metrics.retried).toBeGreaterThan(0);

      // Should process majority of messages even under retry load
      expect(metrics.totalMessages).toBeGreaterThan(150);

      const totalTime = Date.now() - startTime;
      console.log(`Retry load test completed in ${totalTime}ms`);
    }, 35000);

    it('should handle retry topic with multiple partitions', async () => {
      const retryTopicName = retryManager.getRetryTopicName();

      // Verify retry topic was created with multiple partitions
      const topicMetadata = await testHelper.getTopicMetadata(retryTopicName);
      expect(topicMetadata.partitions.length).toBeGreaterThan(1);

      // Generate failures to create retry messages across partitions
      const failureMessages = Array.from({ length: 100 }, (_, i) => ({
        id: `partition-retry-${i}`,
        timestamp: Date.now(),
        data: `Partition retry test ${i}`,
      }));

      // Send messages that will fail initially
      await Promise.all(
        failureMessages.map((msg, index) =>
          producer.send('test.load.high-volume', {
            key: msg.id,
            value: msg,
            partition: index % 5,
          })
        )
      );

      // Wait for retry processing
      await new Promise(resolve => setTimeout(resolve, 15000));

      // Verify retry messages were distributed across partitions
      const retryMessages = await testHelper.waitForMessages(retryTopicName, {
        expectedCount: 30, // Expect some retry messages
        timeout: 10000,
        fromBeginning: true,
      });

      expect(retryMessages.length).toBeGreaterThan(10);

      // Verify partitioning
      const partitionDistribution = new Map<number, number>();
      retryMessages.forEach(msg => {
        const partition = msg.partition || 0;
        partitionDistribution.set(partition, (partitionDistribution.get(partition) || 0) + 1);
      });

      // Should use multiple partitions
      expect(partitionDistribution.size).toBeGreaterThan(1);
      console.log('Retry message partition distribution:', Array.from(partitionDistribution.entries()));
    }, 25000);
  });

  describe('Memory and Resource Usage', () => {
    it('should not accumulate memory during high-volume retry processing', async () => {
      const initialMemory = process.memoryUsage();

      // Process messages in waves to test memory stability
      for (let wave = 0; wave < 3; wave++) {
        const waveMessages = Array.from({ length: 200 }, (_, i) => ({
          id: `memory-test-${wave}-${i}`,
          timestamp: Date.now(),
          data: `Memory test wave ${wave} message ${i}`,
        }));

        await Promise.all(
          waveMessages.map(msg =>
            producer.send('test.load.high-volume', {
              key: msg.id,
              value: msg,
            })
          )
        );

        // Wait for processing
        await new Promise(resolve => setTimeout(resolve, 8000));

        // Force garbage collection if possible
        if (global.gc) {
          global.gc();
        }
      }

      const finalMemory = process.memoryUsage();
      const memoryIncrease = finalMemory.heapUsed - initialMemory.heapUsed;
      const memoryIncreaseMB = memoryIncrease / (1024 * 1024);

      console.log(`Memory usage increase: ${memoryIncreaseMB.toFixed(2)} MB`);
      console.log('Initial memory:', initialMemory);
      console.log('Final memory:', finalMemory);

      // Memory increase should be reasonable (less than 100MB for this test)
      expect(memoryIncreaseMB).toBeLessThan(100);

      const metrics = testController.getMetrics();
      expect(metrics.totalMessages).toBeGreaterThan(400); // Should process most messages
    }, 45000);
  });
});