import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import * as request from 'supertest';
import { AppModule } from '../src/app.module';
import { KafkaDlqService } from '../../src/services/kafka.dlq.service';
import DlqTestHelper from './helpers/dlq-test.helper';

describe('Kafka Retry Mechanism (E2E)', () => {
  let app: INestApplication;
  let httpServer: any;
  let dlqService: KafkaDlqService;
  let dlqHelper: DlqTestHelper;

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.init();
    httpServer = app.getHttpServer();

    // Initialize DLQ service and helper
    dlqService = app.get<KafkaDlqService>(KafkaDlqService);
    dlqHelper = new DlqTestHelper(app, httpServer);
  }, 60000); // 60 second timeout for app startup

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    // Reset application state before each test
    await request(httpServer)
      .post('/reset')
      .expect(201);
  });

  describe('Application Health', () => {
    it('should return health status', async () => {
      const response = await request(httpServer)
        .get('/health')
        .expect(200);

      expect(response.body).toHaveProperty('status', 'healthy');
      expect(response.body).toHaveProperty('kafka');
      expect(response.body.kafka).toHaveProperty('bootstrap');
      expect(response.body.kafka.bootstrap).toHaveProperty('initialized', true);
      expect(response.body.kafka.bootstrap).toHaveProperty('retryConsumerRunning', true);
      expect(response.body.kafka.bootstrap.handlerCount).toBeGreaterThan(0);
    });

    it('should return debug information', async () => {
      const response = await request(httpServer)
        .get('/debug')
        .expect(200);

      expect(response.body).toHaveProperty('bootstrap');
      expect(response.body).toHaveProperty('handlers');
      expect(response.body).toHaveProperty('retryTopic');
      expect(response.body.bootstrap.initialized).toBe(true);
      expect(response.body.handlers.length).toBeGreaterThan(0);

      // Verify specific handlers are registered
      const handlerIds = response.body.handlers.map((h: any) => h.id);
      expect(handlerIds).toContain('AppController.handleImmediateSuccess');
      expect(handlerIds).toContain('AppController.handleRetrySuccess');
      expect(handlerIds).toContain('AppController.handleAlwaysFail');
    });

    it('should return metrics', async () => {
      const response = await request(httpServer)
        .get('/metrics')
        .expect(200);

      expect(response.body).toHaveProperty('application');
      expect(response.body).toHaveProperty('kafka');
      expect(response.body.kafka).toHaveProperty('handlers');
      expect(response.body.kafka.handlers).toBeGreaterThan(0);
    });
  });

  describe('Message Processing', () => {
    it('should process immediate success messages', async () => {
      // Send test message
      const sendResponse = await request(httpServer)
        .post('/test/send')
        .send({ scenario: 'immediate' })
        .expect(201);

      expect(sendResponse.body.success).toBe(true);
      expect(sendResponse.body.sent.topic).toBe('example.immediate.success');

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Check messages were processed
      const messagesResponse = await request(httpServer)
        .get('/messages')
        .expect(200);

      const successfulMessages = messagesResponse.body.messages.filter(
        (msg: any) => msg.status === 'success' && msg.topic === 'example.immediate.success'
      );

      expect(successfulMessages).toHaveLength(1);
      expect(successfulMessages[0].attempt).toBe(1);
    });

    it('should handle retry mechanism with eventual success', async () => {
      // Send test message that will fail 2 times, then succeed
      const sendResponse = await request(httpServer)
        .post('/test/send')
        .send({ scenario: 'retry' })
        .expect(201);

      expect(sendResponse.body.success).toBe(true);
      expect(sendResponse.body.sent.topic).toBe('example.retry.success');

      // Wait for all retries to complete (2 failures + 1 success)
      // With 2s base delay and exponential backoff: 2s, 4s delays
      await new Promise(resolve => setTimeout(resolve, 15000));

      // Check final state
      const messagesResponse = await request(httpServer)
        .get('/messages')
        .expect(200);

      const allMessages = messagesResponse.body.messages.filter(
        (msg: any) => msg.topic === 'example.retry.success'
      );

      const failedMessages = allMessages.filter((msg: any) => msg.status === 'failed');
      const successfulMessages = allMessages.filter((msg: any) => msg.status === 'success');

      // Should have 2 failures and 1 success
      expect(failedMessages).toHaveLength(2);
      expect(successfulMessages).toHaveLength(1);

      // Verify attempt counts
      expect(failedMessages[0].attempt).toBe(1);
      expect(failedMessages[1].attempt).toBe(2);
      expect(successfulMessages[0].attempt).toBe(3);

      // Verify timing - retries should be spaced out
      const firstFailTime = new Date(failedMessages[0].timestamp).getTime();
      const successTime = new Date(successfulMessages[0].timestamp).getTime();
      const timeDiff = successTime - firstFailTime;

      // Should take at least 6 seconds (2s + 4s delays)
      expect(timeDiff).toBeGreaterThan(6000);
    }, 30000); // 30 second timeout

    it('should handle max retries exceeded scenario', async () => {
      // Send message that always fails
      const sendResponse = await request(httpServer)
        .post('/test/send')
        .send({ scenario: 'fail' })
        .expect(201);

      expect(sendResponse.body.success).toBe(true);
      expect(sendResponse.body.sent.topic).toBe('example.always.fail');

      // Wait for all retry attempts to be exhausted
      await new Promise(resolve => setTimeout(resolve, 8000));

      // Check final state
      const messagesResponse = await request(httpServer)
        .get('/messages')
        .expect(200);

      const failedMessages = messagesResponse.body.messages.filter(
        (msg: any) => msg.topic === 'example.always.fail' && msg.status === 'failed'
      );

      const successfulMessages = messagesResponse.body.messages.filter(
        (msg: any) => msg.topic === 'example.always.fail' && msg.status === 'success'
      );

      // Should have 2 failures (max attempts) and no success
      expect(failedMessages).toHaveLength(2);
      expect(successfulMessages).toHaveLength(0);

      // Verify attempt counts
      expect(failedMessages[0].attempt).toBe(1);
      expect(failedMessages[1].attempt).toBe(2);
    }, 15000); // 15 second timeout

    it('should handle custom message payload', async () => {
      const customPayload = {
        id: 'custom-test-123',
        action: 'success',
        data: { userId: 456, operation: 'test' },
        timestamp: new Date().toISOString(),
      };

      // Send custom message
      const sendResponse = await request(httpServer)
        .post('/test/send')
        .send({
          topic: 'example.manual.test',
          payload: customPayload,
        })
        .expect(201);

      expect(sendResponse.body.success).toBe(true);

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Check message was processed correctly
      const messagesResponse = await request(httpServer)
        .get('/messages/example.manual.test')
        .expect(200);

      expect(messagesResponse.body.messages).toHaveLength(1);
      expect(messagesResponse.body.messages[0].status).toBe('success');
      expect(messagesResponse.body.messages[0].payload).toMatchObject(customPayload);
    });
  });

  describe('Statistics and Monitoring', () => {
    it('should track processing statistics correctly', async () => {
      // Send multiple test messages
      await request(httpServer)
        .post('/test/send')
        .send({ scenario: 'immediate' });

      await request(httpServer)
        .post('/test/send')
        .send({ scenario: 'retry' });

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 12000));

      // Check statistics
      const statsResponse = await request(httpServer)
        .get('/stats')
        .expect(200);

      expect(statsResponse.body.total).toBeGreaterThan(0);
      expect(statsResponse.body.successful).toBeGreaterThan(0);
      expect(statsResponse.body.uniqueMessages).toBeGreaterThan(0);
      expect(parseFloat(statsResponse.body.successRate)).toBeGreaterThan(0);
    }, 20000);

    it('should provide real-time metrics via health endpoint', async () => {
      const healthResponse = await request(httpServer)
        .get('/health')
        .expect(200);

      expect(healthResponse.body.kafka.retryConsumer).toHaveProperty('isRunning', true);
      expect(healthResponse.body.kafka.retryConsumer).toHaveProperty('messagesProcessed');
      expect(healthResponse.body.kafka.retryConsumer).toHaveProperty('messagesSkipped');
      expect(healthResponse.body.kafka.retryConsumer).toHaveProperty('errorsEncountered');
    });
  });

  describe('Application Lifecycle', () => {
    it('should confirm bootstrap service initialized all components', async () => {
      const debugResponse = await request(httpServer)
        .get('/debug')
        .expect(200);

      // Bootstrap service should be initialized
      expect(debugResponse.body.bootstrap.initialized).toBe(true);
      expect(debugResponse.body.bootstrap.handlerCount).toBeGreaterThan(0);
      expect(debugResponse.body.bootstrap.retryConsumerRunning).toBe(true);

      // Retry consumer should be running
      expect(debugResponse.body.retryConsumerRunning).toBe(true);

      // Handlers should be registered
      expect(debugResponse.body.handlers.length).toBeGreaterThan(0);

      // Retry topic should exist
      expect(debugResponse.body.retryTopic).toBeTruthy();
      expect(debugResponse.body.retryTopic).toMatch(/\.retry$/);
    });

    it('should have DLQ properly initialized and integrated', async () => {
      const debugResponse = await request(httpServer)
        .get('/debug')
        .expect(200);

      // DLQ should be initialized
      expect(debugResponse.body.dlq).toBeDefined();
      expect(debugResponse.body.dlq.enabled).toBe(true);
      expect(debugResponse.body.dlq.topicName).toMatch(/\.dlq$/);
      expect(debugResponse.body.dlq.isReprocessing).toBe(false);

      // DLQ topic should exist
      const topicExists = await dlqHelper.dlqTopicExists();
      expect(topicExists).toBe(true);

      // Verify topic naming pattern
      dlqHelper.verifyDlqTopicNaming('kafka-retry-example');
    });
  });

  describe('DLQ Integration with Retry Mechanism', () => {
    it('should send failed messages to DLQ after max retries exceeded', async () => {
      // Send message that will always fail and go to DLQ
      const testId = await dlqHelper.sendDlqTestMessage();

      // Wait for retries and DLQ processing
      await dlqHelper.waitForDlqProcessing(1, 12000);

      // Verify DLQ metrics
      const metrics = dlqHelper.getDlqMetrics();
      expect(metrics.messagesStored).toBeGreaterThan(0);

      // Verify in health endpoint
      const dlqHealth = await dlqHelper.verifyDlqInHealthEndpoint();
      expect(dlqHealth.metrics.messagesStored).toBeGreaterThan(0);
    }, 20000);

    it('should integrate DLQ information in monitoring endpoints', async () => {
      // Verify DLQ in health endpoint
      const healthResponse = await request(httpServer)
        .get('/health')
        .expect(200);

      expect(healthResponse.body.kafka.dlq).toBeDefined();
      expect(healthResponse.body.kafka.dlq.enabled).toBe(true);
      expect(healthResponse.body.kafka.dlq.topicName).toBe('kafka-retry-example.dlq');

      // Verify DLQ in debug endpoint
      const debugResponse = await request(httpServer)
        .get('/debug')
        .expect(200);

      expect(debugResponse.body.dlq).toBeDefined();
      expect(debugResponse.body.dlq.enabled).toBe(true);

      // Verify DLQ metrics endpoint
      const metricsResponse = await request(httpServer)
        .get('/dlq/metrics')
        .expect(200);

      expect(metricsResponse.body).toHaveProperty('messagesStored');
      expect(metricsResponse.body).toHaveProperty('messagesReprocessed');
      expect(metricsResponse.body).toHaveProperty('dlqTopicName');
    });

    it('should handle DLQ reprocessing workflow', async () => {
      // Create DLQ scenario with reprocessing
      const result = await dlqHelper.createDlqTestScenario({
        messageCount: 2,
        waitForDlq: true,
        reprocessAfter: true,
        reprocessOptions: {
          batchSize: 5,
          timeoutMs: 10000,
          stopOnError: false,
        },
      });

      expect(result.messagesSentToDlq).toBe(2);
      expect(result.reprocessingResults).toBeDefined();
      expect(result.reprocessingResults.messagesReprocessed).toBeGreaterThan(0);
    }, 30000);

    it('should prevent concurrent reprocessing sessions', async () => {
      await dlqHelper.testConcurrentReprocessingPrevention();
    }, 25000);

    it('should handle reprocessing timeout correctly', async () => {
      await dlqHelper.testReprocessingTimeout(3000);
    }, 10000);

    it('should track DLQ metrics accurately', async () => {
      // Reset to clean state
      await dlqHelper.resetDlqState();

      const initialMetrics = dlqHelper.getDlqMetrics();
      expect(initialMetrics.messagesStored).toBe(0);

      // Send messages to DLQ
      const messageIds = await dlqHelper.sendMultipleDlqMessages(3);
      await dlqHelper.waitForDlqProcessing(3);

      const postDlqMetrics = dlqHelper.getDlqMetrics();
      expect(postDlqMetrics.messagesStored).toBe(3);

      // Start reprocessing
      const reprocessResult = await dlqHelper.startReprocessingAndWait({
        batchSize: 2,
        timeoutMs: 8000,
      });

      expect(reprocessResult.reprocessingResults.messagesReprocessed).toBeGreaterThan(0);
    }, 25000);
  });
});