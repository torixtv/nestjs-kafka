import { Controller, Get, Post, Body, Param, Logger, Injectable, UseInterceptors } from '@nestjs/common';
import { EventHandler } from '../../src/decorators/event-handler.decorator';
import { KafkaProducerService } from '../../src/core/kafka.producer';
import { KafkaHandlerRegistry } from '../../src/services/kafka.registry';
import { KafkaRetryService } from '../../src/services/kafka.retry.service';
import { KafkaDlqService } from '../../src/services/kafka.dlq.service';
import { RetryInterceptor } from '../../src/interceptors/retry.interceptor';
import { AppService } from './app.service';
import { trace } from '@opentelemetry/api';

@Injectable()
@Controller()
@UseInterceptors(RetryInterceptor)
export class AppController {
  private readonly logger = new Logger(AppController.name);

  constructor(
    private readonly appService: AppService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly handlerRegistry: KafkaHandlerRegistry,
    private readonly retryService: KafkaRetryService,
    private readonly dlqService: KafkaDlqService,
  ) {}

  // === Kafka Event Handlers ===

  @EventHandler('example.immediate.success')
  async handleImmediateSuccess(payload: any) {
    this.logger.log(`📨 Received immediate success message: ${JSON.stringify(payload)}`);

    // This always succeeds
    this.appService.recordSuccess('example.immediate.success', payload);

    return { success: true, message: 'Processed immediately' };
  }

  @EventHandler('example.retry.success', {
    retry: {
      enabled: true,
      attempts: 3,
      baseDelay: 2000, // 2 seconds
      maxDelay: 8000,  // 8 seconds max
      backoff: 'exponential',
    },
  })
  async handleRetrySuccess(payload: any) {
    this.logger.log(`📨 Received retry success message: ${JSON.stringify(payload)}`);

    const attempt = this.appService.getAttemptCount(payload.id);

    // Fail first 2 attempts, succeed on 3rd
    if (payload.shouldFail && attempt < 2) {
      const error = `Simulated failure ${attempt + 1} for ${payload.id}`;
      this.appService.recordFailure('example.retry.success', payload, error);
      throw new Error(error);
    }

    // Success on 3rd attempt (or immediate if shouldFail is false)
    this.appService.recordSuccess('example.retry.success', payload);
    return { success: true, message: `Processed on attempt ${attempt + 1}` };
  }

  @EventHandler('example.always.fail', {
    retry: {
      enabled: true,
      attempts: 2, // Low attempts for faster testing
      baseDelay: 1000,
      maxDelay: 3000,
      backoff: 'linear',
    },
    dlq: {
      enabled: true,
    },
  })
  async handleAlwaysFail(payload: any) {
    this.logger.log(`📨 Received always fail message: ${JSON.stringify(payload)}`);

    const attempt = this.appService.getAttemptCount(payload.id);
    const error = `Always fails attempt ${attempt + 1} for ${payload.id}`;

    this.appService.recordFailure('example.always.fail', payload, error);
    throw new Error(error);
  }

  @EventHandler('example.manual.test')
  async handleManualTest(payload: any) {
    this.logger.log(`📨 Received manual test message: ${JSON.stringify(payload)}`);

    if (payload.action === 'fail') {
      const error = `Manual test failure for ${payload.id}`;
      this.appService.recordFailure('example.manual.test', payload, error);
      throw new Error(error);
    }

    this.appService.recordSuccess('example.manual.test', payload);
    return { success: true, message: 'Manual test processed successfully' };
  }

  @EventHandler('example.dlq.test', {
    retry: {
      enabled: true,
      attempts: 2, // Low attempts for faster testing
      baseDelay: 1000,
      maxDelay: 3000,
      backoff: 'linear',
    },
    dlq: {
      enabled: true,
    },
  })
  async handleDlqTest(payload: any) {
    this.logger.log(`📨 Received DLQ test message: ${JSON.stringify(payload)}`);

    const attempt = this.appService.getAttemptCount(payload.id);

    if (payload.action === 'fail') {
      const error = `DLQ test failure attempt ${attempt + 1} for ${payload.id}`;
      this.appService.recordFailure('example.dlq.test', payload, error);
      throw new Error(error);
    } else if (payload.action === 'succeed-on-reprocess') {
      // This handler will fail during normal processing but succeed during DLQ reprocessing
      const isReprocessing = payload.isReprocessing || false;
      if (!isReprocessing) {
        const error = `DLQ test - will succeed on reprocessing for ${payload.id}`;
        this.appService.recordFailure('example.dlq.test', payload, error);
        throw new Error(error);
      }
    }

    this.appService.recordSuccess('example.dlq.test', payload);
    return { success: true, message: `DLQ test processed successfully on attempt ${attempt + 1}` };
  }

  @EventHandler('example.dlq.disabled', {
    retry: {
      enabled: true,
      attempts: 2,
      baseDelay: 1000,
      maxDelay: 3000,
      backoff: 'linear',
    },
    dlq: {
      enabled: false, // DLQ disabled for this handler
    },
  })
  async handleDlqDisabled(payload: any) {
    this.logger.log(`📨 Received DLQ disabled test message: ${JSON.stringify(payload)}`);

    const attempt = this.appService.getAttemptCount(payload.id);
    const error = `DLQ disabled test failure attempt ${attempt + 1} for ${payload.id}`;

    this.appService.recordFailure('example.dlq.disabled', payload, error);
    throw new Error(error);
  }

  // === HTTP Endpoints ===

  @Get('/')
  getWelcome() {
    return {
      message: 'Kafka Retry Example Application',
      description: 'NestJS application demonstrating Kafka retry mechanism with DLQ support and full lifecycle support',
      endpoints: {
        health: '/health',
        metrics: '/metrics',
        debug: '/debug',
        messages: '/messages',
        stats: '/stats',
        send: 'POST /test/send',
        trace: 'POST /test/trace',
        reset: 'POST /reset',
        dlq: {
          status: '/dlq/status',
          metrics: '/dlq/metrics',
          reprocess: 'POST /dlq/reprocess',
          stop: 'POST /dlq/stop',
        },
      },
    };
  }

  @Get('health')
  getHealth() {
    const retryMetrics = this.retryService.getMetrics();
    const dlqMetrics = this.dlqService.getMetrics();

    return {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      kafka: {
        retryService: retryMetrics,
        retryTopic: this.retryService.getRetryTopicName(),
        retryConsumerRunning: this.retryService.isRetryConsumerRunning(),
        dlq: {
          enabled: true,
          topicName: this.dlqService.getDlqTopicName(),
          isReprocessing: this.dlqService.isReprocessingActive(),
          metrics: dlqMetrics,
        },
      },
      application: {
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        pid: process.pid,
      },
    };
  }

  @Get('metrics')
  getMetrics() {
    const appStats = this.appService.getStats();
    const retryMetrics = this.retryService.getMetrics();
    const dlqMetrics = this.dlqService.getMetrics();

    return {
      timestamp: new Date().toISOString(),
      application: appStats,
      kafka: {
        retry: retryMetrics,
        dlq: dlqMetrics,
        handlers: this.handlerRegistry.getAllHandlers().length,
        topics: this.handlerRegistry.getAllPatterns(),
      },
    };
  }

  @Get('debug')
  getDebug() {
    const handlers = this.handlerRegistry.getAllHandlers();

    return {
      timestamp: new Date().toISOString(),
      handlers: handlers.map(h => ({
        id: h.handlerId,
        pattern: h.pattern,
        metadata: h.metadata,
        className: h.instance.constructor.name,
      })),
      retryTopic: this.retryService.getRetryTopicName(),
      retryConsumerRunning: this.retryService.isRetryConsumerRunning(),
      dlq: {
        topicName: this.dlqService.getDlqTopicName(),
        enabled: true,
        isReprocessing: this.dlqService.isReprocessingActive(),
        metrics: this.dlqService.getMetrics(),
      },
      environment: {
        nodeEnv: process.env.NODE_ENV,
        kafkaBrokers: process.env.KAFKA_BROKERS || 'localhost:9092',
      },
    };
  }

  @Get('messages')
  getMessages() {
    return {
      timestamp: new Date().toISOString(),
      messages: this.appService.getAllMessages(),
      stats: this.appService.getStats(),
    };
  }

  @Get('messages/:topic')
  getMessagesByTopic(@Param('topic') topic: string) {
    return {
      timestamp: new Date().toISOString(),
      topic,
      messages: this.appService.getMessagesByTopic(topic),
    };
  }

  @Get('stats')
  getStats() {
    return {
      timestamp: new Date().toISOString(),
      ...this.appService.getStats(),
    };
  }

  @Post('test/trace')
  async sendTracedMessage(@Body() body: {
    topic?: string;
    payload?: any;
    scenario?: string;
    correlationId?: string;
  }) {
    const { topic, payload, scenario, correlationId } = body;

    // Get current trace context for response info
    const activeSpan = trace.getActiveSpan();
    const traceId = activeSpan?.spanContext().traceId;
    const spanId = activeSpan?.spanContext().spanId;

    try {
      // Predefined test scenarios - correlation ID will be auto-generated if not provided
      const scenarios = {
        'trace-immediate': {
          topic: 'example.immediate.success',
          payload: {
            id: `trace-immediate-${Date.now()}`,
            message: 'Test immediate success with auto tracing',
          },
        },
        'trace-retry': {
          topic: 'example.retry.success',
          payload: {
            id: `trace-retry-${Date.now()}`,
            shouldFail: true,
            message: 'Test retry mechanism with auto tracing',
          },
        },
        'trace-dlq': {
          topic: 'example.dlq.test',
          payload: {
            id: `trace-dlq-${Date.now()}`,
            action: 'fail',
            message: 'Test DLQ functionality with auto tracing',
          },
        },
      };

      let messageToSend: { topic: string; payload: any };

      if (scenario && scenarios[scenario]) {
        messageToSend = scenarios[scenario];
      } else if (topic && payload) {
        messageToSend = { topic, payload };
      } else {
        // Default to trace-retry scenario
        messageToSend = scenarios['trace-retry'];
      }

      // Send message with automatic correlation ID propagation
      // The package will automatically generate and inject correlation ID
      await this.kafkaProducer.send(messageToSend.topic, {
        key: messageToSend.payload.id,
        value: messageToSend.payload,
        correlationId, // Optional: provide explicit correlation ID
      });

      this.logger.log(`📤 Sent traced message to ${messageToSend.topic}: ${JSON.stringify(messageToSend.payload)}`);

      return {
        success: true,
        message: 'Traced message sent successfully (automatic correlation propagation)',
        sent: messageToSend,
        trace: {
          traceId,
          spanId,
          correlationId: correlationId || 'auto-generated',
        },
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`❌ Failed to send traced message:`, error);

      return {
        success: false,
        error: error.message,
        trace: { traceId, spanId },
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Post('test/send')
  async sendTestMessage(@Body() body: { topic?: string; payload?: any; scenario?: string }) {
    const { topic, payload, scenario } = body;

    // Predefined test scenarios
    const scenarios = {
      immediate: {
        topic: 'example.immediate.success',
        payload: { id: `immediate-${Date.now()}`, message: 'Test immediate success' },
      },
      retry: {
        topic: 'example.retry.success',
        payload: { id: `retry-${Date.now()}`, shouldFail: true, message: 'Test retry mechanism' },
      },
      fail: {
        topic: 'example.always.fail',
        payload: { id: `fail-${Date.now()}`, message: 'Test failure scenario' },
      },
      manual: {
        topic: 'example.manual.test',
        payload: { id: `manual-${Date.now()}`, action: 'success', message: 'Manual test' },
      },
      dlq: {
        topic: 'example.dlq.test',
        payload: { id: `dlq-${Date.now()}`, action: 'fail', message: 'Test DLQ functionality' },
      },
      'dlq-reprocess': {
        topic: 'example.dlq.test',
        payload: { id: `dlq-reprocess-${Date.now()}`, action: 'succeed-on-reprocess', message: 'Test DLQ reprocessing' },
      },
      'dlq-disabled': {
        topic: 'example.dlq.disabled',
        payload: { id: `dlq-disabled-${Date.now()}`, action: 'fail', message: 'Test DLQ disabled scenario' },
      },
    };

    let messageToSend: { topic: string; payload: any };

    if (scenario && scenarios[scenario]) {
      messageToSend = scenarios[scenario];
    } else if (topic && payload) {
      messageToSend = { topic, payload };
    } else {
      // Default to retry scenario
      messageToSend = scenarios.retry;
    }

    try {
      await this.kafkaProducer.send(messageToSend.topic, {
        key: messageToSend.payload.id,
        value: messageToSend.payload,
      });

      this.logger.log(`📤 Sent test message to ${messageToSend.topic}: ${JSON.stringify(messageToSend.payload)}`);

      return {
        success: true,
        message: 'Test message sent successfully',
        sent: messageToSend,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`❌ Failed to send test message:`, error);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Post('reset')
  reset() {
    this.appService.reset();
    this.retryService.resetMetrics();
    this.dlqService.resetMetrics();

    return {
      success: true,
      message: 'Application state reset',
      timestamp: new Date().toISOString(),
    };
  }

  // === DLQ-specific Endpoints ===

  @Get('dlq/status')
  getDlqStatus() {
    return {
      timestamp: new Date().toISOString(),
      dlq: {
        topicName: this.dlqService.getDlqTopicName(),
        enabled: true,
        isReprocessing: this.dlqService.isReprocessingActive(),
        topicExists: this.dlqService.dlqTopicExists(),
      },
    };
  }

  @Get('dlq/metrics')
  getDlqMetrics() {
    const metrics = this.dlqService.getMetrics();

    return {
      timestamp: new Date().toISOString(),
      ...metrics,
    };
  }

  @Post('dlq/reprocess')
  async startDlqReprocessing(@Body() body?: {
    batchSize?: number;
    timeoutMs?: number;
    stopOnError?: boolean;
  }) {
    try {
      if (this.dlqService.isReprocessingActive()) {
        return {
          success: false,
          message: 'DLQ reprocessing is already in progress',
          timestamp: new Date().toISOString(),
        };
      }

      const options = {
        batchSize: body?.batchSize || 100,
        timeoutMs: body?.timeoutMs || 30000,
        stopOnError: body?.stopOnError ?? false,
      };

      // Start reprocessing in background
      this.dlqService.startReprocessing(options);

      this.logger.log(`🔄 DLQ reprocessing started with options: ${JSON.stringify(options)}`);

      return {
        success: true,
        message: 'DLQ reprocessing started successfully',
        options,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error('❌ Failed to start DLQ reprocessing:', error);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Post('dlq/stop')
  async stopDlqReprocessing() {
    try {
      if (!this.dlqService.isReprocessingActive()) {
        return {
          success: false,
          message: 'DLQ reprocessing is not currently active',
          timestamp: new Date().toISOString(),
        };
      }

      await this.dlqService.stopReprocessing();

      this.logger.log('⏹️ DLQ reprocessing stopped');

      return {
        success: true,
        message: 'DLQ reprocessing stopped successfully',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error('❌ Failed to stop DLQ reprocessing:', error);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }
}