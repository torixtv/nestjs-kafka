import { Controller, Get, Post, Body, Param, Logger, Injectable } from '@nestjs/common';
import { EventHandler } from '../../src/decorators/event-handler.decorator';
import { KafkaProducerService } from '../../src/core/kafka.producer';
import { KafkaHandlerRegistry } from '../../src/services/kafka.registry';
import { KafkaRetryService } from '../../src/services/kafka.retry.service';
import { AppService } from './app.service';

@Injectable()
@Controller()
export class AppController {
  private readonly logger = new Logger(AppController.name);

  constructor(
    private readonly appService: AppService,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly handlerRegistry: KafkaHandlerRegistry,
    private readonly retryService: KafkaRetryService,
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

  // === HTTP Endpoints ===

  @Get('/')
  getWelcome() {
    return {
      message: 'Kafka Retry Example Application',
      description: 'NestJS application demonstrating Kafka retry mechanism with full lifecycle support',
      endpoints: {
        health: '/health',
        metrics: '/metrics',
        debug: '/debug',
        messages: '/messages',
        stats: '/stats',
        send: 'POST /test/send',
        reset: 'POST /reset',
      },
    };
  }

  @Get('health')
  getHealth() {
    const retryMetrics = this.retryService.getMetrics();

    return {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      kafka: {
        retryService: retryMetrics,
        retryTopic: this.retryService.getRetryTopicName(),
        retryConsumerRunning: this.retryService.isRetryConsumerRunning(),
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

    return {
      timestamp: new Date().toISOString(),
      application: appStats,
      kafka: {
        retry: retryMetrics,
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
    };

    let messageToSend;

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

    return {
      success: true,
      message: 'Application state reset',
      timestamp: new Date().toISOString(),
    };
  }
}