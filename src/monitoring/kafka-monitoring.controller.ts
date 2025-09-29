import { Controller, Get, Post, Body, Query, Logger } from '@nestjs/common';
import { KafkaHandlerRegistry } from '../services/kafka.registry';
import { KafkaRetryService } from '../services/kafka.retry.service';
import { KafkaDlqService } from '../services/kafka.dlq.service';
import { KafkaProducerService } from '../core/kafka.producer';

/**
 * Optional monitoring controller for Kafka infrastructure.
 *
 * Provides health checks, metrics, and operational endpoints.
 *
 * @example
 * // Just import and add to controllers
 * import { KafkaMonitoringController } from '@torix/nestjs-kafka';
 *
 * @Module({
 *   imports: [KafkaModule.forRoot({ ... })],
 *   controllers: [KafkaMonitoringController],
 * })
 * export class AppModule {}
 */
@Controller('kafka')
export class KafkaMonitoringController {
  private readonly logger = new Logger(KafkaMonitoringController.name);

  constructor(
    private readonly handlerRegistry: KafkaHandlerRegistry,
    private readonly retryService: KafkaRetryService,
    private readonly dlqService: KafkaDlqService,
    private readonly producerService: KafkaProducerService,
  ) {}

  /**
   * Simple health check
   * GET /kafka/health
   */
  @Get('health')
  async getHealth() {
    const producerReady = await this.producerService.isReady();

    return {
      status: producerReady ? 'healthy' : 'degraded',
      timestamp: new Date().toISOString(),
      components: {
        producer: { connected: producerReady },
        retry: { running: this.retryService.isRetryConsumerRunning() },
        dlq: { reprocessing: this.dlqService.isReprocessingActive() },
        handlers: { registered: this.handlerRegistry.getHandlerCount() },
      },
    };
  }

  /**
   * Kubernetes readiness probe
   * GET /kafka/health/ready
   */
  @Get('health/ready')
  async getReadiness() {
    const producerReady = await this.producerService.isReady();
    const handlersRegistered = this.handlerRegistry.getHandlerCount() > 0;

    return {
      ready: producerReady && handlersRegistered,
      checks: {
        producer: producerReady,
        handlers: handlersRegistered,
      },
    };
  }

  /**
   * Kubernetes liveness probe
   * GET /kafka/health/live
   */
  @Get('health/live')
  getLiveness() {
    return {
      alive: true,
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Aggregated metrics
   * GET /kafka/metrics
   */
  @Get('metrics')
  getMetrics() {
    return {
      timestamp: new Date().toISOString(),
      handlers: {
        registered: this.handlerRegistry.getHandlerCount(),
        patterns: this.handlerRegistry.getAllPatterns(),
      },
      retry: {
        topic: this.retryService.getRetryTopicName(),
        running: this.retryService.isRetryConsumerRunning(),
        ...this.retryService.getMetrics(),
      },
      dlq: {
        topic: this.dlqService.getDlqTopicName(),
        reprocessing: this.dlqService.isReprocessingActive(),
        ...this.dlqService.getMetrics(),
      },
    };
  }

  /**
   * List registered event handlers
   * GET /kafka/handlers
   */
  @Get('handlers')
  getHandlers() {
    const handlers = this.handlerRegistry.getAllHandlers();

    return {
      total: handlers.length,
      handlers: handlers.map((h) => ({
        id: h.handlerId,
        pattern: h.pattern,
        method: h.methodName,
        class: h.instance.constructor.name,
        options: h.metadata.options,
      })),
    };
  }

  /**
   * Start DLQ reprocessing for a specific topic
   * POST /kafka/dlq/reprocess
   *
   * @example
   * POST /kafka/dlq/reprocess
   * {
   *   "topic": "user-created",
   *   "batchSize": 100,
   *   "timeoutMs": 30000,
   *   "stopOnError": false
   * }
   */
  @Post('dlq/reprocess')
  async reprocessDlq(
    @Body()
    body: {
      topic: string;
      batchSize?: number;
      timeoutMs?: number;
      stopOnError?: boolean;
    },
  ) {
    try {
      // Validate required topic parameter
      if (!body.topic) {
        return {
          success: false,
          message: 'Topic parameter is required for DLQ reprocessing',
        };
      }

      if (this.dlqService.isReprocessingActive()) {
        return {
          success: false,
          message: 'DLQ reprocessing already in progress',
        };
      }

      await this.dlqService.startReprocessing({
        topic: body.topic,
        batchSize: body.batchSize || 100,
        timeoutMs: body.timeoutMs || 30000,
        stopOnError: body.stopOnError ?? false,
      });

      this.logger.log(`DLQ reprocessing started for topic: ${body.topic}`);

      return {
        success: true,
        message: `DLQ reprocessing started for topic: ${body.topic}`,
      };
    } catch (error) {
      this.logger.error('Failed to start DLQ reprocessing', error);
      return {
        success: false,
        message: error.message,
      };
    }
  }

  /**
   * Stop DLQ reprocessing
   * POST /kafka/dlq/stop
   */
  @Post('dlq/stop')
  async stopDlqReprocessing() {
    try {
      if (!this.dlqService.isReprocessingActive()) {
        return {
          success: false,
          message: 'DLQ reprocessing is not active',
        };
      }

      await this.dlqService.stopReprocessing();

      this.logger.log('DLQ reprocessing stopped');

      return {
        success: true,
        message: 'DLQ reprocessing stopped',
      };
    } catch (error) {
      this.logger.error('Failed to stop DLQ reprocessing', error);
      return {
        success: false,
        message: error.message,
      };
    }
  }

  /**
   * Reset metrics
   * POST /kafka/metrics/reset
   */
  @Post('metrics/reset')
  resetMetrics(@Query('component') component: 'retry' | 'dlq' | 'all' = 'all') {
    const reset: string[] = [];

    if (component === 'retry' || component === 'all') {
      this.retryService.resetMetrics();
      reset.push('retry');
    }

    if (component === 'dlq' || component === 'all') {
      this.dlqService.resetMetrics();
      reset.push('dlq');
    }

    this.logger.log(`Metrics reset: ${reset.join(', ')}`);

    return {
      success: true,
      message: 'Metrics reset successfully',
      components: reset,
    };
  }
}