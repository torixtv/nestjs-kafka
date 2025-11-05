import { Injectable, Logger, Optional } from '@nestjs/common';
import { KafkaProducerService } from '../core/kafka.producer';
import { KafkaConsumerService } from '../services/kafka.consumer.service';
import { KafkaBootstrapService } from '../services/kafka.bootstrap.service';
import { KafkaHandlerRegistry } from '../services/kafka.registry';
import { KafkaRetryService } from '../services/kafka.retry.service';

/**
 * Component health status details
 */
export interface ComponentHealth {
  healthy: boolean;
  message?: string;
  details?: Record<string, any>;
}

/**
 * Complete Kafka health check result
 */
export interface KafkaHealthResult {
  status: 'healthy' | 'unhealthy';
  timestamp: string;
  components: {
    broker: ComponentHealth;
    bootstrap: ComponentHealth;
    producer: ComponentHealth;
    consumer: ComponentHealth;
    handlers: ComponentHealth;
    // Non-critical components (for informational purposes only)
    retry?: ComponentHealth;
  };
}

/**
 * Health indicator for Kafka infrastructure.
 *
 * This service checks critical Kafka components and determines overall health status.
 * It is designed to work with @nestjs/terminus for Kubernetes readiness/liveness probes.
 *
 * **Critical Checks (must pass for healthy status):**
 * - Broker reachability (producer or consumer connected)
 * - Bootstrap initialization complete
 *
 * **Non-Critical Checks (informational only):**
 * - Partition assignment issues
 * - Retry consumer status
 * - Individual consumer scaling beyond partition count
 *
 * @example
 * // Using with @nestjs/terminus
 * import { Injectable } from '@nestjs/common';
 * import { HealthCheckService, HealthIndicatorResult } from '@nestjs/terminus';
 * import { KafkaHealthIndicator } from '@torixtv/nestjs-kafka';
 *
 * @Injectable()
 * export class HealthService {
 *   constructor(
 *     private health: HealthCheckService,
 *     private kafkaHealth: KafkaHealthIndicator,
 *   ) {}
 *
 *   check() {
 *     return this.health.check([
 *       () => this.kafkaHealth.isHealthy('kafka'),
 *     ]);
 *   }
 * }
 *
 * @example
 * // Using standalone (without @nestjs/terminus)
 * const healthResult = await kafkaHealthIndicator.checkHealth();
 * if (healthResult.status === 'healthy') {
 *   console.log('Kafka is healthy');
 * }
 */
@Injectable()
export class KafkaHealthIndicator {
  private readonly logger = new Logger(KafkaHealthIndicator.name);

  constructor(
    @Optional() private readonly producerService: KafkaProducerService,
    @Optional() private readonly consumerService: KafkaConsumerService,
    @Optional() private readonly bootstrapService: KafkaBootstrapService,
    @Optional() private readonly handlerRegistry: KafkaHandlerRegistry,
    @Optional() private readonly retryService: KafkaRetryService,
  ) {}

  /**
   * Check Kafka health and return detailed status.
   *
   * This method performs comprehensive health checks on all Kafka components
   * and returns a detailed health status object.
   *
   * @returns Detailed health check result
   */
  async checkHealth(): Promise<KafkaHealthResult> {
    const timestamp = new Date().toISOString();

    // Check critical components
    const brokerHealth = await this.checkBrokerConnection();
    const bootstrapHealth = this.checkBootstrapStatus();

    // Check additional components (informational)
    const producerHealth = await this.checkProducerHealth();
    const consumerHealth = this.checkConsumerHealth();
    const handlersHealth = this.checkHandlersRegistered();
    const retryHealth = this.checkRetryConsumerStatus();

    // Determine overall health status based on CRITICAL checks only
    const isCriticalHealthy = brokerHealth.healthy && bootstrapHealth.healthy;

    return {
      status: isCriticalHealthy ? 'healthy' : 'unhealthy',
      timestamp,
      components: {
        broker: brokerHealth,
        bootstrap: bootstrapHealth,
        producer: producerHealth,
        consumer: consumerHealth,
        handlers: handlersHealth,
        retry: retryHealth,
      },
    };
  }

  /**
   * Check if Kafka is healthy (for @nestjs/terminus integration).
   *
   * This method is designed to work with @nestjs/terminus HealthCheckService.
   * It returns a HealthIndicatorResult compatible with Terminus.
   *
   * @param key The health check key (typically 'kafka')
   * @returns Health indicator result for Terminus
   *
   * @example
   * // In your health controller
   * @Get('health')
   * @HealthCheck()
   * check() {
   *   return this.health.check([
   *     () => this.kafkaHealth.isHealthy('kafka'),
   *   ]);
   * }
   */
  async isHealthy(key: string): Promise<Record<string, any>> {
    const healthResult = await this.checkHealth();

    // Return Terminus-compatible result
    return {
      [key]: {
        status: healthResult.status,
        ...healthResult.components,
      },
    };
  }

  /**
   * Check broker connection health.
   *
   * The broker is considered healthy if EITHER:
   * - Producer is connected, OR
   * - Consumer is connected
   *
   * This allows the service to be healthy even if one component is down,
   * as long as we can communicate with Kafka brokers.
   */
  private async checkBrokerConnection(): Promise<ComponentHealth> {
    try {
      const producerReady = this.producerService
        ? await this.producerService.isReady()
        : false;

      const consumerConnected = this.consumerService
        ? (this.consumerService as any).isConnected
        : false;

      const brokerReachable = producerReady || consumerConnected;

      if (brokerReachable) {
        return {
          healthy: true,
          message: 'Broker connection established',
          details: {
            producerReady,
            consumerConnected,
          },
        };
      } else {
        return {
          healthy: false,
          message: 'Unable to connect to Kafka brokers',
          details: {
            producerReady,
            consumerConnected,
          },
        };
      }
    } catch (error) {
      this.logger.error('Error checking broker connection', error);
      return {
        healthy: false,
        message: `Broker connection check failed: ${error.message}`,
      };
    }
  }

  /**
   * Check if bootstrap initialization is complete.
   *
   * Bootstrap must be complete for the service to process messages correctly.
   * This ensures:
   * - Handler registry has discovered all @EventHandler decorators
   * - Retry system is initialized (if enabled)
   * - DLQ system is initialized (if enabled)
   * - Main consumer is ready to process messages
   */
  private checkBootstrapStatus(): ComponentHealth {
    try {
      if (!this.bootstrapService) {
        return {
          healthy: false,
          message: 'Bootstrap service not available',
        };
      }

      const isInitialized = (this.bootstrapService as any).isInitialized;

      if (isInitialized) {
        return {
          healthy: true,
          message: 'Bootstrap initialization complete',
        };
      } else {
        return {
          healthy: false,
          message: 'Bootstrap initialization not complete',
        };
      }
    } catch (error) {
      this.logger.error('Error checking bootstrap status', error);
      return {
        healthy: false,
        message: `Bootstrap status check failed: ${error.message}`,
      };
    }
  }

  /**
   * Check producer health (informational).
   */
  private async checkProducerHealth(): Promise<ComponentHealth> {
    try {
      if (!this.producerService) {
        return {
          healthy: false,
          message: 'Producer service not available',
        };
      }

      const isReady = await this.producerService.isReady();

      return {
        healthy: isReady,
        message: isReady ? 'Producer connected' : 'Producer not connected',
      };
    } catch (error) {
      return {
        healthy: false,
        message: `Producer check failed: ${error.message}`,
      };
    }
  }

  /**
   * Check consumer health (informational).
   *
   * Note: Consumer may be connected but not have partitions assigned if
   * the number of consumers in the group exceeds the number of partitions.
   * This is NOT considered an error - the consumer is ready and will receive
   * partitions when available (e.g., after rebalancing or scaling down).
   */
  private checkConsumerHealth(): ComponentHealth {
    try {
      if (!this.consumerService) {
        return {
          healthy: false,
          message: 'Consumer service not available',
        };
      }

      const isConnected = (this.consumerService as any).isConnected;

      return {
        healthy: isConnected,
        message: isConnected
          ? 'Consumer connected (may be waiting for partition assignment)'
          : 'Consumer not connected',
        details: {
          note: 'No partitions assigned is normal when consumer group size > partition count',
        },
      };
    } catch (error) {
      return {
        healthy: false,
        message: `Consumer check failed: ${error.message}`,
      };
    }
  }

  /**
   * Check if handlers are registered (informational).
   */
  private checkHandlersRegistered(): ComponentHealth {
    try {
      if (!this.handlerRegistry) {
        return {
          healthy: false,
          message: 'Handler registry not available',
        };
      }

      const handlerCount = this.handlerRegistry.getHandlerCount();

      return {
        healthy: handlerCount > 0,
        message:
          handlerCount > 0
            ? `${handlerCount} handler(s) registered`
            : 'No handlers registered',
        details: {
          count: handlerCount,
        },
      };
    } catch (error) {
      return {
        healthy: false,
        message: `Handler check failed: ${error.message}`,
      };
    }
  }

  /**
   * Check retry consumer status (non-critical, informational only).
   *
   * Retry consumer issues should NOT affect overall health status.
   * If the retry consumer is not running, new messages will still be processed,
   * but failed messages won't be retried.
   */
  private checkRetryConsumerStatus(): ComponentHealth {
    try {
      if (!this.retryService) {
        return {
          healthy: true,
          message: 'Retry service not configured',
        };
      }

      const isRunning = this.retryService.isRetryConsumerRunning();

      return {
        healthy: true, // Always healthy - retry is non-critical
        message: isRunning
          ? 'Retry consumer running'
          : 'Retry consumer not running',
        details: {
          running: isRunning,
          note: 'Retry consumer status does not affect overall health',
        },
      };
    } catch (error) {
      return {
        healthy: true, // Still healthy - retry is non-critical
        message: `Retry consumer status unknown: ${error.message}`,
      };
    }
  }
}
