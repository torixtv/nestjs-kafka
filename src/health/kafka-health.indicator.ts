import { Injectable, Logger, Optional } from '@nestjs/common';
import { HealthIndicatorResult, HealthIndicatorService } from '@nestjs/terminus';
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
 * - Consumer has partitions assigned (with smart grace periods)
 *
 * **Non-Critical Checks (informational only):**
 * - Retry consumer status
 *
 * **Smart Grace Periods:**
 * The consumer health check includes grace periods to avoid false positives:
 * - Startup: 3 minutes grace period for initial partition assignment
 * - Rebalancing: 2 minutes grace period during normal rebalances
 * - Stale threshold: 10 minutes without partitions triggers unhealthy status
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
    private readonly healthIndicatorService: HealthIndicatorService,
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
    const consumerHealth = this.checkConsumerHealth();

    // Check additional components (informational)
    const producerHealth = await this.checkProducerHealth();
    const handlersHealth = this.checkHandlersRegistered();
    const retryHealth = this.checkRetryConsumerStatus();

    // Determine overall health status based on CRITICAL checks
    // Consumer health is now critical - but uses smart grace periods internally
    const isCriticalHealthy = brokerHealth.healthy && bootstrapHealth.healthy && consumerHealth.healthy;

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
  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    const check = this.healthIndicatorService.check(key);

    try {
      const healthResult = await this.checkHealth();

      if (healthResult.status === 'healthy') {
        // Pass all component details to the up() method
        return check.up(healthResult.components);
      } else {
        // Pass all component details to the down() method
        return check.down(healthResult.components);
      }
    } catch (error) {
      this.logger.error('Kafka health check failed', error);
      return check.down({
        message: error instanceof Error ? error.message : 'Unknown Kafka health check error',
      });
    }
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
   * Check consumer health using smart state tracking.
   *
   * This check uses grace periods to avoid false positives during:
   * - Startup (3 minutes grace period)
   * - Rebalancing (2 minutes grace period)
   *
   * Only reports unhealthy when consumer is truly stale:
   * - Disconnected from broker
   * - No partitions assigned for > 10 minutes (after startup)
   *
   * This is now a CRITICAL check - unhealthy consumer will trigger pod restart.
   */
  private checkConsumerHealth(): ComponentHealth {
    try {
      if (!this.consumerService) {
        return {
          healthy: false,
          message: 'Consumer service not available',
        };
      }

      // Use the smart health state tracking with grace periods
      const healthState = this.consumerService.getHealthState();
      const assignedPartitions = this.consumerService.getAssignedPartitions();
      const groupInfo = this.consumerService.getGroupInfo();

      // Group partitions by topic for clearer output
      const topicPartitions = this.groupPartitionsByTopic(assignedPartitions);

      return {
        healthy: healthState.isHealthy,
        message: healthState.reason,
        details: {
          state: healthState.state,
          // Include consumer group metadata when available
          ...(groupInfo && {
            groupId: groupInfo.groupId,
            memberId: groupInfo.memberId,
            isLeader: groupInfo.isLeader,
          }),
          partitionsAssigned: assignedPartitions.length,
          // New format: group by topic with partition list
          topics: topicPartitions,
          // Keep original format for backwards compatibility
          partitions: assignedPartitions,
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
   * Group partition assignments by topic for clearer health output.
   * Returns an object where keys are topic names and values are arrays of partition numbers.
   *
   * Example: { "mux.video.events": [0], "video.asset.ready": [0, 1, 2] }
   */
  private groupPartitionsByTopic(partitions: { topic: string; partition: number }[]): Record<string, number[]> {
    const grouped: Record<string, number[]> = {};
    for (const { topic, partition } of partitions) {
      if (!grouped[topic]) {
        grouped[topic] = [];
      }
      grouped[topic].push(partition);
    }
    // Sort partitions within each topic for consistent output
    for (const topic of Object.keys(grouped)) {
      grouped[topic].sort((a, b) => a - b);
    }
    return grouped;
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
