import { Controller, Get } from '@nestjs/common';
import {
  HealthCheck,
  HealthCheckService,
  HealthCheckResult,
} from '@nestjs/terminus';
import { KafkaHealthIndicator } from '../../src/health';

/**
 * Health check controller using @nestjs/terminus
 *
 * This controller demonstrates integration with the KafkaHealthIndicator
 * from @torixtv/nestjs-kafka for production-ready health checks.
 *
 * **Endpoints:**
 * - GET /health - Comprehensive health check (Kafka + application)
 * - GET /health/live - Liveness probe (is the app running?)
 * - GET /health/ready - Readiness probe (is the app ready to serve traffic?)
 *
 * **Health Check Criteria:**
 * - ✅ Healthy: Kafka broker reachable + bootstrap complete
 * - ❌ Unhealthy: Kafka broker unreachable OR bootstrap incomplete
 * - ℹ️ Non-critical: Partition assignment issues, retry consumer status
 *
 * **Kubernetes Integration:**
 * ```yaml
 * livenessProbe:
 *   httpGet:
 *     path: /health/live
 *     port: 3000
 * readinessProbe:
 *   httpGet:
 *     path: /health/ready
 *     port: 3000
 * ```
 */
@Controller('health')
export class HealthController {
  constructor(
    private health: HealthCheckService,
    private kafkaHealth: KafkaHealthIndicator,
  ) {}

  /**
   * Comprehensive health check
   * GET /health
   *
   * Returns detailed health status for all components.
   * Suitable for monitoring dashboards and debugging.
   */
  @Get()
  @HealthCheck()
  check(): Promise<HealthCheckResult> {
    return this.health.check([
      // Kafka health check using the integrated health indicator
      () => this.kafkaHealth.isHealthy('kafka'),
    ]);
  }

  /**
   * Liveness probe
   * GET /health/live
   *
   * Kubernetes liveness probe - checks if the application is alive.
   * Should almost never fail (only on catastrophic failures).
   * If this fails, Kubernetes will restart the pod.
   */
  @Get('live')
  @HealthCheck()
  checkLive(): Promise<HealthCheckResult> {
    return this.health.check([
      // Just check if the service is responding
      // Kafka connection issues should NOT affect liveness
    ]);
  }

  /**
   * Readiness probe
   * GET /health/ready
   *
   * Kubernetes readiness probe - checks if the application is ready to serve traffic.
   * If this fails, Kubernetes will remove the pod from the load balancer.
   *
   * **Ready Criteria:**
   * - Kafka broker is reachable (producer OR consumer connected)
   * - Bootstrap initialization is complete
   * - Handlers are registered
   *
   * **NOT affected by:**
   * - Consumer has no partitions (normal when scaled beyond partition count)
   * - Retry consumer is down
   */
  @Get('ready')
  @HealthCheck()
  checkReady(): Promise<HealthCheckResult> {
    return this.health.check([
      // Check if Kafka is ready
      () => this.kafkaHealth.isHealthy('kafka'),
    ]);
  }
}
