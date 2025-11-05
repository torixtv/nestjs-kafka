import { Module } from '@nestjs/common';
import { KafkaHealthIndicator } from './kafka-health.indicator';
import { KafkaModule } from '../core/kafka.module';

/**
 * Optional module for Kafka health checks with @nestjs/terminus integration.
 *
 * This module provides the KafkaHealthIndicator service that can be used
 * with @nestjs/terminus HealthCheckService for Kubernetes readiness/liveness probes.
 *
 * **Usage:**
 *
 * 1. Import this module alongside your KafkaModule configuration
 * 2. Inject KafkaHealthIndicator into your health check service
 * 3. Use it with @nestjs/terminus HealthCheckService
 *
 * @example
 * // In your AppModule
 * import { Module } from '@nestjs/common';
 * import { TerminusModule } from '@nestjs/terminus';
 * import { KafkaModule, KafkaHealthModule } from '@torixtv/nestjs-kafka';
 * import { HealthController } from './health.controller';
 *
 * @Module({
 *   imports: [
 *     KafkaModule.forRoot({
 *       client: { brokers: ['localhost:9092'] },
 *       consumer: { groupId: 'my-service' },
 *     }),
 *     KafkaHealthModule, // Add this to enable health checks
 *     TerminusModule,
 *   ],
 *   controllers: [HealthController],
 * })
 * export class AppModule {}
 *
 * @example
 * // In your HealthController
 * import { Controller, Get } from '@nestjs/common';
 * import { HealthCheck, HealthCheckService, HealthCheckResult } from '@nestjs/terminus';
 * import { KafkaHealthIndicator } from '@torixtv/nestjs-kafka';
 *
 * @Controller('health')
 * export class HealthController {
 *   constructor(
 *     private health: HealthCheckService,
 *     private kafkaHealth: KafkaHealthIndicator,
 *   ) {}
 *
 *   @Get()
 *   @HealthCheck()
 *   check(): Promise<HealthCheckResult> {
 *     return this.health.check([
 *       () => this.kafkaHealth.isHealthy('kafka'),
 *     ]);
 *   }
 * }
 *
 * @example
 * // Standalone usage (without @nestjs/terminus)
 * import { Injectable } from '@nestjs/common';
 * import { KafkaHealthIndicator } from '@torixtv/nestjs-kafka';
 *
 * @Injectable()
 * export class MyService {
 *   constructor(private kafkaHealth: KafkaHealthIndicator) {}
 *
 *   async checkKafkaStatus() {
 *     const result = await this.kafkaHealth.checkHealth();
 *     console.log(`Kafka status: ${result.status}`);
 *     return result;
 *   }
 * }
 */
@Module({
  imports: [
    // Import KafkaModule to ensure all Kafka services are available
    // Note: This assumes KafkaModule is already configured at the root level
  ],
  providers: [KafkaHealthIndicator],
  exports: [KafkaHealthIndicator],
})
export class KafkaHealthModule {}
