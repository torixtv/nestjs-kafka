import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { HealthController } from './health.controller';
import { KafkaModule } from '../../src/core/kafka.module';

@Module({
  imports: [
    // Kafka module with retry, DLQ, and health indicator support
    KafkaModule.forRoot({
      client: {
        clientId: 'kafka-retry-example',
        brokers: ['localhost:9092'],
      },
      consumer: {
        groupId: 'kafka-retry-example-group',
      },
      subscriptions: {
        topics: [
          'example.immediate.success',
          'example.retry.success',
          'example.always.fail',
          'example.manual.test',
          'example.dlq.test',
          'example.dlq.disabled',
        ],
        fromBeginning: true,
      },
      retry: {
        enabled: true,
        attempts: 3,
        baseDelay: 2000, // 2 seconds for demonstration
        maxDelay: 10000, // 10 seconds max
        backoff: 'exponential',
        topicPartitions: 3,
        topicReplicationFactor: 1,
        topicRetentionMs: 24 * 60 * 60 * 1000, // 24 hours
        topicSegmentMs: 60 * 60 * 1000, // 1 hour
      },
      dlq: {
        enabled: true,
      },
      // monitoring is enabled by default - no need to specify
      // monitoring: { enabled: true, path: 'kafka' },
    }),
    // TerminusModule provides HealthCheckService needed by KafkaHealthIndicator
    // KafkaHealthIndicator is provided by KafkaModule (which is @Global)
    TerminusModule,
  ],
  controllers: [
    AppController,
    HealthController, // Terminus-based health checks (GET /health, /health/live, /health/ready)
    // Note: KafkaMonitoringController is auto-registered by KafkaModule (GET /kafka/*)
  ],
  providers: [AppService],
})
export class AppModule {}