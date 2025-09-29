import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { KafkaModule } from '../../src/core/kafka.module';

@Module({
  imports: [
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
  ],
  controllers: [AppController], // KafkaMonitoringController is auto-registered by KafkaModule
  providers: [AppService],
})
export class AppModule {}