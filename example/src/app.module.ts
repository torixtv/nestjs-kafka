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
      requireBroker: false, // Don't fail if Kafka is not available (for development)
    }),
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}