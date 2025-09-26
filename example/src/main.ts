// Initialize tracing BEFORE any other imports
import './tracing';

import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { AppModule } from './app.module';
import { Transport } from '@nestjs/microservices';

async function bootstrap() {
  const logger = new Logger('ExampleApp');

  logger.log('🚀 Starting Example Kafka Retry Application...');

  // Create NestJS application
  const app = await NestFactory.create(AppModule, {
    logger: ['error', 'warn', 'log', 'debug', 'verbose'],
  });

  // Enable graceful shutdown
  app.enableShutdownHooks();

  // Register Kafka microservice
  logger.log('📡 Registering Kafka microservice...');
  app.connectMicroservice({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'kafka-retry-example',
        brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
      },
      consumer: {
        groupId: 'kafka-retry-example-group',
      },
      subscribe: {
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
    },
  });

  // Start HTTP server
  const port = process.env.PORT || 3000;
  await app.listen(port);
  logger.log(`🌐 HTTP server listening on port ${port}`);

  // Start Kafka microservice
  logger.log('🔧 Starting Kafka microservice...');
  await app.startAllMicroservices();

  logger.log('✅ Example application fully started and ready!');
  logger.log(`📋 Health check: http://localhost:${port}/health`);
  logger.log(`📊 Metrics: http://localhost:${port}/metrics`);
  logger.log(`🔍 Debug: http://localhost:${port}/debug`);
  logger.log(`📨 Send test message: POST http://localhost:${port}/test/send`);
}

bootstrap().catch((error) => {
  console.error('❌ Failed to start application:', error);
  process.exit(1);
});