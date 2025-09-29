// Initialize tracing BEFORE any other imports
import './tracing';

import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { AppModule } from './app.module';

async function bootstrap() {
  const logger = new Logger('ExampleApp');

  logger.log('🚀 Starting Example Kafka Retry Application...');

  // Create NestJS application
  const app = await NestFactory.create(AppModule, {
    logger: ['error', 'warn', 'log', 'debug', 'verbose'],
  });

  // Enable graceful shutdown
  app.enableShutdownHooks();

  // Kafka consumption is now handled by the KafkaModule itself
  logger.log('📡 Kafka configuration handled by KafkaModule...');

  // Start HTTP server
  const port = process.env.PORT || 3000;
  await app.listen(port);
  logger.log(`🌐 HTTP server listening on port ${port}`);

  // Kafka consumer is started automatically by the bootstrap service
  logger.log('🔧 Kafka consumer will be started by bootstrap service...');

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