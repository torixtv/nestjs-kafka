import { INestApplication, Logger } from '@nestjs/common';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';

import { KAFKA_MODULE_OPTIONS } from './kafka.constants';
import { KafkaModuleOptions } from '../interfaces/kafka.interfaces';
import { KafkaBootstrapService } from '../services/kafka.bootstrap.service';

export interface RegisterKafkaMicroserviceOptions {
  /**
   * Override microservice options before connecting. Useful for testing or
   * programmatic tweaks (e.g. fromBeginning flag).
   */
  overrides?: Partial<MicroserviceOptions['options']>;
}

export async function registerKafkaMicroservice(
  app: INestApplication,
  options: RegisterKafkaMicroserviceOptions = {},
) {
  const moduleOptions = app.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS, {
    strict: false,
  });

  if (!moduleOptions) {
    throw new Error(
      'KafkaModule options not found. Ensure KafkaModule.forRoot/forRootAsync is imported before registering the Kafka microservice.',
    );
  }

  const microserviceOptions: MicroserviceOptions = {
    transport: Transport.KAFKA,
    options: {
      client: moduleOptions.client,
      consumer: moduleOptions.consumer,
      producer: moduleOptions.producer,
      ...(moduleOptions.subscriptions
        ? { subscribe: moduleOptions.subscriptions }
        : {}),
      ...(options.overrides || {}),
    },
  } as MicroserviceOptions;

  const logger = new Logger('KafkaBootstrap');
  const topics = moduleOptions.subscriptions?.topics || [];
  logger.log(
    `Registering Kafka microservice with ${topics.length} topic(s): ${topics.join(', ') || 'none'}`,
  );

  return app.connectMicroservice<MicroserviceOptions>(microserviceOptions);
}

export async function startKafkaMicroservice(app: INestApplication) {
  const moduleOptions = app.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS, {
    strict: false,
  });

  const logger = new Logger('KafkaBootstrap');

  try {
    await app.startAllMicroservices();

    // CRITICAL: In microservice environments, OnApplicationBootstrap might not trigger
    // Force initialization of Kafka services after microservice is running
    try {
      const bootstrapService = app.get<KafkaBootstrapService>(
        KafkaBootstrapService,
        {
          strict: false,
        },
      );

      if (bootstrapService) {
        logger.log(
          '🔧 Force-initializing Kafka services in microservice environment...',
        );
        await bootstrapService.forceInitialization();
        logger.log('✅ Kafka services initialization completed');
      } else {
        logger.warn(
          'KafkaBootstrapService not found - manual initialization may be required',
        );
      }
    } catch (initError) {
      logger.error('Failed to initialize Kafka services:', initError);
      // Don't throw - let the microservice continue running
    }
  } catch (error) {
    if (moduleOptions?.requireBroker === false) {
      logger.warn(
        `Kafka microservice failed to start (will continue without Kafka): ${error.message}`,
      );
      logger.debug(error.stack);
      const microservices = app.getMicroservices();
      await Promise.all(
        microservices.map(async (microservice) => {
          try {
            await microservice.close();
          } catch (closeError) {
            logger.debug(
              `Failed to close microservice cleanly: ${closeError.message}`,
            );
          }
        }),
      );
    } else {
      logger.error('Kafka microservice failed to start', error.stack);
      throw error;
    }
  }
}
