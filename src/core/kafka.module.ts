import { DynamicModule, Global, Module, Provider } from '@nestjs/common';
import { DiscoveryModule, Reflector } from '@nestjs/core';
import { TerminusModule } from '@nestjs/terminus';
import { Kafka } from 'kafkajs';

import { KAFKA_MODULE_OPTIONS, KAFKAJS_INSTANCE } from './kafka.constants';
import {
  KafkaModuleAsyncOptions,
  KafkaModuleOptions,
  KafkaModuleOptionsFactory,
} from '../interfaces/kafka.interfaces';
import { KafkaProducerService } from './kafka.producer';
import { RetryInterceptor } from '../interceptors/retry.interceptor';
import { KafkaHandlerRegistry } from '../services/kafka.registry';
import { KafkaRetryService } from '../services/kafka.retry.service';
import { KafkaDlqService } from '../services/kafka.dlq.service';
import { KafkaConsumerService } from '../services/kafka.consumer.service';
import { KafkaBootstrapService } from '../services/kafka.bootstrap.service';
import { KafkaMonitoringController } from '../monitoring/kafka-monitoring.controller';
import { mergeWithEnvironmentConfig } from '../utils/config.utils';
import { KafkaHealthIndicator } from '../health';

@Global()
@Module({
  imports: [DiscoveryModule, TerminusModule],
})
export class KafkaModule {
  static forRoot(options: KafkaModuleOptions = {}): DynamicModule {
    const mergedOptions = this.mergeWithDefaults(options);
    const providers = this.createProviders(mergedOptions);
    const imports = [DiscoveryModule, TerminusModule];
    const controllers = this.shouldEnableMonitoring(mergedOptions)
      ? [KafkaMonitoringController]
      : [];
    return this.createModule(providers, imports, controllers);
  }

  static forRootAsync(options: KafkaModuleAsyncOptions): DynamicModule {
    const asyncProviders = this.createAsyncProviders(options);
    const providers = [...asyncProviders, ...this.createSharedProviders()];
    const imports = [DiscoveryModule, TerminusModule, ...(options.imports || [])];
    // Note: For async, we can't determine monitoring status at build time
    // So we always include the controller, but it could be made conditional via injection
    return this.createModule(providers, imports, [KafkaMonitoringController]);
  }

  private static shouldEnableMonitoring(options: KafkaModuleOptions): boolean {
    // Monitoring is enabled by default unless explicitly disabled
    return options.monitoring?.enabled !== false;
  }

  private static createModule(
    providers: Provider[],
    imports: any[] = [],
    controllers: any[] = [],
  ): DynamicModule {
    return {
      module: KafkaModule,
      imports,
      controllers,
      providers,
      exports: [
        // Core services
        KafkaProducerService,
        KafkaConsumerService,
        KafkaBootstrapService,
        RetryInterceptor,
        KafkaHandlerRegistry,
        KafkaRetryService,
        KafkaDlqService,
        KafkaHealthIndicator,
        // Options and instance
        KAFKA_MODULE_OPTIONS,
        KAFKAJS_INSTANCE,
      ],
    };
  }

  private static createProviders(options: KafkaModuleOptions): Provider[] {
    return [
      {
        provide: KAFKA_MODULE_OPTIONS,
        useValue: this.mergeWithDefaults(options),
      },
      ...this.createSharedProviders(),
    ];
  }

  private static createAsyncProviders(
    options: KafkaModuleAsyncOptions,
  ): Provider[] {
    const providers: Provider[] = [];

    if (options.useFactory) {
      providers.push({
        provide: KAFKA_MODULE_OPTIONS,
        useFactory: async (...args: any[]) => {
          const userOptions = await options.useFactory!(...args);
          return this.mergeWithDefaults(userOptions);
        },
        inject: options.inject || [],
      });
    }

    if (options.useClass) {
      providers.push(
        {
          provide: options.useClass,
          useClass: options.useClass,
        },
        {
          provide: KAFKA_MODULE_OPTIONS,
          useFactory: async (factory: KafkaModuleOptionsFactory) => {
            const userOptions = await factory.createKafkaModuleOptions();
            return this.mergeWithDefaults(userOptions);
          },
          inject: [options.useClass],
        },
      );
    }

    if (options.useExisting) {
      providers.push({
        provide: KAFKA_MODULE_OPTIONS,
        useFactory: async (factory: KafkaModuleOptionsFactory) => {
          const userOptions = await factory.createKafkaModuleOptions();
          return this.mergeWithDefaults(userOptions);
        },
        inject: [options.useExisting],
      });
    }

    return providers;
  }

  private static createSharedProviders(): Provider[] {
    return [
      {
        provide: KAFKAJS_INSTANCE,
        inject: [KAFKA_MODULE_OPTIONS],
        useFactory: (options: KafkaModuleOptions) => {
          if (!options.client) {
            throw new Error('Kafka client configuration is required');
          }

          // Merge with environment config and apply smart defaults
          // This automatically:
          // - Reads SASL config from KAFKA_SASL_* env vars
          // - Reads SSL config from KAFKA_SSL_ENABLED env var
          // - Enables SSL automatically when SASL is configured
          // - Normalizes SASL mechanism to lowercase
          const clientConfig = mergeWithEnvironmentConfig(options.client);

          return new Kafka(clientConfig);
        },
      },
      Reflector,
      // Core services
      KafkaHandlerRegistry,
      KafkaRetryService,
      KafkaDlqService,
      KafkaProducerService,
      KafkaConsumerService,
      KafkaBootstrapService,
      RetryInterceptor,
      KafkaHealthIndicator,
    ];
  }

  private static mergeWithDefaults(
    userOptions: KafkaModuleOptions = {},
  ): KafkaModuleOptions {
    const defaults: KafkaModuleOptions = {
      client: {
        clientId: 'kafka-client',
        brokers: ['localhost:9092'],
      },
      consumer: {
        groupId: 'kafka-consumer-group',
      },
      producer: {},
      subscriptions: {
        topics: [],
        fromBeginning: false,
      },
      retry: {
        enabled: false,
        attempts: 3,
        backoff: 'exponential',
        maxDelay: 30000,
        baseDelay: 1000,
        topicPartitions: 3,
        topicReplicationFactor: 1,
        topicRetentionMs: 24 * 60 * 60 * 1000, // 24 hours
        // Don't set topicSegmentMs default - let Kafka use its default
      },
      dlq: {
        enabled: false,
        topicPartitions: 3,
        topicReplicationFactor: 1,
        topicRetentionMs: 7 * 24 * 60 * 60 * 1000, // 7 days
        // Don't set topicSegmentMs default - let Kafka use its default
        reprocessingOptions: {
          batchSize: 100,
          timeoutMs: 30000,
          stopOnError: false,
        },
      },
      monitoring: {
        enabled: true, // Enabled by default
        path: 'kafka',
      },
    };

    const mergedClient = userOptions.client
      ? { ...defaults.client, ...userOptions.client }
      : defaults.client;

    const mergedConsumer = userOptions.consumer
      ? { ...defaults.consumer, ...userOptions.consumer }
      : defaults.consumer;

    const mergedProducer = userOptions.producer
      ? { ...defaults.producer, ...userOptions.producer }
      : defaults.producer;

    const mergedSubscriptions = userOptions.subscriptions
      ? { ...defaults.subscriptions, ...userOptions.subscriptions }
      : defaults.subscriptions;

    // Normalize retry backoff to lowercase
    const mergedRetry = userOptions.retry
      ? { ...defaults.retry, ...userOptions.retry }
      : defaults.retry;

    if (mergedRetry?.backoff) {
      mergedRetry.backoff = mergedRetry.backoff.toLowerCase() as 'exponential' | 'linear';
    }

    return {
      ...defaults,
      ...userOptions,
      client: mergedClient,
      consumer: mergedConsumer,
      producer: mergedProducer,
      subscriptions: mergedSubscriptions,
      retry: mergedRetry,
      dlq: userOptions.dlq
        ? { ...defaults.dlq, ...userOptions.dlq }
        : defaults.dlq,
      monitoring: userOptions.monitoring
        ? { ...defaults.monitoring, ...userOptions.monitoring }
        : defaults.monitoring,
    };
  }
}
