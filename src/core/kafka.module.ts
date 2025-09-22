import { DynamicModule, Global, Module, Provider } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { Kafka } from 'kafkajs';

import {
  KAFKA_MODULE_OPTIONS,
  KAFKAJS_INSTANCE,
  KAFKA_PLUGINS,
} from './kafka.constants';
import {
  KafkaModuleAsyncOptions,
  KafkaModuleOptions,
  KafkaModuleOptionsFactory,
  KafkaPlugin,
} from '../interfaces/kafka.interfaces';
import { KafkaProducerService } from './kafka.producer';
import { RetryInterceptor } from '../interceptors/retry.interceptor';
import { KafkaHandlerRegistry } from '../services/kafka.registry';
import { KafkaRetryManager } from '../services/kafka.retry-manager';
import { KafkaRetryConsumer } from '../services/kafka.retry-consumer';
import { KafkaBootstrapService } from '../services/kafka.bootstrap.service';

@Global()
@Module({
  imports: [DiscoveryModule],
})
export class KafkaModule {
  static forRoot(options: KafkaModuleOptions = {}): DynamicModule {
    const providers = this.createProviders(options);
    const imports = [DiscoveryModule];
    return this.createModule(providers, imports);
  }

  static forRootAsync(options: KafkaModuleAsyncOptions): DynamicModule {
    const asyncProviders = this.createAsyncProviders(options);
    const providers = [...asyncProviders, ...this.createSharedProviders()];
    const imports = [DiscoveryModule, ...(options.imports || [])];
    return this.createModule(providers, imports);
  }

  private static createModule(
    providers: Provider[],
    imports: any[] = [],
  ): DynamicModule {
    return {
      module: KafkaModule,
      imports,
      providers,
      exports: [
        KafkaProducerService,
        RetryInterceptor,
        KafkaHandlerRegistry,
        KafkaRetryManager,
        KafkaRetryConsumer,
        KafkaBootstrapService,
        KAFKA_MODULE_OPTIONS,
        KAFKAJS_INSTANCE,
        KAFKA_PLUGINS,
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
          return new Kafka(options.client);
        },
      },
      {
        provide: KAFKA_PLUGINS,
        inject: [KAFKA_MODULE_OPTIONS],
        useFactory: (options: KafkaModuleOptions) => {
          return options.plugins || [];
        },
      },
      KafkaHandlerRegistry,
      KafkaRetryManager,
      KafkaRetryConsumer,
      KafkaProducerService,
      RetryInterceptor,
      KafkaBootstrapService,
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
      requireBroker: true,
      retry: {
        enabled: false,
        attempts: 3,
        backoff: 'exponential',
        maxDelay: 30000,
        baseDelay: 1000,
        topicPartitions: 3,
        topicReplicationFactor: 1,
        topicRetentionMs: 24 * 60 * 60 * 1000, // 24 hours
        topicSegmentMs: 60 * 60 * 1000, // 1 hour
      },
      dlq: {
        enabled: false,
      },
      plugins: [],
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

    return {
      ...defaults,
      ...userOptions,
      client: mergedClient,
      consumer: mergedConsumer,
      producer: mergedProducer,
      retry: userOptions.retry
        ? { ...defaults.retry, ...userOptions.retry }
        : defaults.retry,
      dlq: userOptions.dlq
        ? { ...defaults.dlq, ...userOptions.dlq }
        : defaults.dlq,
      plugins: [...(defaults.plugins || []), ...(userOptions.plugins || [])],
    };
  }
}
