import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  Inject,
} from '@nestjs/common';

import { KAFKA_MODULE_OPTIONS } from '../core/kafka.constants';
import { KafkaModuleOptions } from '../interfaces/kafka.interfaces';
import { KafkaHandlerRegistry } from './kafka.registry';
import { KafkaRetryService } from './kafka.retry.service';
import { KafkaDlqService } from './kafka.dlq.service';
import { KafkaConsumerService } from './kafka.consumer.service';

@Injectable()
export class KafkaBootstrapService implements OnApplicationBootstrap {
  private readonly logger = new Logger(KafkaBootstrapService.name);
  private isInitialized = false;

  constructor(
    @Inject(KAFKA_MODULE_OPTIONS) private readonly options: KafkaModuleOptions,
    private readonly handlerRegistry: KafkaHandlerRegistry,
    private readonly retryService: KafkaRetryService,
    private readonly dlqService: KafkaDlqService,
    private readonly consumerService: KafkaConsumerService,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    if (this.isInitialized) {
      return;
    }

    this.logger.log('🚀 Starting Kafka bootstrap sequence...');

    try {
      // Step 1: Initialize handler registry (discover @EventHandler decorators)
      await this.initializeHandlerRegistry();

      // Step 2: Initialize retry system if enabled
      if (this.options.retry?.enabled) {
        await this.initializeRetrySystem();
      }

      // Step 3: Initialize DLQ system if enabled
      if (this.options.dlq?.enabled) {
        await this.initializeDlqSystem();
      }

      // Step 4: Start main consumer
      await this.initializeMainConsumer();

      this.isInitialized = true;
      this.logger.log('✅ Kafka bootstrap completed successfully');
    } catch (error) {
      this.logger.error('❌ Kafka bootstrap failed', error);
      throw error;
    }
  }

  private async initializeHandlerRegistry(): Promise<void> {
    this.logger.log('📋 Initializing handler registry...');
    await this.handlerRegistry.discoverHandlers();
    const handlerCount = this.handlerRegistry.getHandlerCount();
    this.logger.log(`📋 Handler registry initialized with ${handlerCount} handlers`);
  }

  private async initializeRetrySystem(): Promise<void> {
    this.logger.log('🔄 Initializing retry system...');

    if (this.options.retry?.enabled) {
      this.logger.log('Retry is enabled, starting retry consumer...');
      await this.retryService.startRetryConsumer();
    } else {
      this.logger.log('Retry is disabled, retry service will remain inactive');
    }

    this.logger.log('🔄 Retry system initialized');
  }

  private async initializeDlqSystem(): Promise<void> {
    this.logger.log('💀 Initializing DLQ system...');
    await this.dlqService.onModuleInit();
    this.logger.log('💀 DLQ system initialized');
  }

  private async initializeMainConsumer(): Promise<void> {
    this.logger.log('📡 Initializing main consumer...');

    // Connect and subscribe to topics
    await this.consumerService.connect();
    await this.consumerService.subscribe();

    // Start consuming messages
    await this.consumerService.startConsumer();

    this.logger.log('📡 Main consumer initialized and running');
  }

  /**
   * Force initialization for testing purposes
   */
  async forceInitialization(): Promise<void> {
    this.isInitialized = false;
    await this.onApplicationBootstrap();
  }
}