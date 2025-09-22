import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { KafkaHandlerRegistry } from './kafka.registry';
import { KafkaRetryManager } from './kafka.retry-manager';
import { KafkaRetryConsumer } from './kafka.retry-consumer';

/**
 * Centralized bootstrap service for Kafka components
 *
 * Ensures proper initialization order in both regular and microservice environments:
 * 1. Handler Registry discovers and registers event handlers
 * 2. Retry Manager creates retry topics
 * 3. Retry Consumer starts after handlers are available
 */
@Injectable()
export class KafkaBootstrapService implements OnApplicationBootstrap {
  private readonly logger = new Logger(KafkaBootstrapService.name);
  private isInitialized = false;
  private initializationPromise: Promise<void> | null = null;

  constructor(
    private readonly handlerRegistry: KafkaHandlerRegistry,
    private readonly retryManager: KafkaRetryManager,
    private readonly retryConsumer: KafkaRetryConsumer,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    if (this.isInitialized || this.initializationPromise) {
      await this.initializationPromise;
      return;
    }

    this.initializationPromise = this.initializeKafkaServices();
    await this.initializationPromise;
  }

  /**
   * Force initialization - useful for microservice test environments
   * where OnApplicationBootstrap might not be triggered automatically
   */
  async forceInitialization(): Promise<void> {
    if (this.isInitialized) {
      this.logger.log('Kafka services already initialized');
      return;
    }

    this.logger.log('Force-initializing Kafka services...');
    await this.onApplicationBootstrap();
  }

  private async initializeKafkaServices(): Promise<void> {
    try {
      this.logger.log('Starting Kafka services initialization...');

      // Step 1: Initialize handler registry to discover all event handlers
      this.logger.log('Initializing handler registry...');
      await this.handlerRegistry.onModuleInit();

      // Step 2: Initialize retry manager to ensure retry topics exist
      this.logger.log('Initializing retry manager...');
      await this.retryManager.onModuleInit();

      // Step 3: Initialize retry consumer (depends on handler registry being ready)
      this.logger.log('Initializing retry consumer...');
      await this.retryConsumer.onModuleInit();

      this.isInitialized = true;
      this.logger.log('✅ All Kafka services initialized successfully');

      // Log service states for debugging
      this.logServiceStates();
    } catch (error) {
      this.logger.error('❌ Failed to initialize Kafka services:', error);
      throw error;
    }
  }

  private logServiceStates(): void {
    try {
      const handlerCount = this.handlerRegistry.getAllHandlers().length;
      const retryConsumerRunning = this.retryConsumer.isRetryConsumerRunning();
      const retryTopicName = this.retryManager.getRetryTopicName();

      this.logger.log(`Service states:
        - Handlers registered: ${handlerCount}
        - Retry consumer running: ${retryConsumerRunning}
        - Retry topic: ${retryTopicName}`);

      // Log all registered handlers for debugging
      const handlers = this.handlerRegistry.getAllHandlers();
      handlers.forEach((handler) => {
        this.logger.debug(
          `Handler: ${handler.handlerId} -> pattern: ${handler.pattern}`,
        );
      });
    } catch (error) {
      this.logger.warn('Could not log service states:', error.message);
    }
  }

  /**
   * Get initialization status
   */
  isKafkaInitialized(): boolean {
    return this.isInitialized;
  }

  /**
   * Wait for initialization to complete (useful for tests)
   */
  async waitForInitialization(timeoutMs: number = 10000): Promise<void> {
    const startTime = Date.now();

    while (!this.isInitialized && Date.now() - startTime < timeoutMs) {
      if (this.initializationPromise) {
        await this.initializationPromise;
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    if (!this.isInitialized) {
      throw new Error(`Kafka initialization timeout after ${timeoutMs}ms`);
    }
  }

  /**
   * Get service status for monitoring/debugging
   */
  getServiceStatus() {
    return {
      initialized: this.isInitialized,
      handlerCount: this.isInitialized
        ? this.handlerRegistry.getAllHandlers().length
        : 0,
      retryConsumerRunning: this.isInitialized
        ? this.retryConsumer.isRetryConsumerRunning()
        : false,
      retryTopicName: this.isInitialized
        ? this.retryManager.getRetryTopicName()
        : null,
    };
  }
}
