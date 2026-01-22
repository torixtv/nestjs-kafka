import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
  Inject,
} from '@nestjs/common';
import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';

import {
  KAFKAJS_INSTANCE,
  KAFKA_MODULE_OPTIONS,
} from '../core/kafka.constants';
import { KafkaModuleOptions } from '../interfaces/kafka.interfaces';
import { KafkaHandlerRegistry, RegisteredHandler } from './kafka.registry';
import { calculateRetryDelay } from '../utils/retry.utils';
import { KafkaRetryService } from './kafka.retry.service';
import { KafkaDlqService } from './kafka.dlq.service';
import { KafkaProducerService } from '../core/kafka.producer';

/**
 * Consumer state for health monitoring
 */
export enum ConsumerState {
  DISCONNECTED = 'disconnected',
  CONNECTING = 'connecting',
  CONNECTED = 'connected',        // Connected but no partitions yet
  REBALANCING = 'rebalancing',    // Partitions temporarily revoked
  ACTIVE = 'active',              // Has partitions, actively consuming
  STALE = 'stale',                // Was active, lost partitions for too long
}

/**
 * Partition assignment info
 */
export interface PartitionAssignment {
  topic: string;
  partition: number;
}

/**
 * Consumer group metadata from GROUP_JOIN event
 */
export interface ConsumerGroupInfo {
  groupId: string;
  memberId: string;
  isLeader: boolean;
}

/**
 * Consumer health state for health checks
 */
export interface ConsumerHealthState {
  state: ConsumerState;
  isHealthy: boolean;
  reason: string;
}

@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumerService.name);
  private consumer: Consumer;
  private isConnected = false;

  // State tracking for health monitoring
  private state: ConsumerState = ConsumerState.DISCONNECTED;
  private assignedPartitions: PartitionAssignment[] = [];
  private groupInfo: ConsumerGroupInfo | null = null;
  private readonly startupTime = Date.now();
  private lastPartitionAssignmentTime: number | null = null;
  private rebalanceStartTime: number | null = null;

  // Grace periods (configurable with tolerant defaults for production stability)
  private readonly startupGracePeriodMs: number;
  private readonly rebalanceGracePeriodMs: number;
  private readonly staleThresholdMs: number;

  // Default grace period values
  private static readonly DEFAULT_STARTUP_GRACE_PERIOD_MS = 180000;      // 3 min after startup
  private static readonly DEFAULT_REBALANCE_GRACE_PERIOD_MS = 120000;    // 2 min during rebalance
  private static readonly DEFAULT_STALE_THRESHOLD_MS = 600000;           // 10 min without partitions = stale

  constructor(
    @Inject(KAFKAJS_INSTANCE) private readonly kafka: Kafka,
    @Inject(KAFKA_MODULE_OPTIONS) private readonly options: KafkaModuleOptions,
    private readonly handlerRegistry: KafkaHandlerRegistry,
    private readonly retryService: KafkaRetryService,
    private readonly dlqService: KafkaDlqService,
    private readonly producer: KafkaProducerService,
  ) {
    if (!this.options.consumer) {
      throw new Error('Consumer configuration is required');
    }

    // Initialize configurable grace periods with defaults
    this.startupGracePeriodMs = this.options.health?.startupGracePeriodMs
      ?? KafkaConsumerService.DEFAULT_STARTUP_GRACE_PERIOD_MS;
    this.rebalanceGracePeriodMs = this.options.health?.rebalanceGracePeriodMs
      ?? KafkaConsumerService.DEFAULT_REBALANCE_GRACE_PERIOD_MS;
    this.staleThresholdMs = this.options.health?.staleThresholdMs
      ?? KafkaConsumerService.DEFAULT_STALE_THRESHOLD_MS;

    this.consumer = this.kafka.consumer(this.options.consumer);
    this.setupConsumerEventListeners();
  }

  /**
   * Set up KafkaJS consumer event listeners for state tracking.
   * These events allow us to track the actual consumer state beyond just isConnected.
   */
  private setupConsumerEventListeners(): void {
    // Safety check: consumer.events may not exist in test environments
    if (!this.consumer?.events) {
      this.logger.warn('Consumer events not available - state tracking will be limited');
      return;
    }

    const { CONNECT, DISCONNECT, CRASH, GROUP_JOIN, REBALANCING } = this.consumer.events;

    this.consumer.on(CONNECT, () => {
      this.logger.log('Consumer connected to broker');
      this.isConnected = true;
      this.state = ConsumerState.CONNECTED;
    });

    this.consumer.on(DISCONNECT, () => {
      this.logger.warn('Consumer disconnected from broker');
      this.isConnected = false;
      this.state = ConsumerState.DISCONNECTED;
      this.assignedPartitions = [];
    });

    this.consumer.on(CRASH, ({ payload }) => {
      this.logger.error('Consumer crashed', { error: payload.error, restart: payload.restart });
      this.isConnected = false;
      this.state = ConsumerState.DISCONNECTED;
      this.assignedPartitions = [];
    });

    this.consumer.on(GROUP_JOIN, ({ payload }) => {
      this.logger.log('Consumer joined group', {
        groupId: payload.groupId,
        memberId: payload.memberId,
        isLeader: payload.isLeader,
        memberAssignment: payload.memberAssignment,
      });
      this.state = ConsumerState.ACTIVE;
      this.lastPartitionAssignmentTime = Date.now();
      this.rebalanceStartTime = null;
      this.groupInfo = {
        groupId: payload.groupId,
        memberId: payload.memberId,
        isLeader: payload.isLeader,
      };
      this.assignedPartitions = this.extractPartitions(payload.memberAssignment);
    });

    this.consumer.on(REBALANCING, ({ payload }) => {
      this.logger.warn('Consumer group rebalancing', { groupId: payload.groupId, memberId: payload.memberId });
      this.state = ConsumerState.REBALANCING;
      this.rebalanceStartTime = Date.now();
      // Note: We don't clear assignedPartitions here to preserve last known state for logging
    });
  }

  /**
   * Extract partition assignments from KafkaJS memberAssignment format.
   */
  private extractPartitions(memberAssignment: Record<string, number[]>): PartitionAssignment[] {
    const result: PartitionAssignment[] = [];
    for (const [topic, partitions] of Object.entries(memberAssignment)) {
      // Defensive check in case partitions is null/undefined
      for (const partition of (partitions ?? [])) {
        result.push({ topic, partition });
      }
    }
    return result;
  }

  /**
   * Get currently assigned partitions.
   */
  getAssignedPartitions(): PartitionAssignment[] {
    return [...this.assignedPartitions];
  }

  /**
   * Get consumer group information (groupId, memberId, isLeader).
   * Returns null if the consumer has not joined a group yet.
   */
  getGroupInfo(): ConsumerGroupInfo | null {
    return this.groupInfo ? { ...this.groupInfo } : null;
  }

  /**
   * Check if consumer has any partitions assigned.
   */
  hasPartitionsAssigned(): boolean {
    return this.assignedPartitions.length > 0;
  }

  /**
   * Get health state with smart grace period handling.
   * This method is designed to prevent unnecessary pod restarts during:
   * - Startup (before partitions are assigned)
   * - Normal rebalancing (partitions temporarily unassigned)
   *
   * Only marks unhealthy when consumer is truly stale (no partitions for extended period).
   *
   * Note: This method is read-only and does not mutate internal state.
   * It computes the effective state based on current conditions.
   */
  getHealthState(): ConsumerHealthState {
    const now = Date.now();
    const timeSinceStart = now - this.startupTime;

    // During startup grace period, always report healthy
    if (timeSinceStart < this.startupGracePeriodMs) {
      return {
        state: this.state,
        isHealthy: true,
        reason: `Within startup grace period (${Math.round(timeSinceStart / 1000)}s / ${this.startupGracePeriodMs / 1000}s)`,
      };
    }

    // During rebalance grace period, stay healthy
    if (this.state === ConsumerState.REBALANCING && this.rebalanceStartTime) {
      const rebalanceDuration = now - this.rebalanceStartTime;
      if (rebalanceDuration < this.rebalanceGracePeriodMs) {
        return {
          state: this.state,
          isHealthy: true,
          reason: `Rebalancing (${Math.round(rebalanceDuration / 1000)}s / ${this.rebalanceGracePeriodMs / 1000}s max)`,
        };
      }
      // Rebalance taking too long - compute as stale without mutating
      return {
        state: ConsumerState.STALE,
        isHealthy: false,
        reason: 'Consumer stale - rebalance exceeded grace period',
      };
    }

    // Check for stale state (had partitions before, lost them, didn't get them back)
    // Use local variable to compute effective state without mutation
    let computedState = this.state;
    if (
      (this.state === ConsumerState.CONNECTED || this.state === ConsumerState.REBALANCING) &&
      this.lastPartitionAssignmentTime
    ) {
      const timeSinceLastAssignment = now - this.lastPartitionAssignmentTime;
      if (timeSinceLastAssignment > this.staleThresholdMs) {
        computedState = ConsumerState.STALE;
      }
    }

    // Final health determination based on computed state
    switch (computedState) {
      case ConsumerState.ACTIVE:
        return {
          state: computedState,
          isHealthy: true,
          reason: `Active with ${this.assignedPartitions.length} partition(s)`,
        };
      case ConsumerState.STALE:
        return {
          state: computedState,
          isHealthy: false,
          reason: 'Consumer stale - no partitions assigned for extended period',
        };
      case ConsumerState.DISCONNECTED:
        return {
          state: computedState,
          isHealthy: false,
          reason: 'Consumer disconnected from broker',
        };
      case ConsumerState.CONNECTED:
        return {
          state: computedState,
          isHealthy: true,
          reason: 'Connected, waiting for partition assignment',
        };
      case ConsumerState.CONNECTING:
        return {
          state: computedState,
          isHealthy: true,
          reason: 'Connecting to broker',
        };
      default:
        return {
          state: computedState,
          isHealthy: true,
          reason: `State: ${computedState}`,
        };
    }
  }

  /**
   * Get current consumer state (for external monitoring).
   */
  getState(): ConsumerState {
    return this.state;
  }

  async onModuleInit(): Promise<void> {
    // Don't auto-start consumer - let bootstrap service handle this
    this.logger.log('Consumer service initialized');
  }

  async onModuleDestroy(): Promise<void> {
    if (this.isConnected) {
      await this.disconnect();
    }
  }

  async connect(): Promise<void> {
    if (this.isConnected) {
      return;
    }

    try {
      // Set CONNECTING state before initiating connection
      this.state = ConsumerState.CONNECTING;
      this.logger.log('Connecting Kafka consumer...');

      await this.consumer.connect();
      // Note: isConnected and state are updated by the CONNECT event listener
      // but we set them here as well for environments where events may not fire
      this.isConnected = true;
      if (this.state === ConsumerState.CONNECTING) {
        this.state = ConsumerState.CONNECTED;
      }
      this.logger.log('Kafka consumer connected successfully');
    } catch (error) {
      this.state = ConsumerState.DISCONNECTED;
      this.logger.error('Failed to connect Kafka consumer', error.stack);
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    if (!this.isConnected) {
      return;
    }

    try {
      await this.consumer.disconnect();
      this.isConnected = false;
      this.logger.log('Kafka consumer disconnected');
    } catch (error) {
      this.logger.error('Failed to disconnect Kafka consumer', error.stack);
    }
  }

  async subscribe(): Promise<void> {
    if (!this.isConnected) {
      await this.connect();
    }

    if (!this.options.subscriptions) {
      this.logger.warn('No subscriptions configured');
      return;
    }

    const { topics, fromBeginning } = this.options.subscriptions;

    if (!topics || topics.length === 0) {
      this.logger.warn('No topics configured for subscription');
      return;
    }

    for (const topic of topics) {
      await this.consumer.subscribe({
        topic,
        fromBeginning: fromBeginning ?? false,
      });
      this.logger.log(`Subscribed to topic: ${topic}`);
    }
  }

  async startConsumer(): Promise<void> {
    await this.consumer.run({
      eachMessage: async (payload: EachMessagePayload) => {
        await this.handleMessage(payload);
      },
    });
    this.logger.log('Consumer started and processing messages');
  }

  private async handleMessage(payload: EachMessagePayload): Promise<void> {
    const { topic, partition, message } = payload;

    try {
      const handler = this.handlerRegistry.getHandlerByTopic(topic);

      if (!handler) {
        this.logger.warn(`No handler found for topic: ${topic}`);
        return;
      }

      // Parse message
      const parsedMessage = {
        key: message.key?.toString(),
        value: this.parseMessageValue(message.value),
        headers: this.parseHeaders(message.headers),
        timestamp: message.timestamp,
        partition,
        offset: message.offset,
      };

      // Get retry/DLQ configuration from handler metadata
      const retryOptions = handler.metadata?.options?.retry;
      const dlqOptions = handler.metadata?.options?.dlq;

      try {
        // Execute handler - pass only the value to match handler expectations
        await handler.method.call(handler.instance, parsedMessage.value);
        this.logger.debug(`Successfully processed message from ${topic}:${partition}:${message.offset}`);
      } catch (error) {
        // Handle error with retry logic
        await this.handleMessageError(
          error,
          message,
          topic,
          partition,
          handler,
          retryOptions,
          dlqOptions,
        );
      }
    } catch (error) {
      this.logger.error(
        `Failed to process message from ${topic}:${partition}:${message.offset}`,
        error.stack
      );
      // Don't throw - message is handled
    }
  }

  private async handleMessageError(
    error: Error,
    message: any,
    topic: string,
    partition: number,
    handler: RegisteredHandler,
    retryOptions: any,
    dlqOptions: any,
  ): Promise<void> {
    const retryCount = this.getRetryCountFromHeaders(message.headers);

    this.logger.warn(`Message processing failed`, {
      topic,
      partition,
      offset: message.offset,
      retryCount,
      error: error.message,
    });

    if (retryOptions?.enabled) {
      if (retryCount < retryOptions.attempts) {
        // Calculate delay
        const delay = calculateRetryDelay({
          attempt: retryCount + 1,
          baseDelay: retryOptions.baseDelay,
          maxDelay: retryOptions.maxDelay,
          backoff: retryOptions.backoff,
          jitter: true,
        });

        // Schedule retry
        await this.scheduleRetry(
          message,
          topic,
          partition,
          handler.handlerId,
          retryCount + 1,
          delay,
        );

        this.logger.log(`Scheduled retry ${retryCount + 1} with ${delay}ms delay`);
      } else if (dlqOptions?.enabled) {
        // Max retries exceeded - send to DLQ
        await this.dlqService.storeToDlq(
          message,
          error,
          topic,
          handler.handlerId,
          retryCount,
          this.getCorrelationId(message.headers),
        );

        this.logger.error(`Max retries exceeded, sent to DLQ`, {
          topic,
          handlerId: handler.handlerId,
          retryCount,
        });
      }
    } else {
      // No retry configured, just log
      this.logger.error(`Handler failed without retry`, error.stack);
    }
  }

  private async scheduleRetry(
    message: any,
    originalTopic: string,
    partition: number,
    handlerId: string,
    retryCount: number,
    delayMs: number,
  ): Promise<void> {
    const retryTopicName = this.retryService.getRetryTopicName();
    const processAfter = Date.now() + delayMs;

    const retryHeaders: Record<string, string> = {
      'x-original-topic': originalTopic,
      'x-handler-id': handlerId,
      'x-retry-count': retryCount.toString(),
      'x-process-after': processAfter.toString(),
      'x-original-partition': partition.toString(),
      'x-original-offset': message.offset || '0',
      'x-original-timestamp': message.timestamp || Date.now().toString(),
    };

    // Preserve correlation ID if exists
    const correlationId = this.getCorrelationId(message.headers);
    if (correlationId) {
      retryHeaders['x-correlation-id'] = correlationId;
    }

    // Copy original headers (excluding retry headers)
    if (message.headers) {
      for (const [key, value] of Object.entries(message.headers)) {
        if (!key.startsWith('x-retry-') &&
            !key.startsWith('x-original-') &&
            !key.startsWith('x-handler-')) {
          retryHeaders[key] = value ? value.toString() : '';
        }
      }
    }

    await this.producer.send(retryTopicName, {
      key: message.key,
      value: message.value,
      headers: retryHeaders,
    });
  }

  private parseMessageValue(value: Buffer | null): any {
    if (!value) return null;

    try {
      return JSON.parse(value.toString());
    } catch {
      return value.toString();
    }
  }

  private parseHeaders(headers: any): Record<string, string> {
    if (!headers) return {};

    const parsed: Record<string, string> = {};
    for (const [key, value] of Object.entries(headers)) {
      if (Buffer.isBuffer(value)) {
        parsed[key] = value.toString();
      } else {
        parsed[key] = String(value);
      }
    }
    return parsed;
  }

  private getRetryCountFromHeaders(headers: any): number {
    if (!headers || !headers['x-retry-count']) {
      return 0;
    }
    const value = headers['x-retry-count'];
    const count = parseInt(Buffer.isBuffer(value) ? value.toString() : value, 10);
    return isNaN(count) ? 0 : count;
  }

  private getCorrelationId(headers: any): string | undefined {
    if (!headers || !headers['x-correlation-id']) {
      return undefined;
    }
    const value = headers['x-correlation-id'];
    return Buffer.isBuffer(value) ? value.toString() : value;
  }
}