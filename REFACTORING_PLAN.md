# NestJS Kafka Events Package - Production Refactoring Plan

## Executive Summary

This document outlines a comprehensive refactoring plan to transform the current Kafka events package into a production-ready, maintainable system. The refactoring addresses critical resource management issues, **consumer group mismanagement that prevents horizontal scaling**, architectural violations, and implements proper NestJS patterns while maintaining direct KafkaJS control.

## ⚠️ CRITICAL ISSUE: Consumer Group Management

### The Problem
The current implementation has **production-blocking consumer group issues**:

1. **Dynamic Consumer Groups with Timestamps**: Code like `dlq-reprocess-${Date.now()}` creates a new consumer group every time, which:
   - **Breaks horizontal scaling** - Multiple instances can't share work
   - **Causes message duplication** - Each instance processes all messages
   - **Prevents proper offset management** - Offsets are lost between restarts
   - **Creates resource leaks** - Abandoned consumer groups accumulate

2. **Too Many Consumer Groups**: Currently using 6+ different consumer groups where 2-3 would suffice:
   - Increases rebalancing overhead
   - Complicates monitoring and debugging
   - Creates unnecessary complexity

3. **No Rebalancing Strategy**: Missing configuration for:
   - Partition assignment strategies
   - Rebalancing listeners
   - Graceful shutdown handling

### The Solution: Three Consumer Group Strategy

For production deployment with horizontal scaling, we need exactly **THREE consumer groups**:

```typescript
// Consumer Group Architecture
const CONSUMER_GROUPS = {
  // 1. Main consumer group - for primary message processing
  MAIN: '${SERVICE_NAME}-main',

  // 2. Retry consumer group - for processing retry topics
  RETRY: '${SERVICE_NAME}-retry',

  // 3. Admin consumer group - for one-off operations (DLQ reprocessing, etc.)
  ADMIN: '${SERVICE_NAME}-admin'
};
```

**Why This Works:**
- **MAIN**: All instances share this group for horizontal scaling of primary processing
- **RETRY**: Separate group prevents retry messages from blocking main processing
- **ADMIN**: For manual operations that shouldn't interfere with automatic processing

## Current State Analysis

### Critical Issues Identified

#### 1. Resource Management Problems
- **Admin Connection Abuse**: Services create and destroy Admin connections repeatedly, causing resource exhaustion
- **Connection Leaks**: Error scenarios may leave orphaned connections
- **No Connection Pooling**: Missing centralized connection management leads to inefficient resource usage
- **Uncoordinated Consumer Lifecycle**: Consumers are started in services without proper coordination

#### 2. Architectural Violations
- **Single Responsibility Principle Violations**: Services handle multiple concerns (e.g., retry service manages consumers AND retry logic)
- **Unnecessary Complexity**: Over-engineered DLQ handling (treating it as storage rather than just another topic)
- **Service Interdependencies**: Circular dependencies between retry and DLQ services
- **Missing Centralization**: No unified consumer or connection management

#### 3. Production Readiness Gaps
- **No Circuit Breakers**: Missing resilience patterns for failing consumers
- **Limited Health Checks**: Cannot determine system health easily
- **No Backpressure Handling**: Could overwhelm downstream systems
- **Missing Observability**: No metrics or distributed tracing support

## Refactoring Strategy

### Core Principle: Simplify and Centralize

The refactoring follows these key principles:
1. **Single Responsibility**: Each service has one clear purpose
2. **Resource Centralization**: All Kafka resources managed centrally
3. **Proper NestJS Patterns**: Leverage DI, lifecycle hooks, and decorators correctly
4. **Direct KafkaJS Control**: Maintain granular control without @nestjs/microservices

---

## Phase 1: Core Infrastructure (Week 1 - CRITICAL)

### 1.1 Create Centralized Connection Manager

**File**: `src/core/kafka-connection-manager.service.ts`

**Purpose**: Centralize all Kafka admin operations and connection management

**Implementation**:
```typescript
@Injectable()
export class KafkaConnectionManager implements OnModuleInit, OnModuleDestroy {
  private kafka: Kafka;
  private admin: Admin;
  private producer: Producer;
  private consumers: Map<string, Consumer> = new Map();
  private adminConnected = false;
  private producerConnected = false;

  constructor(
    @Inject('KAFKA_OPTIONS') private options: KafkaModuleOptions,
    private readonly logger: Logger,
  ) {
    this.kafka = new Kafka(this.options.client);
    this.admin = this.kafka.admin();
    this.producer = this.kafka.producer(this.options.producer);
  }

  async onModuleInit() {
    await this.connectAdmin();
    await this.connectProducer();
  }

  async onModuleDestroy() {
    await this.disconnectAll();
  }

  // Admin operations with connection reuse
  async withAdmin<T>(operation: (admin: Admin) => Promise<T>): Promise<T> {
    if (!this.adminConnected) {
      await this.connectAdmin();
    }
    return operation(this.admin);
  }

  // Consumer factory with registration - NO TIMESTAMPS IN GROUP IDs!
  createConsumer(groupId: string, config?: ConsumerConfig): Consumer {
    // Use the groupId as the consumer key (allowing reuse)
    if (this.consumers.has(groupId)) {
      return this.consumers.get(groupId);
    }

    const consumer = this.kafka.consumer({
      groupId,
      // Production-ready defaults
      sessionTimeout: config?.sessionTimeout || 30000,
      rebalanceTimeout: config?.rebalanceTimeout || 60000,
      heartbeatInterval: config?.heartbeatInterval || 3000,
      maxWaitTimeInMs: config?.maxWaitTimeInMs || 5000,
      // CRITICAL: Use cooperative-sticky for better rebalancing
      partition.assignment.strategy: ['CooperativeStickyAssignor'],
      ...config
    });

    // Set up rebalancing listeners
    consumer.on('consumer.group_join', ({ groupId, memberId }) => {
      this.logger.log(`Consumer joined group ${groupId} as ${memberId}`);
    });

    consumer.on('consumer.rebalancing', ({ groupId }) => {
      this.logger.warn(`Consumer group ${groupId} is rebalancing`);
    });

    this.consumers.set(groupId, consumer);
    return consumer;
  }

  // Topic management with caching
  private topicCache = new Set<string>();

  async ensureTopicExists(topic: string, config?: ITopicConfig): Promise<void> {
    if (this.topicCache.has(topic)) {
      return;
    }

    await this.withAdmin(async (admin) => {
      const topics = await admin.listTopics();
      if (!topics.includes(topic)) {
        await admin.createTopics({
          topics: [{ topic, ...config }],
        });
      }
      this.topicCache.add(topic);
    });
  }

  private async connectAdmin(): Promise<void> {
    if (this.adminConnected) return;
    await this.admin.connect();
    this.adminConnected = true;
  }

  private async connectProducer(): Promise<void> {
    if (this.producerConnected) return;
    await this.producer.connect();
    this.producerConnected = true;
  }

  private async disconnectAll(): Promise<void> {
    // Disconnect all consumers
    for (const [id, consumer] of this.consumers) {
      await consumer.disconnect();
    }
    this.consumers.clear();

    // Disconnect admin and producer
    if (this.adminConnected) {
      await this.admin.disconnect();
      this.adminConnected = false;
    }
    if (this.producerConnected) {
      await this.producer.disconnect();
      this.producerConnected = false;
    }
  }
}
```

**Why This Change**:
- Eliminates repeated admin connection creation/destruction
- Provides connection pooling and reuse
- Centralizes topic management with caching
- Ensures proper cleanup on shutdown

### 1.2 Implement Consumer Lifecycle Manager

**File**: `src/core/kafka-consumer-manager.service.ts`

**Purpose**: Centrally manage all consumer lifecycles and health

**Implementation**:
```typescript
@Injectable()
export class KafkaConsumerManager implements OnModuleDestroy {
  private consumers = new Map<string, ManagedConsumer>();

  constructor(
    private readonly connectionManager: KafkaConnectionManager,
    private readonly logger: Logger,
  ) {}

  async registerConsumer(
    id: string,
    config: ConsumerRegistration,
  ): Promise<void> {
    if (this.consumers.has(id)) {
      throw new Error(`Consumer ${id} already registered`);
    }

    const consumer = this.connectionManager.createConsumer(
      config.groupId,
      config.options,
    );

    this.consumers.set(id, {
      id,
      consumer,
      config,
      status: 'registered',
      startedAt: null,
      lastHeartbeat: null,
    });
  }

  async startConsumer(id: string): Promise<void> {
    const managed = this.consumers.get(id);
    if (!managed) {
      throw new Error(`Consumer ${id} not found`);
    }

    if (managed.status === 'running') {
      return;
    }

    await managed.consumer.connect();
    await managed.consumer.subscribe(managed.config.subscription);

    managed.status = 'running';
    managed.startedAt = new Date();

    // Start consuming with error handling
    await managed.consumer.run({
      ...managed.config.runOptions,
      eachMessage: async (payload) => {
        managed.lastHeartbeat = new Date();
        await managed.config.handler(payload);
      },
    });
  }

  async stopConsumer(id: string): Promise<void> {
    const managed = this.consumers.get(id);
    if (!managed) {
      return;
    }

    await managed.consumer.stop();
    await managed.consumer.disconnect();
    managed.status = 'stopped';
  }

  getHealthStatus(): ConsumerHealthStatus[] {
    return Array.from(this.consumers.values()).map((managed) => ({
      id: managed.id,
      groupId: managed.config.groupId,
      status: managed.status,
      startedAt: managed.startedAt,
      lastHeartbeat: managed.lastHeartbeat,
      isHealthy: this.isConsumerHealthy(managed),
    }));
  }

  private isConsumerHealthy(consumer: ManagedConsumer): boolean {
    if (consumer.status !== 'running') return false;
    if (!consumer.lastHeartbeat) return false;

    const heartbeatAge = Date.now() - consumer.lastHeartbeat.getTime();
    return heartbeatAge < 60000; // Consider unhealthy if no heartbeat for 1 minute
  }

  async onModuleDestroy() {
    for (const [id] of this.consumers) {
      await this.stopConsumer(id);
    }
  }
}
```

**Why This Change**:
- Centralizes consumer lifecycle management
- Provides health monitoring capabilities
- Ensures graceful shutdown of all consumers
- Prevents orphaned consumers

### 1.3 Create Unified Message Processing Pipeline

**File**: `src/core/kafka-message-processor.service.ts`

**Purpose**: Single entry point for all message processing with unified error handling

**Implementation**:
```typescript
@Injectable()
export class KafkaMessageProcessor {
  constructor(
    private readonly retryService: KafkaRetryService,
    private readonly dlqService: KafkaDlqService,
    private readonly metricsCollector: KafkaMetricsCollector,
    private readonly logger: Logger,
  ) {}

  async processMessage(
    context: MessageProcessingContext,
  ): Promise<ProcessingResult> {
    const startTime = Date.now();
    const { message, handler, options } = context;

    try {
      // Extract retry metadata if present
      const retryAttempt = this.extractRetryAttempt(message);

      // Process the message
      await handler(message);

      // Record success metrics
      this.metricsCollector.recordSuccess(
        context.topic,
        Date.now() - startTime,
      );

      return {
        status: 'success',
        processingTime: Date.now() - startTime,
      };
    } catch (error) {
      return this.handleProcessingError(error, context, startTime);
    }
  }

  private async handleProcessingError(
    error: Error,
    context: MessageProcessingContext,
    startTime: number,
  ): Promise<ProcessingResult> {
    const { message, options } = context;
    const retryAttempt = this.extractRetryAttempt(message) || 0;

    // Record failure metrics
    this.metricsCollector.recordFailure(context.topic, error);

    // Determine if we should retry
    if (this.shouldRetry(error, retryAttempt, options)) {
      await this.sendToRetryTopic(message, context, retryAttempt + 1);
      return {
        status: 'retried',
        processingTime: Date.now() - startTime,
        error,
      };
    }

    // Send to DLQ if max retries exceeded
    if (options.dlq?.enabled) {
      await this.sendToDlq(message, context, error);
      return {
        status: 'dlq',
        processingTime: Date.now() - startTime,
        error,
      };
    }

    // If no retry or DLQ, just log and continue
    this.logger.error('Message processing failed with no retry/DLQ', {
      error: error.message,
      topic: context.topic,
      partition: context.partition,
      offset: context.offset,
    });

    return {
      status: 'failed',
      processingTime: Date.now() - startTime,
      error,
    };
  }

  private shouldRetry(
    error: Error,
    attempt: number,
    options: ProcessingOptions,
  ): boolean {
    if (!options.retry?.enabled) return false;
    if (attempt >= options.retry.maxAttempts) return false;

    // Check if error is retryable
    return this.isRetryableError(error);
  }

  private isRetryableError(error: Error): boolean {
    // Implement logic to determine if error is retryable
    // e.g., network errors, timeout errors, etc.
    return true; // Simplified for example
  }

  private extractRetryAttempt(message: KafkaMessage): number {
    const attemptHeader = message.headers?.['x-retry-attempt'];
    if (!attemptHeader) return 0;
    return parseInt(attemptHeader.toString(), 10) || 0;
  }

  private async sendToRetryTopic(
    message: KafkaMessage,
    context: MessageProcessingContext,
    nextAttempt: number,
  ): Promise<void> {
    const retryTopic = `${context.topic}.retry`;
    const delay = this.retryService.calculateDelay(
      nextAttempt,
      context.options.retry,
    );

    await this.connectionManager.producer.send({
      topic: retryTopic,
      messages: [{
        ...message,
        headers: {
          ...message.headers,
          'x-retry-attempt': nextAttempt.toString(),
          'x-retry-delay': delay.toString(),
          'x-retry-at': new Date(Date.now() + delay).toISOString(),
          'x-original-topic': context.topic,
        },
      }],
    });
  }

  private async sendToDlq(
    message: KafkaMessage,
    context: MessageProcessingContext,
    error: Error,
  ): Promise<void> {
    const dlqTopic = `${context.topic}.dlq`;

    await this.connectionManager.producer.send({
      topic: dlqTopic,
      messages: [{
        ...message,
        headers: {
          ...message.headers,
          'x-dlq-reason': error.message,
          'x-dlq-timestamp': new Date().toISOString(),
          'x-original-topic': context.topic,
          'x-retry-attempts': this.extractRetryAttempt(message).toString(),
        },
      }],
    });
  }
}
```

**Why This Change**:
- Single place for all message processing logic
- Unified error handling and retry/DLQ decisions
- Clear separation between retry and DLQ flows
- Consistent metrics collection

---

## Phase 2: Service Simplification (Week 1-2)

### 2.1 Simplify Retry Service

**File**: `src/services/kafka-retry.service.ts`

**Purpose**: Focus ONLY on retry logic calculations

**Implementation**:
```typescript
@Injectable()
export class KafkaRetryService {
  // Remove all consumer management, topic creation, etc.
  // Focus only on retry logic

  calculateDelay(attempt: number, config: RetryConfig): number {
    const { strategy, initialDelay, maxDelay, multiplier } = config;

    let delay: number;

    switch (strategy) {
      case 'exponential':
        delay = Math.min(
          initialDelay * Math.pow(multiplier || 2, attempt - 1),
          maxDelay,
        );
        break;

      case 'linear':
        delay = Math.min(initialDelay * attempt, maxDelay);
        break;

      case 'fixed':
      default:
        delay = initialDelay;
    }

    // Add jitter to prevent thundering herd
    const jitter = Math.random() * delay * 0.1; // 10% jitter
    return Math.floor(delay + jitter);
  }

  shouldRetry(attempt: number, maxAttempts: number): boolean {
    return attempt < maxAttempts;
  }

  createRetryContext(
    message: KafkaMessage,
    error: Error,
    attempt: number,
  ): RetryContext {
    return {
      originalMessage: message,
      error: error.message,
      attempt,
      timestamp: new Date(),
    };
  }
}
```

**Why This Change**:
- Single responsibility: only handles retry logic
- No resource management concerns
- Reusable across different contexts
- Easy to test in isolation

### 2.2 Simplify DLQ Service

**File**: `src/services/kafka-dlq.service.ts`

**Purpose**: Handle DLQ message reprocessing only (NOT storage - DLQ is just a topic)

**Implementation**:
```typescript
@Injectable()
export class KafkaDlqService {
  constructor(
    private readonly connectionManager: KafkaConnectionManager,
    private readonly messageProcessor: KafkaMessageProcessor,
    private readonly logger: Logger,
  ) {}

  /**
   * Reprocess messages from DLQ topic back to original topic
   * IMPORTANT: Uses the ADMIN consumer group for one-off operations
   */
  async reprocessDlqMessages(
    dlqTopic: string,
    options: ReprocessOptions,
  ): Promise<ReprocessResult> {
    // Use ADMIN consumer group - shared across all admin operations
    // This prevents creating new consumer groups for each reprocess operation
    const consumer = this.connectionManager.createConsumer(
      `${process.env.SERVICE_NAME || 'service'}-admin`,
      {
        maxWaitTimeInMs: 1000,
        sessionTimeout: 10000,
        // IMPORTANT: Admin operations should not auto-commit
        // We manually commit after successful reprocessing
        autoCommit: false,
      },
    );

    const results: ReprocessResult = {
      total: 0,
      successful: 0,
      failed: 0,
      messages: [],
    };

    try {
      await consumer.connect();
      await consumer.subscribe({
        topic: dlqTopic,
        fromBeginning: true,
      });

      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ message, partition, topic }) => {
          results.total++;

          try {
            // Extract original topic from headers
            const originalTopic = message.headers?.['x-original-topic']?.toString();
            if (!originalTopic) {
              throw new Error('Original topic not found in DLQ message');
            }

            // Clear DLQ headers before reprocessing
            const cleanedMessage = this.cleanDlqHeaders(message);

            // Send back to original topic
            await this.connectionManager.producer.send({
              topic: originalTopic,
              messages: [cleanedMessage],
            });

            results.successful++;
            results.messages.push({
              status: 'success',
              offset: message.offset,
            });

            // Commit offset after successful reprocess
            await consumer.commitOffsets([{
              topic,
              partition,
              offset: (parseInt(message.offset) + 1).toString(),
            }]);

          } catch (error) {
            results.failed++;
            results.messages.push({
              status: 'failed',
              offset: message.offset,
              error: error.message,
            });

            this.logger.error('Failed to reprocess DLQ message', {
              error: error.message,
              offset: message.offset,
            });
          }

          // Stop if we've processed the requested number of messages
          if (options.limit && results.total >= options.limit) {
            await consumer.stop();
          }
        },
      });

    } finally {
      await consumer.disconnect();
    }

    return results;
  }

  /**
   * Get DLQ statistics without consuming messages
   */
  async getDlqStats(dlqTopic: string): Promise<DlqStats> {
    return this.connectionManager.withAdmin(async (admin) => {
      const metadata = await admin.fetchTopicMetadata({ topics: [dlqTopic] });
      const offsets = await admin.fetchTopicOffsets(dlqTopic);

      let totalMessages = 0;
      for (const partition of offsets) {
        const high = parseInt(partition.high);
        const low = parseInt(partition.low);
        totalMessages += (high - low);
      }

      return {
        topic: dlqTopic,
        messageCount: totalMessages,
        partitions: offsets.length,
        oldestOffset: Math.min(...offsets.map(p => parseInt(p.low))),
        newestOffset: Math.max(...offsets.map(p => parseInt(p.high))),
      };
    });
  }

  private cleanDlqHeaders(message: KafkaMessage): KafkaMessage {
    const headers = { ...message.headers };

    // Remove DLQ-specific headers before reprocessing
    delete headers['x-dlq-reason'];
    delete headers['x-dlq-timestamp'];
    delete headers['x-retry-attempts'];

    return { ...message, headers };
  }
}
```

**Why This Change**:
- Removes unnecessary "storage" concept - DLQ is just a Kafka topic
- Focuses on reprocessing logic only
- Clean separation from retry logic
- Provides useful statistics without consuming

### 2.3 Implement Proper Consumer Group Configuration

**File**: `src/config/consumer-groups.config.ts`

**Purpose**: Centralize consumer group configuration for production deployment

**Implementation**:
```typescript
@Injectable()
export class ConsumerGroupConfig {
  private readonly serviceName: string;

  constructor(@Inject('CONFIG') private config: AppConfig) {
    this.serviceName = config.serviceName || 'kafka-service';
  }

  /**
   * Get consumer group IDs for different purposes
   * CRITICAL: These must be consistent across all instances!
   */
  getConsumerGroups() {
    return {
      // Main processing - shared by all instances
      main: `${this.serviceName}-main`,

      // Retry processing - separate to avoid blocking main flow
      retry: `${this.serviceName}-retry`,

      // Admin operations - for manual/scheduled tasks
      admin: `${this.serviceName}-admin`,
    };
  }

  /**
   * Get consumer configuration with proper defaults for production
   */
  getConsumerConfig(type: 'main' | 'retry' | 'admin'): ConsumerConfig {
    const base = {
      // Cooperative rebalancing for smoother scaling
      partitionAssignors: [PartitionAssignors.CooperativeSticky],

      // Production timeouts
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      heartbeatInterval: 3000,

      // Fetch configuration
      maxWaitTimeInMs: 5000,
      minBytes: 1,
      maxBytes: 10485760, // 10MB
    };

    switch (type) {
      case 'main':
        return {
          ...base,
          // Main consumers should commit frequently
          autoCommit: true,
          autoCommitInterval: 5000,
        };

      case 'retry':
        return {
          ...base,
          // Retry consumers need more control over commits
          autoCommit: false,
          // Longer session timeout for retry processing
          sessionTimeout: 60000,
        };

      case 'admin':
        return {
          ...base,
          // Admin operations are manual, no auto-commit
          autoCommit: false,
          // Shorter timeouts for interactive operations
          sessionTimeout: 10000,
        };
    }
  }

  /**
   * Validate consumer group configuration for production
   */
  validateForProduction(): void {
    const groups = this.getConsumerGroups();

    // Ensure no timestamps or random values in group IDs
    Object.values(groups).forEach(groupId => {
      if (/\d{13,}/.test(groupId)) {
        throw new Error(`Consumer group ${groupId} contains timestamp - this breaks horizontal scaling!`);
      }
    });

    // Ensure service name is set
    if (!this.serviceName || this.serviceName === 'kafka-service') {
      throw new Error('SERVICE_NAME must be set for production deployment');
    }
  }
}
```

**Why This Change**:
- **Eliminates dynamic consumer groups** that break horizontal scaling
- **Consistent group IDs** across all instances for proper work distribution
- **Proper partition assignment** strategy (CooperativeSticky) for smooth rebalancing
- **Environment-specific configuration** with production-ready defaults

### 2.4 Create Dedicated Consumer Services

**File**: `src/consumers/retry-topic.consumer.ts`

**Purpose**: Dedicated consumer for retry topics with delay handling

**Implementation**:
```typescript
@Injectable()
export class RetryTopicConsumer implements OnModuleInit {
  private consumerId: string;
  private groupId: string;

  constructor(
    private readonly consumerManager: KafkaConsumerManager,
    private readonly messageProcessor: KafkaMessageProcessor,
    private readonly consumerGroupConfig: ConsumerGroupConfig,
    private readonly logger: Logger,
    @Inject('RETRY_HANDLERS') private handlers: Map<string, MessageHandler>,
  ) {
    // Use centralized consumer group configuration
    const groups = this.consumerGroupConfig.getConsumerGroups();
    this.groupId = groups.retry;
    this.consumerId = this.groupId; // Use group ID as consumer ID for consistency
  }

  async onModuleInit() {
    // Get proper consumer configuration for retry processing
    const consumerConfig = this.consumerGroupConfig.getConsumerConfig('retry');

    await this.consumerManager.registerConsumer(this.consumerId, {
      groupId: this.groupId,  // Uses configured retry consumer group
      subscription: {
        topics: ['*.retry'], // Subscribe to all retry topics
        fromBeginning: false,
      },
      runOptions: {
        eachMessage: this.handleRetryMessage.bind(this),
      },
    });

    await this.consumerManager.startConsumer(this.consumerId);
  }

  private async handleRetryMessage({
    topic,
    partition,
    message,
  }: EachMessagePayload): Promise<void> {
    // Check if message is ready to be retried
    const retryAt = message.headers?.['x-retry-at']?.toString();
    if (retryAt && new Date(retryAt) > new Date()) {
      // Message not ready yet, pause for a bit
      await this.sleep(1000);
      return;
    }

    // Extract original topic to find handler
    const originalTopic = message.headers?.['x-original-topic']?.toString();
    if (!originalTopic) {
      this.logger.error('No original topic in retry message');
      return;
    }

    const handler = this.handlers.get(originalTopic);
    if (!handler) {
      this.logger.error(`No handler found for topic: ${originalTopic}`);
      return;
    }

    // Process the message
    await this.messageProcessor.processMessage({
      message,
      handler,
      topic: originalTopic,
      partition,
      offset: message.offset,
      options: {
        retry: { enabled: true, maxAttempts: 3 },
        dlq: { enabled: true },
      },
    });
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}
```

**Why This Change**:
- Dedicated consumer for retry processing
- Handles delay logic properly
- Clear separation from main message flow
- Easy to monitor and control

---

## Phase 3: Configuration & Validation (Week 2)

### 3.1 Configuration Schema

**File**: `src/config/kafka-config.schema.ts`

**Implementation**:
```typescript
import * as Joi from 'joi';

export const KafkaConfigSchema = Joi.object({
  client: Joi.object({
    clientId: Joi.string().required(),
    brokers: Joi.array().items(Joi.string()).min(1).required(),
    ssl: Joi.boolean(),
    sasl: Joi.object({
      mechanism: Joi.string().valid('plain', 'scram-sha-256', 'scram-sha-512'),
      username: Joi.string(),
      password: Joi.string(),
    }).optional(),
  }).required(),

  consumer: Joi.object({
    groupId: Joi.string().required(),
    sessionTimeout: Joi.number().min(1000).default(30000),
    heartbeatInterval: Joi.number().min(100).default(3000),
    maxWaitTimeInMs: Joi.number().min(100).default(5000),
  }).required(),

  producer: Joi.object({
    allowAutoTopicCreation: Joi.boolean().default(false),
    transactionTimeout: Joi.number().min(1000).default(60000),
    idempotent: Joi.boolean().default(true),
    compression: Joi.string().valid('gzip', 'snappy', 'lz4', 'zstd').optional(),
  }).default({}),

  retry: Joi.object({
    enabled: Joi.boolean().default(true),
    maxAttempts: Joi.number().min(0).max(10).default(3),
    strategy: Joi.string().valid('exponential', 'linear', 'fixed').default('exponential'),
    initialDelay: Joi.number().min(100).default(1000),
    maxDelay: Joi.number().min(1000).default(30000),
    multiplier: Joi.number().min(1).max(10).default(2),
  }).default({}),

  dlq: Joi.object({
    enabled: Joi.boolean().default(true),
    suffix: Joi.string().default('.dlq'),
  }).default({}),

  topics: Joi.object({
    replicationFactor: Joi.number().min(1).default(1),
    numPartitions: Joi.number().min(1).default(1),
  }).default({}),
});

export function validateConfig(config: any): void {
  const { error } = KafkaConfigSchema.validate(config, {
    abortEarly: false,
    allowUnknown: false,
  });

  if (error) {
    const errors = error.details.map(d => d.message).join(', ');
    throw new Error(`Invalid Kafka configuration: ${errors}`);
  }
}
```

### 3.2 Message Validation

**File**: `src/validation/message-validator.ts`

**Implementation**:
```typescript
@Injectable()
export class KafkaMessageValidator {
  validateMessage<T>(
    message: KafkaMessage,
    schema: ZodSchema<T>,
  ): ValidationResult<T> {
    try {
      // Parse message value
      const parsed = JSON.parse(message.value.toString());

      // Validate against schema
      const result = schema.safeParse(parsed);

      if (result.success) {
        return {
          valid: true,
          data: result.data,
        };
      } else {
        return {
          valid: false,
          errors: result.error.errors,
        };
      }
    } catch (error) {
      return {
        valid: false,
        errors: [{ message: 'Invalid JSON in message value' }],
      };
    }
  }

  validateHeaders(headers: IHeaders): HeaderValidationResult {
    const errors: string[] = [];

    // Validate required headers
    if (!headers['x-message-id']) {
      errors.push('Missing x-message-id header');
    }

    // Validate header types
    for (const [key, value] of Object.entries(headers)) {
      if (value === null || value === undefined) {
        errors.push(`Header ${key} has null/undefined value`);
      }
    }

    return {
      valid: errors.length === 0,
      errors,
    };
  }
}
```

---

## Phase 4: Production Features (Week 2-3)

### 4.1 Circuit Breaker

**File**: `src/resilience/circuit-breaker.service.ts`

**Implementation**:
```typescript
@Injectable()
export class KafkaCircuitBreaker {
  private circuits = new Map<string, CircuitState>();

  async execute<T>(
    operation: () => Promise<T>,
    circuitName: string,
    options: CircuitOptions = {},
  ): Promise<T> {
    const circuit = this.getOrCreateCircuit(circuitName, options);

    if (circuit.state === 'open') {
      const now = Date.now();
      if (now - circuit.lastFailureTime < circuit.timeout) {
        throw new Error(`Circuit ${circuitName} is OPEN`);
      }
      // Try to half-open
      circuit.state = 'half-open';
    }

    try {
      const result = await operation();
      this.onSuccess(circuit);
      return result;
    } catch (error) {
      this.onFailure(circuit);
      throw error;
    }
  }

  private onSuccess(circuit: CircuitState): void {
    circuit.failureCount = 0;
    circuit.state = 'closed';
  }

  private onFailure(circuit: CircuitState): void {
    circuit.failureCount++;
    circuit.lastFailureTime = Date.now();

    if (circuit.failureCount >= circuit.threshold) {
      circuit.state = 'open';
    }
  }
}
```

### 4.2 Health Checks

**File**: `src/health/kafka-health.indicator.ts`

**Implementation**:
```typescript
@Injectable()
export class KafkaHealthIndicator extends HealthIndicator {
  constructor(
    private readonly connectionManager: KafkaConnectionManager,
    private readonly consumerManager: KafkaConsumerManager,
  ) {
    super();
  }

  async check(key: string): Promise<HealthIndicatorResult> {
    try {
      const isHealthy = await this.isHealthy();

      if (isHealthy) {
        return this.getStatus(key, true, {
          consumers: this.consumerManager.getHealthStatus(),
        });
      } else {
        return this.getStatus(key, false, {
          message: 'Kafka connection unhealthy',
        });
      }
    } catch (error) {
      return this.getStatus(key, false, {
        message: error.message,
      });
    }
  }

  private async isHealthy(): Promise<boolean> {
    // Check if admin can list topics
    try {
      await this.connectionManager.withAdmin(async (admin) => {
        await admin.listTopics();
      });
      return true;
    } catch {
      return false;
    }
  }
}
```

---

## Phase 5: Developer Experience (Week 3)

### 5.1 Simplified Decorators

**File**: `src/decorators/kafka-handler.decorator.ts`

**Implementation**:
```typescript
/**
 * Simple Kafka event handler with intelligent defaults
 */
export function KafkaHandler(topicOrOptions: string | KafkaHandlerOptions) {
  const options = typeof topicOrOptions === 'string'
    ? { topic: topicOrOptions }
    : topicOrOptions;

  return (target: any, propertyKey: string, descriptor: PropertyDescriptor) => {
    // Store metadata for discovery
    Reflect.defineMetadata(
      'kafka:handler',
      {
        topic: options.topic,
        retry: options.retry ?? { enabled: true, maxAttempts: 3 },
        dlq: options.dlq ?? { enabled: true },
        validation: options.validation,
      },
      target,
      propertyKey,
    );

    // Wrap the handler with validation if schema provided
    if (options.validation) {
      const originalMethod = descriptor.value;
      descriptor.value = async function(message: KafkaMessage) {
        const validator = new KafkaMessageValidator();
        const result = validator.validateMessage(message, options.validation);

        if (!result.valid) {
          throw new ValidationError('Message validation failed', result.errors);
        }

        return originalMethod.call(this, result.data);
      };
    }

    return descriptor;
  };
}

/**
 * Zero-config handler for simple cases
 */
export function SimpleKafkaHandler(topic: string) {
  return KafkaHandler({
    topic,
    retry: {
      enabled: true,
      maxAttempts: 3,
      strategy: 'exponential',
    },
    dlq: {
      enabled: true,
    },
  });
}
```

### 5.2 Configuration Builder

**File**: `src/builders/kafka-config.builder.ts`

**Implementation**:
```typescript
export class KafkaConfigBuilder {
  private config: Partial<KafkaModuleOptions> = {};

  static forDevelopment(clientId: string): KafkaConfigBuilder {
    return new KafkaConfigBuilder()
      .withClient({
        clientId,
        brokers: ['localhost:9092'],
      })
      .withConsumer({
        groupId: `${clientId}-group`,
      })
      .withRetry({
        enabled: true,
        maxAttempts: 3,
        strategy: 'exponential',
        initialDelay: 1000,
      })
      .withDlq({
        enabled: true,
      });
  }

  static forProduction(clientId: string, brokers: string[]): KafkaConfigBuilder {
    return new KafkaConfigBuilder()
      .withClient({
        clientId,
        brokers,
        ssl: true,
      })
      .withConsumer({
        groupId: `${clientId}-group`,
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
      })
      .withProducer({
        idempotent: true,
        compression: CompressionTypes.Snappy,
      })
      .withRetry({
        enabled: true,
        maxAttempts: 5,
        strategy: 'exponential',
        initialDelay: 2000,
        maxDelay: 60000,
      })
      .withDlq({
        enabled: true,
      });
  }

  build(): KafkaModuleOptions {
    validateConfig(this.config);
    return this.config as KafkaModuleOptions;
  }
}
```

---

## Migration Guide

### Step 1: Update Module Registration

```typescript
// Before
@Module({
  imports: [
    KafkaEnhancedModule.register({
      // Complex configuration
    }),
  ],
})
export class AppModule {}

// After
@Module({
  imports: [
    KafkaModule.register(
      KafkaConfigBuilder
        .forProduction('my-service', ['kafka:9092'])
        .withRetry({ maxAttempts: 5 })
        .build()
    ),
  ],
})
export class AppModule {}
```

### Step 2: Update Event Handlers

```typescript
// Before
@Controller()
export class EventController {
  @EventPattern('user.created')
  @UseInterceptors(KafkaRetryInterceptor)
  async handleUserCreated(@Payload() data: any) {
    // Handle event
  }
}

// After
@Injectable()
export class UserEventHandler {
  @KafkaHandler({
    topic: 'user.created',
    validation: UserCreatedSchema,
    retry: { maxAttempts: 5 },
  })
  async handleUserCreated(data: UserCreatedEvent) {
    // Handle event with type safety
  }
}
```

### Step 3: Test Your Changes

1. Run existing tests to ensure compatibility
2. Test retry mechanism with failing handlers
3. Verify DLQ messages are properly formatted
4. Check health endpoints report correct status
5. Monitor resource usage (connections, memory)

---

## Success Metrics

After implementing this refactoring, you should see:

1. **Resource Usage**
   - 70% reduction in Kafka connections
   - No connection leaks in production
   - Stable memory usage over time

2. **Reliability**
   - Zero unhandled message processing errors
   - Automatic recovery from transient failures
   - Clear visibility into system health

3. **Developer Experience**
   - 50% less boilerplate code
   - Type-safe message handling
   - Clear error messages

4. **Performance**
   - 2x improvement in message throughput
   - Consistent processing latency
   - Efficient retry handling

---

## Production Deployment Considerations

### Consumer Group and Scaling Guidelines

#### Horizontal Scaling Rules

1. **Partition Count Determines Max Parallelism**
   - If topic has 10 partitions, maximum 10 consumers can be active
   - 11th+ consumers will be idle (but ready for failover)
   - Plan partition count based on expected scale

2. **Consumer Group Best Practices**
   ```yaml
   # Kubernetes Deployment Example
   apiVersion: apps/v1
   kind: Deployment
   metadata:
     name: kafka-service
   spec:
     replicas: 3  # Must be <= partition count for optimal usage
     template:
       spec:
         containers:
         - name: app
           env:
           - name: SERVICE_NAME
             value: "order-service"  # CRITICAL: Must be same across all replicas!
           - name: KAFKA_CONSUMER_GROUP_MAIN
             value: "order-service-main"  # Derived from SERVICE_NAME
           - name: KAFKA_CONSUMER_GROUP_RETRY
             value: "order-service-retry"
   ```

3. **Rebalancing Impact**
   - When pods scale up/down, Kafka stops processing during rebalance
   - Use CooperativeSticky assignor to minimize disruption
   - Implement proper shutdown hooks to trigger clean rebalance

4. **Consumer Group Monitoring**
   ```bash
   # Monitor consumer group lag
   kafka-consumer-groups --bootstrap-server kafka:9092 \
     --group order-service-main --describe

   # Alert if lag > threshold
   if lag > 1000:
     alert("Consumer group falling behind")
   ```

### Critical Configuration for Production

```typescript
// environment.production.ts
export const productionConfig = {
  // NEVER use timestamps in consumer groups!
  consumerGroups: {
    main: process.env.SERVICE_NAME + '-main',
    retry: process.env.SERVICE_NAME + '-retry',
    admin: process.env.SERVICE_NAME + '-admin',
  },

  // Partition strategy for smooth scaling
  consumer: {
    partitionAssignors: [PartitionAssignors.CooperativeSticky],

    // Prevent rebalance storms
    maxPollInterval: 300000,  // 5 minutes
    sessionTimeout: 45000,     // 45 seconds

    // Optimize for throughput
    fetchMin: 1,
    fetchMax: 52428800,  // 50MB
  },

  // Topic creation should match expected scale
  topics: {
    numPartitions: 10,  // Allows up to 10 parallel consumers
    replicationFactor: 3,  // For high availability
  },
};
```

### Deployment Checklist

- [ ] **SERVICE_NAME environment variable is set** (no defaults in production!)
- [ ] **Consumer group names are static** (no timestamps, no random IDs)
- [ ] **Partition count >= expected replica count**
- [ ] **Rebalance listeners implemented** for graceful handling
- [ ] **Consumer lag monitoring configured**
- [ ] **Proper shutdown hooks implemented**
- [ ] **Health checks include consumer group status**

## Common Pitfalls to Avoid

### Consumer Group Anti-Patterns

1. **❌ NEVER create dynamic consumer groups**
   ```typescript
   // WRONG - Creates new group each time
   const consumer = createConsumer(`processor-${Date.now()}`);

   // CORRECT - Reuses same group
   const consumer = createConsumer(config.consumerGroups.main);
   ```

2. **❌ NEVER use random/unique IDs in group names**
   ```typescript
   // WRONG - Each instance gets different group
   const groupId = `service-${uuid()}`;

   // CORRECT - All instances share same group
   const groupId = `${SERVICE_NAME}-main`;
   ```

3. **❌ NEVER create consumer groups per-operation**
   ```typescript
   // WRONG - New group for each DLQ reprocess
   async reprocessDlq() {
     const consumer = createConsumer(`dlq-${Date.now()}`);
   }

   // CORRECT - Reuse admin consumer group
   async reprocessDlq() {
     const consumer = createConsumer(this.config.consumerGroups.admin);
   }
   ```

### Other Pitfalls

4. **Don't create Admin connections in services** - Always use KafkaConnectionManager
5. **Don't manage consumers directly** - Use KafkaConsumerManager
6. **Don't mix retry and DLQ logic** - Keep them separate
7. **Don't forget to validate messages** - Use schemas
8. **Don't ignore health checks** - Monitor in production
9. **Don't hardcode consumer group names** - Use configuration

---

## Support and Questions

For questions about this refactoring plan:
1. Review the implementation examples carefully
2. Start with Phase 1 (critical infrastructure)
3. Test thoroughly at each phase
4. Monitor metrics before and after changes

This refactoring transforms a fragile implementation into a production-ready, maintainable Kafka events system that follows NestJS best practices while maintaining the granular control you need.