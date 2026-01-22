import { ModuleMetadata, Type } from '@nestjs/common';
import { KafkaConfig, ConsumerConfig, ProducerConfig } from 'kafkajs';


export interface KafkaRetryOptions {
  enabled?: boolean;
  attempts?: number;
  backoff?: 'linear' | 'exponential';
  maxDelay?: number;
  baseDelay?: number;
  topicPartitions?: number;
  topicReplicationFactor?: number;
  topicRetentionMs?: number;
  topicSegmentMs?: number;
}

export interface KafkaDlqReprocessingOptions {
  batchSize?: number;
  timeoutMs?: number;
  stopOnError?: boolean;
}

export interface KafkaDlqOptions {
  enabled?: boolean;
  topic?: string;
  topicPartitions?: number;
  topicReplicationFactor?: number;
  topicRetentionMs?: number;
  topicSegmentMs?: number;
  reprocessingOptions?: KafkaDlqReprocessingOptions;
  onFailure?: (message: any, error: Error) => Promise<void>;
}

export interface KafkaConsumerSubscription {
  topics: string[];
  fromBeginning?: boolean;
}

export interface KafkaMonitoringOptions {
  enabled?: boolean; // Default: true
  path?: string;     // Default: 'kafka'
}

/**
 * Health check configuration options for Kafka consumer.
 * These options control the grace periods used to prevent unnecessary
 * pod restarts during normal Kafka operations.
 */
export interface KafkaHealthOptions {
  /**
   * Grace period after startup before health checks become strict.
   * During this period, the consumer is always reported as healthy.
   * Default: 180000 (3 minutes)
   */
  startupGracePeriodMs?: number;

  /**
   * Grace period during rebalancing before marking consumer as stale.
   * Rebalancing temporarily revokes partitions, so we allow time for reassignment.
   * Default: 120000 (2 minutes)
   */
  rebalanceGracePeriodMs?: number;

  /**
   * Time without partitions after which consumer is considered stale.
   * Only applies after consumer previously had partitions assigned.
   * Default: 600000 (10 minutes)
   */
  staleThresholdMs?: number;
}

export interface KafkaModuleOptions {
  client?: KafkaConfig;
  consumer?: ConsumerConfig;
  producer?: ProducerConfig;
  subscriptions?: KafkaConsumerSubscription;
  retry?: KafkaRetryOptions;
  dlq?: KafkaDlqOptions;
  monitoring?: KafkaMonitoringOptions;
  health?: KafkaHealthOptions;
}

export interface KafkaModuleOptionsFactory {
  createKafkaModuleOptions(): Promise<KafkaModuleOptions> | KafkaModuleOptions;
}

export interface KafkaModuleAsyncOptions
  extends Pick<ModuleMetadata, 'imports'> {
  useExisting?: Type<KafkaModuleOptionsFactory>;
  useClass?: Type<KafkaModuleOptionsFactory>;
  useFactory?: (
    ...args: any[]
  ) => Promise<KafkaModuleOptions> | KafkaModuleOptions;
  inject?: any[];
  extraProviders?: any[];
}

export interface EventHandlerOptions {
  retry?: KafkaRetryOptions;
  dlq?: Pick<KafkaDlqOptions, 'enabled' | 'topic'>;
}

export interface EventHandlerMetadata {
  pattern: string;
  options: EventHandlerOptions;
}
