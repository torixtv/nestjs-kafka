import { ModuleMetadata, Type } from '@nestjs/common';
import { KafkaConfig, ConsumerConfig, ProducerConfig } from 'kafkajs';

export interface KafkaConsumerRegistration {
  topics: string[];
  fromBeginning?: boolean;
}

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

export interface KafkaModuleOptions {
  client?: KafkaConfig;
  consumer?: ConsumerConfig;
  producer?: ProducerConfig;
  subscriptions?: KafkaConsumerRegistration;
  retry?: KafkaRetryOptions;
  dlq?: KafkaDlqOptions;
  requireBroker?: boolean;
  serviceName?: string;  // Service name for consumer group generation
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
