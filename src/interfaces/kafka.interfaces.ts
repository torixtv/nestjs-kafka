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

  /**
   * Grace period after a transient disconnect before reporting unhealthy.
   * Applies when KafkaJS auto-restarts after a CRASH (restart=true) or after
   * an unexpected DISCONNECT — the consumer typically self-recovers in
   * 25–30s, so a short grace period prevents Kubernetes from killing pods
   * mid-recovery. Set the timestamp via the disconnect/crash event handlers
   * and clear it on (re)connect.
   *
   * Also applies during a manual reconnect after a non-restartable CRASH
   * (see `manualReconnect`). Fail-fast crashes with `manualReconnect`
   * disabled bypass this grace period and report unhealthy immediately.
   * Default: 60000 (60 seconds)
   */
  disconnectGracePeriodMs?: number;

  /**
   * Manual reconnect behavior after a CRASH where KafkaJS declined to
   * auto-restart (`payload.restart === false`). Without this, the consumer
   * stays dead until Kubernetes kills the pod — which is the desired
   * fail-fast behavior for genuine fatal errors, but problematic when
   * KafkaJS classifies transient broker hiccups (e.g. Redpanda Serverless
   * leader elections returning BROKER_NOT_AVAILABLE) as non-retriable.
   *
   * Note that the consumer's `retry.restartOnFailure` callback in kafkajs
   * 2.x is only consulted for already-retriable errors, so it cannot help
   * with this class of crash — manual reconnect is the only way to recover
   * in-process.
   *
   * When enabled, the service tears down the dead kafkajs Consumer
   * instance, creates a new one, re-subscribes, and resumes consumption —
   * with exponential backoff between attempts. The disconnect grace period
   * applies while reconnect is in progress so health checks stay healthy.
   *
   * Default: disabled (preserves prior fail-fast semantics).
   */
  manualReconnect?: KafkaManualReconnectOptions;
}

/**
 * Configuration for manual consumer recovery after a non-restartable CRASH.
 * See `KafkaHealthOptions.manualReconnect`.
 */
export interface KafkaManualReconnectOptions {
  /**
   * Enable manual reconnect when KafkaJS emits CRASH with `restart=false`.
   * Default: false
   */
  enabled?: boolean;

  /**
   * Maximum number of reconnect attempts before giving up and letting the
   * pod be restarted by Kubernetes.
   * Default: 5
   */
  maxAttempts?: number;

  /**
   * Initial delay between reconnect attempts (milliseconds). Each subsequent
   * attempt doubles this delay, capped at `maxDelayMs`.
   * Default: 1000 (1 second)
   */
  baseDelayMs?: number;

  /**
   * Maximum delay between reconnect attempts (milliseconds).
   * Default: 30000 (30 seconds)
   */
  maxDelayMs?: number;
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
