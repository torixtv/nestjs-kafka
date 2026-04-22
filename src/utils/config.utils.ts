import { KafkaConfig } from 'kafkajs';

import { KafkaModuleOptions } from '../interfaces/kafka.interfaces';

/**
 * Reads SASL configuration from environment variables
 * Environment variables:
 * - KAFKA_SASL_MECHANISM: SASL mechanism (e.g., 'scram-sha-256', 'scram-sha-512', 'plain')
 * - KAFKA_SASL_USERNAME: SASL username
 * - KAFKA_SASL_PASSWORD: SASL password
 * - KAFKA_SSL_ENABLED: Enable SSL (optional - auto-enabled when SASL is configured)
 */
export function readSaslConfigFromEnv(): Partial<KafkaConfig> {
  const mechanism = process.env.KAFKA_SASL_MECHANISM;
  const username = process.env.KAFKA_SASL_USERNAME;
  const password = process.env.KAFKA_SASL_PASSWORD;
  const sslEnabled = process.env.KAFKA_SSL_ENABLED;

  const config: Partial<KafkaConfig> = {};

  // Only configure SASL if all required fields are present
  if (mechanism && username && password) {
    config.sasl = {
      mechanism: mechanism.toLowerCase() as any,
      username,
      password,
    };
  }

  // Handle SSL configuration
  if (sslEnabled !== undefined) {
    // Explicit SSL configuration from env
    config.ssl = sslEnabled === 'true' || sslEnabled === '1';
  }

  return config;
}

/**
 * Applies smart defaults to Kafka configuration:
 * - Automatically enables SSL when SASL is configured (unless explicitly disabled)
 * - Normalizes SASL mechanism to lowercase
 */
export function applyConfigurationSmartDefaults(
  config: KafkaConfig,
): KafkaConfig {
  const result = { ...config };

  // Smart default: Enable SSL when SASL is configured and SSL is not explicitly set
  if (result.sasl && result.ssl === undefined) {
    result.ssl = true;
  }

  // Normalize SASL mechanism to lowercase
  if (result.sasl?.mechanism) {
    result.sasl = {
      ...result.sasl,
      mechanism: result.sasl.mechanism.toLowerCase() as any,
    };
  }

  return result;
}

/**
 * Coerces a value to a number. Useful for values that originate from
 * process.env (always strings) but are typed as number in config.
 * Returns fallback when the value is nullish or cannot be parsed.
 */
export function toNumber(value: unknown, fallback?: number): number | undefined {
  if (value == null) return fallback;
  if (typeof value === 'number') return Number.isFinite(value) ? value : fallback;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed === '') return fallback;
    const n = Number(trimmed);
    return Number.isFinite(n) ? n : fallback;
  }
  return fallback;
}

/**
 * Coerces a value to a boolean. Accepts 'true'/'false', '1'/'0', 'yes'/'no'
 * (case-insensitive). Returns fallback for nullish or unrecognized values.
 */
export function toBoolean(value: unknown, fallback?: boolean): boolean | undefined {
  if (value == null) return fallback;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true' || normalized === '1' || normalized === 'yes') return true;
    if (normalized === 'false' || normalized === '0' || normalized === 'no') return false;
    return fallback;
  }
  return fallback;
}

const assignNumber = <T extends object, K extends keyof T>(target: T, key: K): void => {
  const coerced = toNumber(target[key]);
  if (coerced !== undefined) {
    target[key] = coerced as T[K];
  } else if (target[key] != null && typeof target[key] === 'string') {
    // Unparseable string — delete so kafkajs default kicks in rather than crashing
    delete target[key];
  }
};

const assignBoolean = <T extends object, K extends keyof T>(target: T, key: K): void => {
  const coerced = toBoolean(target[key]);
  if (coerced !== undefined) {
    target[key] = coerced as T[K];
  } else if (target[key] != null && typeof target[key] === 'string') {
    delete target[key];
  }
};

/**
 * Coerces string values to number/boolean for all known numeric/boolean fields
 * of KafkaModuleOptions. Consumers wiring values directly from NestJS
 * ConfigService (which returns strings from process.env) would otherwise
 * crash kafkajs's runtime type validation.
 *
 * Returns a new object; does not mutate the input.
 */
export function coerceKafkaModuleOptions(
  options: KafkaModuleOptions = {},
): KafkaModuleOptions {
  const result: KafkaModuleOptions = {
    ...options,
    client: options.client ? { ...options.client } : options.client,
    consumer: options.consumer ? { ...options.consumer } : options.consumer,
    producer: options.producer ? { ...options.producer } : options.producer,
    subscriptions: options.subscriptions
      ? { ...options.subscriptions }
      : options.subscriptions,
    retry: options.retry ? { ...options.retry } : options.retry,
    dlq: options.dlq
      ? {
          ...options.dlq,
          reprocessingOptions: options.dlq.reprocessingOptions
            ? { ...options.dlq.reprocessingOptions }
            : options.dlq.reprocessingOptions,
        }
      : options.dlq,
    monitoring: options.monitoring ? { ...options.monitoring } : options.monitoring,
    health: options.health ? { ...options.health } : options.health,
  };

  if (result.client) {
    assignNumber(result.client, 'connectionTimeout');
    assignNumber(result.client, 'requestTimeout');
    // ssl can be a boolean or a tls.ConnectionOptions object — only coerce strings
    if (typeof result.client.ssl === 'string') {
      assignBoolean(result.client, 'ssl');
    }
  }

  if (result.consumer) {
    assignNumber(result.consumer, 'sessionTimeout');
    assignNumber(result.consumer, 'heartbeatInterval');
    assignNumber(result.consumer, 'maxWaitTimeInMs');
    assignNumber(result.consumer, 'rebalanceTimeout');
    assignNumber(result.consumer, 'maxBytes');
    assignNumber(result.consumer, 'maxBytesPerPartition');
    assignNumber(result.consumer, 'minBytes');
    assignBoolean(result.consumer, 'allowAutoTopicCreation');
    assignBoolean(result.consumer, 'readUncommitted');
  }

  if (result.producer) {
    assignNumber(result.producer, 'maxInFlightRequests');
    assignNumber(result.producer, 'transactionTimeout');
    assignBoolean(result.producer, 'idempotent');
    assignBoolean(result.producer, 'allowAutoTopicCreation');
  }

  if (result.subscriptions) {
    assignBoolean(result.subscriptions, 'fromBeginning');
  }

  if (result.retry) {
    assignBoolean(result.retry, 'enabled');
    assignNumber(result.retry, 'attempts');
    assignNumber(result.retry, 'maxDelay');
    assignNumber(result.retry, 'baseDelay');
    assignNumber(result.retry, 'topicPartitions');
    assignNumber(result.retry, 'topicReplicationFactor');
    assignNumber(result.retry, 'topicRetentionMs');
    assignNumber(result.retry, 'topicSegmentMs');
  }

  if (result.dlq) {
    assignBoolean(result.dlq, 'enabled');
    assignNumber(result.dlq, 'topicPartitions');
    assignNumber(result.dlq, 'topicReplicationFactor');
    assignNumber(result.dlq, 'topicRetentionMs');
    assignNumber(result.dlq, 'topicSegmentMs');

    if (result.dlq.reprocessingOptions) {
      assignNumber(result.dlq.reprocessingOptions, 'batchSize');
      assignNumber(result.dlq.reprocessingOptions, 'timeoutMs');
      assignBoolean(result.dlq.reprocessingOptions, 'stopOnError');
    }
  }

  if (result.monitoring) {
    assignBoolean(result.monitoring, 'enabled');
  }

  if (result.health) {
    assignNumber(result.health, 'startupGracePeriodMs');
    assignNumber(result.health, 'rebalanceGracePeriodMs');
    assignNumber(result.health, 'staleThresholdMs');
  }

  return result;
}

/**
 * Merges environment-based configuration with user-provided configuration
 * Precedence: user config > environment variables
 */
export function mergeWithEnvironmentConfig(
  userConfig: KafkaConfig,
): KafkaConfig {
  const envConfig = readSaslConfigFromEnv();

  // User config takes precedence
  const merged: KafkaConfig = {
    ...userConfig,
  };

  // Apply SASL from env only if not explicitly provided by user
  if (envConfig.sasl && !userConfig.sasl) {
    merged.sasl = envConfig.sasl;
  }

  // Apply SSL from env only if not explicitly provided by user
  if (envConfig.ssl !== undefined && userConfig.ssl === undefined) {
    merged.ssl = envConfig.ssl;
  }

  // Apply smart defaults after merging
  return applyConfigurationSmartDefaults(merged);
}
