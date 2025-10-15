import { KafkaConfig } from 'kafkajs';

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
