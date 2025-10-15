/**
 * Example: Cloud Kafka Configuration with Automatic SASL + SSL
 *
 * This example demonstrates how the package automatically configures
 * SASL and SSL for cloud Kafka providers (Confluent, Redpanda, AWS MSK, etc.)
 * with minimal configuration.
 */

import { Module } from '@nestjs/common';
import { KafkaModule } from '@torixtv/nestjs-kafka';

/**
 * Example 1: Zero-Config Cloud Kafka (Recommended)
 *
 * Set these environment variables:
 *
 * KAFKA_BROKERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
 * KAFKA_SASL_MECHANISM=plain
 * KAFKA_SASL_USERNAME=your-api-key
 * KAFKA_SASL_PASSWORD=your-api-secret
 *
 * The package automatically:
 * - Reads SASL config from KAFKA_SASL_* environment variables
 * - Enables SSL (because SASL is configured)
 * - Normalizes mechanism to lowercase
 */
@Module({
  imports: [
    KafkaModule.forRoot({
      client: {
        brokers: process.env.KAFKA_BROKERS!.split(','),
        // That's it! SASL and SSL are automatically configured
      },
      consumer: {
        groupId: 'my-consumer-group',
      },
    }),
  ],
})
export class AutomaticCloudKafkaModule {}

/**
 * Example 2: Explicit Cloud Kafka Configuration
 *
 * If you prefer to be explicit or need special settings:
 */
@Module({
  imports: [
    KafkaModule.forRoot({
      client: {
        brokers: ['pkc-xxxxx.us-east-1.aws.confluent.cloud:9092'],
        ssl: true,  // Explicitly enable SSL
        sasl: {
          mechanism: 'plain',
          username: 'your-api-key',
          password: 'your-api-secret',
        },
      },
      consumer: {
        groupId: 'my-consumer-group',
      },
    }),
  ],
})
export class ExplicitCloudKafkaModule {}

/**
 * Example 3: Mixed Configuration (Environment + Explicit)
 *
 * User config takes precedence over environment variables.
 *
 * Environment:
 * KAFKA_SASL_MECHANISM=plain
 * KAFKA_SASL_USERNAME=env-user
 * KAFKA_SASL_PASSWORD=env-pass
 */
@Module({
  imports: [
    KafkaModule.forRoot({
      client: {
        brokers: ['localhost:9092'],
        ssl: false,  // Explicit override - prevents auto-enabling SSL
        // SASL config from environment will still be used
      },
      consumer: {
        groupId: 'my-consumer-group',
      },
    }),
  ],
})
export class MixedConfigModule {}

/**
 * Example 4: Redpanda Cloud with SCRAM-SHA-256
 *
 * Environment:
 * KAFKA_BROKERS=seed-xxxxx.redpanda.cloud:9092
 * KAFKA_SASL_MECHANISM=scram-sha-256
 * KAFKA_SASL_USERNAME=redpanda-user
 * KAFKA_SASL_PASSWORD=redpanda-password
 */
@Module({
  imports: [
    KafkaModule.forRoot({
      client: {
        brokers: process.env.KAFKA_BROKERS!.split(','),
      },
      consumer: {
        groupId: 'redpanda-consumer',
      },
    }),
  ],
})
export class RedpandaCloudModule {}

/**
 * Example 5: Local Development (No SSL, No SASL)
 *
 * No environment variables set - works with local Kafka
 */
@Module({
  imports: [
    KafkaModule.forRoot({
      client: {
        brokers: ['localhost:9092'],
        // No SSL, no SASL - perfect for local development
      },
      consumer: {
        groupId: 'local-consumer',
      },
    }),
  ],
})
export class LocalDevelopmentModule {}

/**
 * Example 6: Using Configuration Utilities Directly
 *
 * Advanced use case: manually applying configuration utilities
 */
import {
  readSaslConfigFromEnv,
  applyConfigurationSmartDefaults,
  mergeWithEnvironmentConfig,
} from '@torixtv/nestjs-kafka';

// Read SASL config from environment
const envConfig = readSaslConfigFromEnv();
console.log('SASL from env:', envConfig);

// Apply smart defaults to config
const userConfig = {
  brokers: ['localhost:9092'],
  sasl: {
    mechanism: 'SCRAM-SHA-256' as any,
    username: 'user',
    password: 'pass',
  },
};
const withDefaults = applyConfigurationSmartDefaults(userConfig);
console.log('With smart defaults:', withDefaults);
// Output: { ...userConfig, sasl: { mechanism: 'scram-sha-256', ... }, ssl: true }

// Merge user config with environment
const merged = mergeWithEnvironmentConfig(userConfig);
console.log('Merged with env:', merged);
// User config takes precedence, smart defaults applied
