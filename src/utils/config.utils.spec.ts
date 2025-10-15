import { KafkaConfig } from 'kafkajs';
import {
  readSaslConfigFromEnv,
  applyConfigurationSmartDefaults,
  mergeWithEnvironmentConfig,
} from './config.utils';

describe('Configuration Utils', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    // Reset environment before each test
    process.env = { ...originalEnv };
    delete process.env.KAFKA_SASL_MECHANISM;
    delete process.env.KAFKA_SASL_USERNAME;
    delete process.env.KAFKA_SASL_PASSWORD;
    delete process.env.KAFKA_SSL_ENABLED;
  });

  afterAll(() => {
    process.env = originalEnv;
  });

  describe('readSaslConfigFromEnv', () => {
    it('should return empty config when no env vars are set', () => {
      const config = readSaslConfigFromEnv();
      expect(config).toEqual({});
    });

    it('should read complete SASL config from environment', () => {
      process.env.KAFKA_SASL_MECHANISM = 'SCRAM-SHA-256';
      process.env.KAFKA_SASL_USERNAME = 'test-user';
      process.env.KAFKA_SASL_PASSWORD = 'test-password';

      const config = readSaslConfigFromEnv();

      expect(config.sasl).toEqual({
        mechanism: 'scram-sha-256', // Normalized to lowercase
        username: 'test-user',
        password: 'test-password',
      });
    });

    it('should normalize SASL mechanism to lowercase', () => {
      process.env.KAFKA_SASL_MECHANISM = 'SCRAM-SHA-512';
      process.env.KAFKA_SASL_USERNAME = 'user';
      process.env.KAFKA_SASL_PASSWORD = 'pass';

      const config = readSaslConfigFromEnv();
      expect(config.sasl?.mechanism).toBe('scram-sha-512');
    });

    it('should not configure SASL if mechanism is missing', () => {
      process.env.KAFKA_SASL_USERNAME = 'user';
      process.env.KAFKA_SASL_PASSWORD = 'pass';

      const config = readSaslConfigFromEnv();
      expect(config.sasl).toBeUndefined();
    });

    it('should not configure SASL if username is missing', () => {
      process.env.KAFKA_SASL_MECHANISM = 'plain';
      process.env.KAFKA_SASL_PASSWORD = 'pass';

      const config = readSaslConfigFromEnv();
      expect(config.sasl).toBeUndefined();
    });

    it('should not configure SASL if password is missing', () => {
      process.env.KAFKA_SASL_MECHANISM = 'plain';
      process.env.KAFKA_SASL_USERNAME = 'user';

      const config = readSaslConfigFromEnv();
      expect(config.sasl).toBeUndefined();
    });

    it('should read SSL config when explicitly set to true', () => {
      process.env.KAFKA_SSL_ENABLED = 'true';

      const config = readSaslConfigFromEnv();
      expect(config.ssl).toBe(true);
    });

    it('should read SSL config when set to "1"', () => {
      process.env.KAFKA_SSL_ENABLED = '1';

      const config = readSaslConfigFromEnv();
      expect(config.ssl).toBe(true);
    });

    it('should read SSL config when explicitly set to false', () => {
      process.env.KAFKA_SSL_ENABLED = 'false';

      const config = readSaslConfigFromEnv();
      expect(config.ssl).toBe(false);
    });

    it('should combine SASL and SSL config from environment', () => {
      process.env.KAFKA_SASL_MECHANISM = 'plain';
      process.env.KAFKA_SASL_USERNAME = 'user';
      process.env.KAFKA_SASL_PASSWORD = 'pass';
      process.env.KAFKA_SSL_ENABLED = 'true';

      const config = readSaslConfigFromEnv();

      expect(config.sasl).toBeDefined();
      expect(config.ssl).toBe(true);
    });
  });

  describe('applyConfigurationSmartDefaults', () => {
    it('should automatically enable SSL when SASL is configured', () => {
      const config: KafkaConfig = {
        brokers: ['localhost:9092'],
        sasl: {
          mechanism: 'scram-sha-256',
          username: 'user',
          password: 'pass',
        },
      };

      const result = applyConfigurationSmartDefaults(config);

      expect(result.ssl).toBe(true);
    });

    it('should not override explicit SSL=false when SASL is configured', () => {
      const config: KafkaConfig = {
        brokers: ['localhost:9092'],
        ssl: false,
        sasl: {
          mechanism: 'plain',
          username: 'user',
          password: 'pass',
        },
      };

      const result = applyConfigurationSmartDefaults(config);

      expect(result.ssl).toBe(false);
    });

    it('should respect explicit SSL=true', () => {
      const config: KafkaConfig = {
        brokers: ['localhost:9092'],
        ssl: true,
      };

      const result = applyConfigurationSmartDefaults(config);

      expect(result.ssl).toBe(true);
    });

    it('should not add SSL when SASL is not configured', () => {
      const config: KafkaConfig = {
        brokers: ['localhost:9092'],
      };

      const result = applyConfigurationSmartDefaults(config);

      expect(result.ssl).toBeUndefined();
    });

    it('should normalize SASL mechanism to lowercase', () => {
      const config: KafkaConfig = {
        brokers: ['localhost:9092'],
        sasl: {
          mechanism: 'SCRAM-SHA-512' as any,
          username: 'user',
          password: 'pass',
        },
      };

      const result = applyConfigurationSmartDefaults(config);

      expect(result.sasl?.mechanism).toBe('scram-sha-512');
    });

    it('should handle mixed case mechanism names', () => {
      const config: KafkaConfig = {
        brokers: ['localhost:9092'],
        sasl: {
          mechanism: 'PlAiN' as any,
          username: 'user',
          password: 'pass',
        },
      };

      const result = applyConfigurationSmartDefaults(config);

      expect(result.sasl?.mechanism).toBe('plain');
    });

    it('should not mutate original config object', () => {
      const config: KafkaConfig = {
        brokers: ['localhost:9092'],
        sasl: {
          mechanism: 'plain',
          username: 'user',
          password: 'pass',
        },
      };

      const result = applyConfigurationSmartDefaults(config);

      expect(result).not.toBe(config);
      expect(config.ssl).toBeUndefined(); // Original unchanged
    });
  });

  describe('mergeWithEnvironmentConfig', () => {
    it('should use environment SASL config when user config has none', () => {
      process.env.KAFKA_SASL_MECHANISM = 'plain';
      process.env.KAFKA_SASL_USERNAME = 'env-user';
      process.env.KAFKA_SASL_PASSWORD = 'env-pass';

      const userConfig: KafkaConfig = {
        brokers: ['localhost:9092'],
      };

      const result = mergeWithEnvironmentConfig(userConfig);

      expect(result.sasl).toEqual({
        mechanism: 'plain',
        username: 'env-user',
        password: 'env-pass',
      });
      expect(result.ssl).toBe(true); // Smart default
    });

    it('should prefer user SASL config over environment', () => {
      process.env.KAFKA_SASL_MECHANISM = 'plain';
      process.env.KAFKA_SASL_USERNAME = 'env-user';
      process.env.KAFKA_SASL_PASSWORD = 'env-pass';

      const userConfig: KafkaConfig = {
        brokers: ['localhost:9092'],
        sasl: {
          mechanism: 'scram-sha-256',
          username: 'user-user',
          password: 'user-pass',
        },
      };

      const result = mergeWithEnvironmentConfig(userConfig);

      expect(result.sasl).toEqual({
        mechanism: 'scram-sha-256', // User config wins
        username: 'user-user',
        password: 'user-pass',
      });
    });

    it('should use environment SSL when user config has none', () => {
      process.env.KAFKA_SSL_ENABLED = 'false';

      const userConfig: KafkaConfig = {
        brokers: ['localhost:9092'],
      };

      const result = mergeWithEnvironmentConfig(userConfig);

      expect(result.ssl).toBe(false);
    });

    it('should prefer user SSL config over environment', () => {
      process.env.KAFKA_SSL_ENABLED = 'false';

      const userConfig: KafkaConfig = {
        brokers: ['localhost:9092'],
        ssl: true,
      };

      const result = mergeWithEnvironmentConfig(userConfig);

      expect(result.ssl).toBe(true); // User config wins
    });

    it('should apply smart defaults after merging', () => {
      process.env.KAFKA_SASL_MECHANISM = 'SCRAM-SHA-256';
      process.env.KAFKA_SASL_USERNAME = 'user';
      process.env.KAFKA_SASL_PASSWORD = 'pass';

      const userConfig: KafkaConfig = {
        brokers: ['localhost:9092'],
      };

      const result = mergeWithEnvironmentConfig(userConfig);

      expect(result.sasl?.mechanism).toBe('scram-sha-256'); // Normalized
      expect(result.ssl).toBe(true); // Auto-enabled
    });

    it('should handle complete cloud Kafka configuration from env', () => {
      process.env.KAFKA_SASL_MECHANISM = 'scram-sha-512';
      process.env.KAFKA_SASL_USERNAME = 'cloud-user';
      process.env.KAFKA_SASL_PASSWORD = 'cloud-pass';

      const userConfig: KafkaConfig = {
        brokers: ['my-cluster.kafka.cloud:9092'],
        clientId: 'my-service',
      };

      const result = mergeWithEnvironmentConfig(userConfig);

      expect(result).toEqual({
        brokers: ['my-cluster.kafka.cloud:9092'],
        clientId: 'my-service',
        sasl: {
          mechanism: 'scram-sha-512',
          username: 'cloud-user',
          password: 'cloud-pass',
        },
        ssl: true, // Auto-enabled
      });
    });

    it('should allow user to explicitly disable SSL even with SASL', () => {
      process.env.KAFKA_SASL_MECHANISM = 'plain';
      process.env.KAFKA_SASL_USERNAME = 'user';
      process.env.KAFKA_SASL_PASSWORD = 'pass';

      const userConfig: KafkaConfig = {
        brokers: ['localhost:9092'],
        ssl: false, // Explicit override
      };

      const result = mergeWithEnvironmentConfig(userConfig);

      expect(result.ssl).toBe(false); // User override respected
    });

    it('should preserve other user config properties', () => {
      process.env.KAFKA_SASL_MECHANISM = 'plain';
      process.env.KAFKA_SASL_USERNAME = 'user';
      process.env.KAFKA_SASL_PASSWORD = 'pass';

      const userConfig: KafkaConfig = {
        brokers: ['localhost:9092'],
        clientId: 'test-client',
        connectionTimeout: 5000,
        requestTimeout: 3000,
      };

      const result = mergeWithEnvironmentConfig(userConfig);

      expect(result.clientId).toBe('test-client');
      expect(result.connectionTimeout).toBe(5000);
      expect(result.requestTimeout).toBe(3000);
    });
  });

  describe('Real-world scenarios', () => {
    it('should handle Confluent Cloud pattern', () => {
      process.env.KAFKA_SASL_MECHANISM = 'plain';
      process.env.KAFKA_SASL_USERNAME = 'confluent-api-key';
      process.env.KAFKA_SASL_PASSWORD = 'confluent-api-secret';

      const userConfig: KafkaConfig = {
        brokers: ['pkc-xxxxx.us-east-1.aws.confluent.cloud:9092'],
      };

      const result = mergeWithEnvironmentConfig(userConfig);

      expect(result.ssl).toBe(true);
      expect(result.sasl?.mechanism).toBe('plain');
    });

    it('should handle Redpanda Cloud pattern', () => {
      process.env.KAFKA_SASL_MECHANISM = 'scram-sha-256';
      process.env.KAFKA_SASL_USERNAME = 'redpanda-user';
      process.env.KAFKA_SASL_PASSWORD = 'redpanda-password';

      const userConfig: KafkaConfig = {
        brokers: ['seed-xxxxx.redpanda.cloud:9092'],
      };

      const result = mergeWithEnvironmentConfig(userConfig);

      expect(result.ssl).toBe(true);
      expect(result.sasl?.mechanism).toBe('scram-sha-256');
    });

    it('should handle AWS MSK with IAM auth (no env SASL)', () => {
      const userConfig: KafkaConfig = {
        brokers: ['b-1.mycluster.xxx.kafka.amazonaws.com:9094'],
        ssl: true,
        sasl: {
          mechanism: 'aws' as any,
          authorizationIdentity: 'xxx',
        } as any,
      };

      const result = mergeWithEnvironmentConfig(userConfig);

      expect(result.sasl?.mechanism).toBe('aws');
      expect(result.ssl).toBe(true);
    });

    it('should handle local development (no SSL, no SASL)', () => {
      const userConfig: KafkaConfig = {
        brokers: ['localhost:9092'],
      };

      const result = mergeWithEnvironmentConfig(userConfig);

      expect(result.ssl).toBeUndefined();
      expect(result.sasl).toBeUndefined();
    });
  });
});
