import { KafkaConfig } from 'kafkajs';
import {
  readSaslConfigFromEnv,
  applyConfigurationSmartDefaults,
  mergeWithEnvironmentConfig,
  toNumber,
  toBoolean,
  coerceKafkaModuleOptions,
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

  describe('toNumber', () => {
    it('passes through finite numbers', () => {
      expect(toNumber(3000)).toBe(3000);
      expect(toNumber(0)).toBe(0);
      expect(toNumber(-1)).toBe(-1);
    });

    it('parses numeric strings', () => {
      expect(toNumber('3000')).toBe(3000);
      expect(toNumber('  42 ')).toBe(42);
      expect(toNumber('3.14')).toBe(3.14);
    });

    it('returns fallback for null/undefined', () => {
      expect(toNumber(null, 500)).toBe(500);
      expect(toNumber(undefined, 500)).toBe(500);
      expect(toNumber(null)).toBeUndefined();
    });

    it('returns fallback for non-parseable strings', () => {
      expect(toNumber('abc', 500)).toBe(500);
      expect(toNumber('', 500)).toBe(500);
      expect(toNumber('   ', 500)).toBe(500);
    });

    it('returns fallback for non-finite numbers', () => {
      expect(toNumber(NaN, 500)).toBe(500);
      expect(toNumber(Infinity, 500)).toBe(500);
    });

    it('returns fallback for unsupported types', () => {
      expect(toNumber(true, 500)).toBe(500);
      expect(toNumber({}, 500)).toBe(500);
      expect(toNumber([], 500)).toBe(500);
    });
  });

  describe('toBoolean', () => {
    it('passes through booleans', () => {
      expect(toBoolean(true)).toBe(true);
      expect(toBoolean(false)).toBe(false);
    });

    it('recognizes truthy strings', () => {
      expect(toBoolean('true')).toBe(true);
      expect(toBoolean('TRUE')).toBe(true);
      expect(toBoolean('1')).toBe(true);
      expect(toBoolean('yes')).toBe(true);
      expect(toBoolean(' Yes ')).toBe(true);
    });

    it('recognizes falsy strings', () => {
      expect(toBoolean('false')).toBe(false);
      expect(toBoolean('FALSE')).toBe(false);
      expect(toBoolean('0')).toBe(false);
      expect(toBoolean('no')).toBe(false);
    });

    it('returns fallback for null/undefined', () => {
      expect(toBoolean(null, true)).toBe(true);
      expect(toBoolean(undefined, false)).toBe(false);
      expect(toBoolean(null)).toBeUndefined();
    });

    it('returns fallback for unrecognized strings', () => {
      expect(toBoolean('maybe', true)).toBe(true);
      expect(toBoolean('', false)).toBe(false);
    });
  });

  describe('coerceKafkaModuleOptions', () => {
    it('returns empty options unchanged', () => {
      expect(coerceKafkaModuleOptions({})).toEqual({});
      expect(coerceKafkaModuleOptions()).toEqual({});
    });

    it('does not mutate the input object', () => {
      const input = {
        client: { brokers: ['localhost:9092'], connectionTimeout: '3000' as any },
        retry: { enabled: 'true' as any, attempts: '5' as any },
      };
      const result = coerceKafkaModuleOptions(input);

      expect(result).not.toBe(input);
      expect(result.client).not.toBe(input.client);
      expect(result.retry).not.toBe(input.retry);
      expect(input.client.connectionTimeout).toBe('3000');
      expect(input.retry.enabled).toBe('true');
    });

    it('coerces client numeric fields from strings', () => {
      const result = coerceKafkaModuleOptions({
        client: {
          brokers: ['localhost:9092'],
          connectionTimeout: '3000' as any,
          requestTimeout: '30000' as any,
        },
      });

      expect(result.client?.connectionTimeout).toBe(3000);
      expect(result.client?.requestTimeout).toBe(30000);
      expect(result.client?.brokers).toEqual(['localhost:9092']);
    });

    it('coerces consumer numeric + boolean fields', () => {
      const result = coerceKafkaModuleOptions({
        consumer: {
          groupId: 'g',
          sessionTimeout: '30000' as any,
          heartbeatInterval: '3000' as any,
          maxWaitTimeInMs: '100' as any,
          allowAutoTopicCreation: 'true' as any,
        },
      });

      expect(result.consumer?.sessionTimeout).toBe(30000);
      expect(result.consumer?.heartbeatInterval).toBe(3000);
      expect(result.consumer?.maxWaitTimeInMs).toBe(100);
      expect(result.consumer?.allowAutoTopicCreation).toBe(true);
      expect(result.consumer?.groupId).toBe('g');
    });

    it('coerces producer fields', () => {
      const result = coerceKafkaModuleOptions({
        producer: {
          maxInFlightRequests: '1' as any,
          idempotent: 'true' as any,
          transactionTimeout: '30000' as any,
        },
      });

      expect(result.producer?.maxInFlightRequests).toBe(1);
      expect(result.producer?.idempotent).toBe(true);
      expect(result.producer?.transactionTimeout).toBe(30000);
    });

    it('coerces retry fields', () => {
      const result = coerceKafkaModuleOptions({
        retry: {
          enabled: 'true' as any,
          attempts: '3' as any,
          baseDelay: '2000' as any,
          maxDelay: '30000' as any,
          topicPartitions: '3' as any,
          topicReplicationFactor: '1' as any,
          topicRetentionMs: '86400000' as any,
          backoff: 'exponential',
        },
      });

      expect(result.retry?.enabled).toBe(true);
      expect(result.retry?.attempts).toBe(3);
      expect(result.retry?.baseDelay).toBe(2000);
      expect(result.retry?.maxDelay).toBe(30000);
      expect(result.retry?.topicPartitions).toBe(3);
      expect(result.retry?.topicReplicationFactor).toBe(1);
      expect(result.retry?.topicRetentionMs).toBe(86400000);
      expect(result.retry?.backoff).toBe('exponential');
    });

    it('coerces dlq fields including nested reprocessingOptions', () => {
      const result = coerceKafkaModuleOptions({
        dlq: {
          enabled: 'true' as any,
          topicPartitions: '3' as any,
          topicReplicationFactor: '1' as any,
          topicRetentionMs: '604800000' as any,
          reprocessingOptions: {
            batchSize: '100' as any,
            timeoutMs: '30000' as any,
            stopOnError: 'false' as any,
          },
        },
      });

      expect(result.dlq?.enabled).toBe(true);
      expect(result.dlq?.topicPartitions).toBe(3);
      expect(result.dlq?.reprocessingOptions?.batchSize).toBe(100);
      expect(result.dlq?.reprocessingOptions?.timeoutMs).toBe(30000);
      expect(result.dlq?.reprocessingOptions?.stopOnError).toBe(false);
    });

    it('coerces subscriptions.fromBeginning', () => {
      const result = coerceKafkaModuleOptions({
        subscriptions: { topics: ['t1'], fromBeginning: 'true' as any },
      });

      expect(result.subscriptions?.fromBeginning).toBe(true);
      expect(result.subscriptions?.topics).toEqual(['t1']);
    });

    it('coerces monitoring.enabled', () => {
      const result = coerceKafkaModuleOptions({
        monitoring: { enabled: 'false' as any },
      });

      expect(result.monitoring?.enabled).toBe(false);
    });

    it('coerces health numeric fields', () => {
      const result = coerceKafkaModuleOptions({
        health: {
          startupGracePeriodMs: '180000' as any,
          rebalanceGracePeriodMs: '120000' as any,
          staleThresholdMs: '600000' as any,
        },
      });

      expect(result.health?.startupGracePeriodMs).toBe(180000);
      expect(result.health?.rebalanceGracePeriodMs).toBe(120000);
      expect(result.health?.staleThresholdMs).toBe(600000);
    });

    it('leaves already-typed values untouched', () => {
      const result = coerceKafkaModuleOptions({
        client: { brokers: ['localhost:9092'], connectionTimeout: 3000 },
        retry: { enabled: true, attempts: 3 },
      });

      expect(result.client?.connectionTimeout).toBe(3000);
      expect(result.retry?.enabled).toBe(true);
      expect(result.retry?.attempts).toBe(3);
    });

    it('drops unparseable string numbers rather than crashing', () => {
      const result = coerceKafkaModuleOptions({
        client: { brokers: ['localhost:9092'], connectionTimeout: 'not-a-number' as any },
      });

      expect(result.client?.connectionTimeout).toBeUndefined();
    });

    it('coerces ssl string but preserves object form', () => {
      const resultString = coerceKafkaModuleOptions({
        client: { brokers: ['localhost:9092'], ssl: 'true' as any },
      });
      expect(resultString.client?.ssl).toBe(true);

      const tlsOpts = { rejectUnauthorized: false };
      const resultObject = coerceKafkaModuleOptions({
        client: { brokers: ['localhost:9092'], ssl: tlsOpts },
      });
      expect(resultObject.client?.ssl).toBe(tlsOpts);
    });

    it('simulates a full NestJS ConfigService-derived options bag', () => {
      // Every field is a string — this is exactly what hits the module
      // when consumers wire `configService.get<number>('KAFKA_...')` directly.
      const result = coerceKafkaModuleOptions({
        client: {
          clientId: 'test',
          brokers: ['localhost:9092'],
          connectionTimeout: '3000' as any,
          requestTimeout: '30000' as any,
        },
        consumer: {
          groupId: 'g',
          sessionTimeout: '30000' as any,
          heartbeatInterval: '3000' as any,
          maxWaitTimeInMs: '100' as any,
          allowAutoTopicCreation: 'true' as any,
        },
        producer: {
          maxInFlightRequests: '1' as any,
          idempotent: 'true' as any,
          transactionTimeout: '30000' as any,
        },
        retry: {
          enabled: 'true' as any,
          attempts: '3' as any,
          baseDelay: '2000' as any,
          maxDelay: '30000' as any,
        },
        dlq: {
          enabled: 'true' as any,
          topicPartitions: '3' as any,
        },
        subscriptions: { topics: [], fromBeginning: 'false' as any },
        monitoring: { enabled: 'true' as any },
      });

      expect(typeof result.client?.connectionTimeout).toBe('number');
      expect(typeof result.consumer?.sessionTimeout).toBe('number');
      expect(typeof result.consumer?.allowAutoTopicCreation).toBe('boolean');
      expect(typeof result.producer?.idempotent).toBe('boolean');
      expect(typeof result.retry?.enabled).toBe('boolean');
      expect(typeof result.retry?.attempts).toBe('number');
      expect(typeof result.dlq?.enabled).toBe('boolean');
      expect(typeof result.subscriptions?.fromBeginning).toBe('boolean');
      expect(typeof result.monitoring?.enabled).toBe('boolean');
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
