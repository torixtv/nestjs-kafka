import { Test, TestingModule } from '@nestjs/testing';
import { KafkaModule } from './kafka.module';
import { KafkaProducerService } from './kafka.producer';
import { RetryInterceptor } from '../interceptors/retry.interceptor';
import { KAFKA_MODULE_OPTIONS, KAFKAJS_INSTANCE, KAFKA_PLUGINS } from './kafka.constants';
import { KafkaModuleOptions } from '../interfaces/kafka.interfaces';
import { KafkaRetryConsumer } from '../services/kafka.retry-consumer';

describe('KafkaModule', () => {
  describe('forRoot', () => {
    it('should create module with default options', async () => {
      const module = KafkaModule.forRoot();

      expect(module.module).toBe(KafkaModule);
      expect(module.providers).toBeDefined();
      expect(module.exports).toBeDefined();
    });

    it('should create module with custom options', async () => {
      const options: KafkaModuleOptions = {
        client: {
          clientId: 'test-client',
          brokers: ['localhost:9092'],
        },
        consumer: {
          groupId: 'test-group',
        },
        retry: {
          enabled: true,
          attempts: 5,
        },
      };

      const module = KafkaModule.forRoot(options);

      expect(module.module).toBe(KafkaModule);
      expect(module.providers).toBeDefined();
    });

    it('should merge options with defaults correctly', async () => {
      const options: KafkaModuleOptions = {
        client: {
          clientId: 'custom-client',
          brokers: ['custom:9092'],
        },
      };

      const testModule = await Test.createTestingModule({
        imports: [KafkaModule.forRoot(options)],
      }).compile();

      const moduleOptions = testModule.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS);

      expect(moduleOptions.client?.clientId).toBe('custom-client');
      expect(moduleOptions.client?.brokers).toEqual(['custom:9092']);
      expect(moduleOptions.consumer?.groupId).toBe('kafka-consumer-group'); // default
      expect(moduleOptions.retry?.enabled).toBe(false); // default
    });
  });

  describe('forRootAsync', () => {
    it('should create module with useFactory', async () => {
      const module = KafkaModule.forRootAsync({
        useFactory: () => ({
          client: {
            clientId: 'async-test',
            brokers: ['localhost:9092'],
          },
        }),
      });

      expect(module.module).toBe(KafkaModule);
      expect(module.providers).toBeDefined();
    });

    it('should create module with useClass', async () => {
      class TestConfigService {
        createKafkaModuleOptions(): KafkaModuleOptions {
          return {
            client: {
              clientId: 'class-test',
              brokers: ['localhost:9092'],
            },
          };
        }
      }

      const module = KafkaModule.forRootAsync({
        useClass: TestConfigService,
      });

      expect(module.module).toBe(KafkaModule);
      expect(module.providers).toBeDefined();
    });

    it('should create module with useExisting', async () => {
      class ExistingConfigService {
        createKafkaModuleOptions(): KafkaModuleOptions {
          return {
            client: {
              clientId: 'existing-test',
              brokers: ['localhost:9092'],
            },
          };
        }
      }

      const module = KafkaModule.forRootAsync({
        useExisting: ExistingConfigService,
      });

      expect(module.module).toBe(KafkaModule);
      expect(module.providers).toBeDefined();
    });
  });

  describe('module compilation', () => {
    let module: TestingModule;

    beforeEach(async () => {
      module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            client: {
              clientId: 'test-client',
              brokers: ['localhost:9092'],
            },
            consumer: {
              groupId: 'test-group',
            },
          }),
        ],
      }).compile();
    });

    afterEach(async () => {
      await module.close();
    });

    it('should provide KafkaProducerService', () => {
      const producer = module.get<KafkaProducerService>(KafkaProducerService);
      expect(producer).toBeInstanceOf(KafkaProducerService);
    });

    it('should provide RetryInterceptor', () => {
      const interceptor = module.get<RetryInterceptor>(RetryInterceptor);
      expect(interceptor).toBeInstanceOf(RetryInterceptor);
    });

    it('should provide KAFKA_MODULE_OPTIONS', () => {
      const options = module.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS);
      expect(options).toBeDefined();
      expect(options.client?.clientId).toBe('test-client');
    });

    it('should provide KAFKAJS_INSTANCE', () => {
      const kafka = module.get(KAFKAJS_INSTANCE);
      expect(kafka).toBeDefined();
      expect(typeof kafka.producer).toBe('function');
    });

    it('should provide KAFKA_PLUGINS as empty array by default', () => {
      const plugins = module.get(KAFKA_PLUGINS);
      expect(plugins).toEqual([]);
    });
  });

  describe('options merging', () => {
    it('should handle undefined client config', async () => {
      const module = await Test.createTestingModule({
        imports: [KafkaModule.forRoot({})],
      }).compile();

      const options = module.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS);
      expect(options.client?.clientId).toBe('kafka-client');
      expect(options.client?.brokers).toEqual(['localhost:9092']);

      await module.close();
    });

    it('should handle partial client config', async () => {
      const module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            client: {
              clientId: 'partial-client',
              brokers: ['localhost:9092'],
              logLevel: 'error' as any,
            },
          }),
        ],
      }).compile();

      const options = module.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS);
      expect(options.client?.clientId).toBe('partial-client');
      expect(options.client?.logLevel).toBe('error');

      await module.close();
    });

    it('should handle plugins configuration', async () => {
      const testPlugin = { name: 'test-plugin' };

      const module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            client: {
              clientId: 'plugin-test',
              brokers: ['localhost:9092'],
            },
            plugins: [testPlugin],
          }),
        ],
      }).compile();

      const plugins = module.get(KAFKA_PLUGINS);
      expect(plugins).toEqual([testPlugin]);

      await module.close();
    });

    it('should merge retry configuration', async () => {
      const module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            client: {
              clientId: 'retry-test',
              brokers: ['localhost:9092'],
            },
            retry: {
              enabled: true,
              attempts: 5,
              maxDelay: 60000,
            },
          }),
        ],
      }).compile();

      const options = module.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS);
      expect(options.retry?.enabled).toBe(true);
      expect(options.retry?.attempts).toBe(5);
      expect(options.retry?.maxDelay).toBe(60000);
      expect(options.retry?.backoff).toBe('exponential'); // default
      expect(options.retry?.baseDelay).toBe(1000); // default

      await module.close();
    });
  });

  describe('retry consumer initialization', () => {
    it('should initialize retry consumer when retry is enabled', async () => {
      const consoleLogSpy = jest.spyOn(console, 'log').mockImplementation();
      const module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            client: {
              clientId: 'retry-init-test',
              brokers: ['localhost:9092'],
            },
            retry: {
              enabled: true,
              attempts: 3,
            },
          }),
        ],
      }).compile();

      // Initialize the application to trigger lifecycle hooks
      const app = module.createNestApplication();
      await app.init();

      // Get the retry consumer instance
      const retryConsumer = module.get<KafkaRetryConsumer>(KafkaRetryConsumer);
      expect(retryConsumer).toBeInstanceOf(KafkaRetryConsumer);

      // The bootstrap service should have logged the retry consumer status
      expect(consoleLogSpy).toHaveBeenCalledWith(
        expect.stringContaining('[KafkaBootstrap] Retry consumer running status:')
      );

      consoleLogSpy.mockRestore();
      await app.close();
    });

    it('should not initialize retry consumer when retry is disabled', async () => {
      const consoleLogSpy = jest.spyOn(console, 'log').mockImplementation();
      const module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            client: {
              clientId: 'retry-disabled-test',
              brokers: ['localhost:9092'],
            },
            retry: {
              enabled: false,
            },
          }),
        ],
      }).compile();

      // Initialize the application to trigger lifecycle hooks
      const app = module.createNestApplication();
      await app.init();

      // Get the retry consumer instance
      const retryConsumer = module.get<KafkaRetryConsumer>(KafkaRetryConsumer);
      expect(retryConsumer).toBeInstanceOf(KafkaRetryConsumer);

      // Should not have running status log since retry is disabled
      expect(consoleLogSpy).not.toHaveBeenCalledWith(
        expect.stringContaining('[KafkaBootstrap] Retry consumer running status: true')
      );

      consoleLogSpy.mockRestore();
      await app.close();
    });

    it('should provide KAFKA_EAGER_INIT token', async () => {
      const module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({
            client: {
              clientId: 'eager-init-test',
              brokers: ['localhost:9092'],
            },
          }),
        ],
      }).compile();

      // Get the eager init token - this should exist and trigger initialization
      const eagerInit = module.get('KAFKA_EAGER_INIT');
      expect(eagerInit).toEqual({ initialized: true });

      await module.close();
    });
  });
});