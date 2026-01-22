import { Test, TestingModule } from '@nestjs/testing';
import { HealthIndicatorService } from '@nestjs/terminus';
import { KafkaHealthIndicator } from '../src/health/kafka-health.indicator';
import { KafkaProducerService } from '../src/core/kafka.producer';
import { KafkaConsumerService, ConsumerState } from '../src/services/kafka.consumer.service';
import { KafkaBootstrapService } from '../src/services/kafka.bootstrap.service';
import { KafkaHandlerRegistry } from '../src/services/kafka.registry';
import { KafkaRetryService } from '../src/services/kafka.retry.service';

describe('KafkaHealthIndicator', () => {
  let healthIndicator: KafkaHealthIndicator;
  let producerService: jest.Mocked<KafkaProducerService>;
  let consumerService: jest.Mocked<KafkaConsumerService>;
  let bootstrapService: jest.Mocked<KafkaBootstrapService>;
  let handlerRegistry: jest.Mocked<KafkaHandlerRegistry>;
  let retryService: jest.Mocked<KafkaRetryService>;

  beforeEach(async () => {
    // Create mock services
    const mockProducerService = {
      isReady: jest.fn(),
    };

    // Mock consumer service with new health state methods
    const mockConsumerService = {
      isConnected: false,
      getHealthState: jest.fn().mockReturnValue({
        state: ConsumerState.ACTIVE,
        isHealthy: true,
        reason: 'Active with 2 partition(s)',
      }),
      getAssignedPartitions: jest.fn().mockReturnValue([
        { topic: 'test-topic', partition: 0 },
        { topic: 'test-topic', partition: 1 },
      ]),
      getGroupInfo: jest.fn().mockReturnValue({
        groupId: 'test-consumer-group',
        memberId: 'test-member-id-12345',
        isLeader: true,
      }),
    };

    const mockBootstrapService = {
      isInitialized: false,
    };

    const mockHandlerRegistry = {
      getHandlerCount: jest.fn(),
    };

    const mockRetryService = {
      isRetryConsumerRunning: jest.fn(),
    };

    const mockHealthIndicatorService = {
      check: jest.fn().mockImplementation((key: string) => ({
        up: jest.fn((data) => ({ [key]: { status: 'healthy', ...data } })),
        down: jest.fn((data) => ({ [key]: { status: 'unhealthy', ...data } })),
      })),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        KafkaHealthIndicator,
        {
          provide: KafkaProducerService,
          useValue: mockProducerService,
        },
        {
          provide: KafkaConsumerService,
          useValue: mockConsumerService,
        },
        {
          provide: KafkaBootstrapService,
          useValue: mockBootstrapService,
        },
        {
          provide: KafkaHandlerRegistry,
          useValue: mockHandlerRegistry,
        },
        {
          provide: KafkaRetryService,
          useValue: mockRetryService,
        },
        {
          provide: HealthIndicatorService,
          useValue: mockHealthIndicatorService,
        },
      ],
    }).compile();

    healthIndicator = module.get<KafkaHealthIndicator>(KafkaHealthIndicator);
    producerService = module.get(KafkaProducerService);
    consumerService = module.get(KafkaConsumerService);
    bootstrapService = module.get(KafkaBootstrapService);
    handlerRegistry = module.get(KafkaHandlerRegistry);
    retryService = module.get(KafkaRetryService);
  });

  describe('checkHealth', () => {
    it('should return healthy status when broker is reachable and bootstrap is complete', async () => {
      // Arrange
      producerService.isReady.mockResolvedValue(true);
      (consumerService as any).isConnected = true;
      (bootstrapService as any).isInitialized = true;
      handlerRegistry.getHandlerCount.mockReturnValue(5);
      retryService.isRetryConsumerRunning.mockReturnValue(true);

      // Act
      const result = await healthIndicator.checkHealth();

      // Assert
      expect(result.status).toBe('healthy');
      expect(result.components.broker.healthy).toBe(true);
      expect(result.components.bootstrap.healthy).toBe(true);
      expect(result.components.producer.healthy).toBe(true);
      expect(result.components.consumer.healthy).toBe(true);
    });

    it('should return unhealthy status when consumer is down (consumer is now critical)', async () => {
      // Arrange - Broker is reachable via producer only, but consumer is down
      producerService.isReady.mockResolvedValue(true);
      (consumerService as any).isConnected = false;
      consumerService.getHealthState.mockReturnValue({
        state: ConsumerState.DISCONNECTED,
        isHealthy: false,
        reason: 'Consumer disconnected from broker',
      });
      consumerService.getAssignedPartitions.mockReturnValue([]);
      (bootstrapService as any).isInitialized = true;
      handlerRegistry.getHandlerCount.mockReturnValue(5);

      // Act
      const result = await healthIndicator.checkHealth();

      // Assert
      expect(result.status).toBe('unhealthy'); // Consumer health is now critical
      expect(result.components.broker.healthy).toBe(true);
      expect(result.components.producer.healthy).toBe(true);
      expect(result.components.consumer.healthy).toBe(false);
    });

    it('should return healthy status when producer is down but consumer is active', async () => {
      // Arrange - Broker is reachable via consumer only
      producerService.isReady.mockResolvedValue(false); // Producer down
      (consumerService as any).isConnected = true;
      consumerService.getHealthState.mockReturnValue({
        state: ConsumerState.ACTIVE,
        isHealthy: true,
        reason: 'Active with 2 partition(s)',
      });
      (bootstrapService as any).isInitialized = true;
      handlerRegistry.getHandlerCount.mockReturnValue(5);

      // Act
      const result = await healthIndicator.checkHealth();

      // Assert
      expect(result.status).toBe('healthy'); // Consumer is active, so healthy
      expect(result.components.broker.healthy).toBe(true); // Broker reachable via consumer
      expect(result.components.producer.healthy).toBe(false); // Producer down but non-critical
      expect(result.components.consumer.healthy).toBe(true);
    });

    it('should return unhealthy status when broker is unreachable', async () => {
      // Arrange - No connection to broker
      producerService.isReady.mockResolvedValue(false);
      (consumerService as any).isConnected = false;
      (bootstrapService as any).isInitialized = true;
      handlerRegistry.getHandlerCount.mockReturnValue(5);

      // Act
      const result = await healthIndicator.checkHealth();

      // Assert
      expect(result.status).toBe('unhealthy');
      expect(result.components.broker.healthy).toBe(false);
      expect(result.components.broker.message).toContain('Unable to connect to Kafka brokers');
    });

    it('should return unhealthy status when bootstrap is not complete', async () => {
      // Arrange
      producerService.isReady.mockResolvedValue(true);
      (consumerService as any).isConnected = true;
      consumerService.getHealthState.mockReturnValue({
        state: ConsumerState.ACTIVE,
        isHealthy: true,
        reason: 'Active with 2 partition(s)',
      });
      (bootstrapService as any).isInitialized = false; // Bootstrap not complete
      handlerRegistry.getHandlerCount.mockReturnValue(0);

      // Act
      const result = await healthIndicator.checkHealth();

      // Assert
      expect(result.status).toBe('unhealthy');
      expect(result.components.bootstrap.healthy).toBe(false);
      expect(result.components.bootstrap.message).toContain('Bootstrap initialization not complete');
    });

    it('should return unhealthy when both broker and bootstrap are down', async () => {
      // Arrange
      producerService.isReady.mockResolvedValue(false);
      (consumerService as any).isConnected = false;
      consumerService.getHealthState.mockReturnValue({
        state: ConsumerState.DISCONNECTED,
        isHealthy: false,
        reason: 'Consumer disconnected',
      });
      consumerService.getAssignedPartitions.mockReturnValue([]);
      (bootstrapService as any).isInitialized = false;
      handlerRegistry.getHandlerCount.mockReturnValue(0);

      // Act
      const result = await healthIndicator.checkHealth();

      // Assert
      expect(result.status).toBe('unhealthy');
      expect(result.components.broker.healthy).toBe(false);
      expect(result.components.bootstrap.healthy).toBe(false);
    });

    it('should include retry consumer status as non-critical', async () => {
      // Arrange
      producerService.isReady.mockResolvedValue(true);
      (consumerService as any).isConnected = true;
      consumerService.getHealthState.mockReturnValue({
        state: ConsumerState.ACTIVE,
        isHealthy: true,
        reason: 'Active with 2 partition(s)',
      });
      (bootstrapService as any).isInitialized = true;
      handlerRegistry.getHandlerCount.mockReturnValue(5);
      retryService.isRetryConsumerRunning.mockReturnValue(false); // Retry consumer down

      // Act
      const result = await healthIndicator.checkHealth();

      // Assert
      expect(result.status).toBe('healthy'); // Still healthy despite retry consumer down
      expect(result.components.retry?.healthy).toBe(true); // Retry is always non-critical
      expect(result.components.retry?.message).toContain('Retry consumer not running');
      expect(result.components.retry?.details?.note).toContain('does not affect overall health');
    });

    it('should include handler count in components', async () => {
      // Arrange
      producerService.isReady.mockResolvedValue(true);
      (consumerService as any).isConnected = true;
      consumerService.getHealthState.mockReturnValue({
        state: ConsumerState.ACTIVE,
        isHealthy: true,
        reason: 'Active with 2 partition(s)',
      });
      (bootstrapService as any).isInitialized = true;
      handlerRegistry.getHandlerCount.mockReturnValue(10);

      // Act
      const result = await healthIndicator.checkHealth();

      // Assert
      expect(result.components.handlers.healthy).toBe(true);
      expect(result.components.handlers.message).toContain('10 handler(s) registered');
      expect(result.components.handlers.details?.count).toBe(10);
    });

    it('should include timestamp in health result', async () => {
      // Arrange
      producerService.isReady.mockResolvedValue(true);
      (consumerService as any).isConnected = true;
      consumerService.getHealthState.mockReturnValue({
        state: ConsumerState.ACTIVE,
        isHealthy: true,
        reason: 'Active with 2 partition(s)',
      });
      (bootstrapService as any).isInitialized = true;
      handlerRegistry.getHandlerCount.mockReturnValue(5);

      // Act
      const result = await healthIndicator.checkHealth();

      // Assert
      expect(result.timestamp).toBeDefined();
      expect(new Date(result.timestamp).getTime()).toBeGreaterThan(0);
    });
  });

  describe('isHealthy (Terminus integration)', () => {
    it('should return Terminus-compatible result when healthy', async () => {
      // Arrange
      producerService.isReady.mockResolvedValue(true);
      (consumerService as any).isConnected = true;
      (bootstrapService as any).isInitialized = true;
      handlerRegistry.getHandlerCount.mockReturnValue(5);

      // Act
      const result = await healthIndicator.isHealthy('kafka');

      // Assert
      expect(result).toHaveProperty('kafka');
      expect(result.kafka.status).toBe('healthy');
      expect(result.kafka).toHaveProperty('broker');
      expect(result.kafka).toHaveProperty('bootstrap');
      expect(result.kafka).toHaveProperty('producer');
      expect(result.kafka).toHaveProperty('consumer');
    });

    it('should return Terminus-compatible result when unhealthy', async () => {
      // Arrange
      producerService.isReady.mockResolvedValue(false);
      (consumerService as any).isConnected = false;
      (bootstrapService as any).isInitialized = false;
      handlerRegistry.getHandlerCount.mockReturnValue(0);

      // Act
      const result = await healthIndicator.isHealthy('kafka');

      // Assert
      expect(result).toHaveProperty('kafka');
      expect(result.kafka.status).toBe('unhealthy');
    });

    it('should use custom key provided by caller', async () => {
      // Arrange
      producerService.isReady.mockResolvedValue(true);
      (consumerService as any).isConnected = true;
      (bootstrapService as any).isInitialized = true;
      handlerRegistry.getHandlerCount.mockReturnValue(5);

      // Act
      const result = await healthIndicator.isHealthy('custom-kafka-check');

      // Assert
      expect(result).toHaveProperty('custom-kafka-check');
      expect(result['custom-kafka-check'].status).toBe('healthy');
    });
  });

  describe('Partition assignment scenarios', () => {
    it('should remain healthy when consumer connected but no partitions (scaled beyond partition count)', async () => {
      // Arrange - This simulates the user's main concern:
      // Consumer is connected to Kafka but doesn't have partitions because
      // the consumer group size exceeds the number of topic partitions
      producerService.isReady.mockResolvedValue(true);
      (consumerService as any).isConnected = true; // Consumer connected
      // Mock the health state to show connected but waiting for partitions
      consumerService.getHealthState.mockReturnValue({
        state: ConsumerState.CONNECTED,
        isHealthy: true,
        reason: 'Connected, waiting for partition assignment',
      });
      consumerService.getAssignedPartitions.mockReturnValue([]); // No partitions
      (bootstrapService as any).isInitialized = true;
      handlerRegistry.getHandlerCount.mockReturnValue(5);

      // Act
      const result = await healthIndicator.checkHealth();

      // Assert
      expect(result.status).toBe('healthy'); // KEY REQUIREMENT: Still healthy!
      expect(result.components.broker.healthy).toBe(true);
      expect(result.components.consumer.healthy).toBe(true);
      expect(result.components.consumer.message).toContain('waiting for partition assignment');
    });
  });

  describe('Consumer group info and topic details', () => {
    it('should include consumer group info when available', async () => {
      // Arrange
      producerService.isReady.mockResolvedValue(true);
      (consumerService as any).isConnected = true;
      consumerService.getHealthState.mockReturnValue({
        state: ConsumerState.ACTIVE,
        isHealthy: true,
        reason: 'Active with 3 partition(s)',
      });
      consumerService.getAssignedPartitions.mockReturnValue([
        { topic: 'mux.video.events', partition: 0 },
        { topic: 'video.asset.ready', partition: 0 },
        { topic: 'video.asset.ready', partition: 1 },
      ]);
      consumerService.getGroupInfo.mockReturnValue({
        groupId: 'my-service-group',
        memberId: 'my-service-abc123',
        isLeader: true,
      });
      (bootstrapService as any).isInitialized = true;
      handlerRegistry.getHandlerCount.mockReturnValue(5);

      // Act
      const result = await healthIndicator.checkHealth();

      // Assert
      expect(result.status).toBe('healthy');
      expect(result.components.consumer.details?.groupId).toBe('my-service-group');
      expect(result.components.consumer.details?.memberId).toBe('my-service-abc123');
      expect(result.components.consumer.details?.isLeader).toBe(true);
    });

    it('should group partitions by topic for clearer output', async () => {
      // Arrange
      producerService.isReady.mockResolvedValue(true);
      (consumerService as any).isConnected = true;
      consumerService.getHealthState.mockReturnValue({
        state: ConsumerState.ACTIVE,
        isHealthy: true,
        reason: 'Active with 5 partition(s)',
      });
      consumerService.getAssignedPartitions.mockReturnValue([
        { topic: 'mux.video.events', partition: 0 },
        { topic: 'video.asset.ready', partition: 2 },
        { topic: 'video.asset.ready', partition: 0 },
        { topic: 'video.asset.ready', partition: 1 },
        { topic: 'video.upload.requested', partition: 0 },
      ]);
      (bootstrapService as any).isInitialized = true;
      handlerRegistry.getHandlerCount.mockReturnValue(5);

      // Act
      const result = await healthIndicator.checkHealth();

      // Assert
      expect(result.components.consumer.details?.topics).toEqual({
        'mux.video.events': [0],
        'video.asset.ready': [0, 1, 2], // Should be sorted
        'video.upload.requested': [0],
      });
      // Also keep original flat format for backwards compatibility
      expect(result.components.consumer.details?.partitions).toHaveLength(5);
    });

    it('should handle null group info when consumer has not joined group yet', async () => {
      // Arrange
      producerService.isReady.mockResolvedValue(true);
      (consumerService as any).isConnected = true;
      consumerService.getHealthState.mockReturnValue({
        state: ConsumerState.CONNECTED,
        isHealthy: true,
        reason: 'Connected, waiting for partition assignment',
      });
      consumerService.getAssignedPartitions.mockReturnValue([]);
      consumerService.getGroupInfo.mockReturnValue(null); // Not joined yet
      (bootstrapService as any).isInitialized = true;
      handlerRegistry.getHandlerCount.mockReturnValue(5);

      // Act
      const result = await healthIndicator.checkHealth();

      // Assert
      expect(result.status).toBe('healthy');
      expect(result.components.consumer.details?.groupId).toBeUndefined();
      expect(result.components.consumer.details?.memberId).toBeUndefined();
      expect(result.components.consumer.details?.isLeader).toBeUndefined();
    });
  });

  describe('Error handling', () => {
    it('should handle errors in broker connection check gracefully', async () => {
      // Arrange
      producerService.isReady.mockRejectedValue(new Error('Connection timeout'));
      (consumerService as any).isConnected = false;
      (bootstrapService as any).isInitialized = true;

      // Act
      const result = await healthIndicator.checkHealth();

      // Assert
      expect(result.status).toBe('unhealthy');
      expect(result.components.broker.healthy).toBe(false);
      expect(result.components.broker.message).toContain('Connection timeout');
    });

    it('should handle missing services gracefully', async () => {
      // Arrange - Create health indicator without some services
      const indicatorWithoutServices = new KafkaHealthIndicator(
        null as any,
        null as any,
        null as any,
        null as any,
        null as any,
        { getComponent: jest.fn() } as any,
      );

      // Act
      const result = await indicatorWithoutServices.checkHealth();

      // Assert
      expect(result.status).toBe('unhealthy');
      expect(result.components.broker.healthy).toBe(false);
      expect(result.components.bootstrap.healthy).toBe(false);
    });
  });
});
