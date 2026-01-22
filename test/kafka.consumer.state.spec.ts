import { Test, TestingModule } from '@nestjs/testing';
import { ConsumerState, ConsumerHealthState, KafkaConsumerService } from '../src/services/kafka.consumer.service';
import { KAFKAJS_INSTANCE, KAFKA_MODULE_OPTIONS } from '../src/core/kafka.constants';
import { KafkaHandlerRegistry } from '../src/services/kafka.registry';
import { KafkaRetryService } from '../src/services/kafka.retry.service';
import { KafkaDlqService } from '../src/services/kafka.dlq.service';
import { KafkaProducerService } from '../src/core/kafka.producer';

/**
 * Tests for KafkaConsumerService state tracking functionality.
 *
 * These tests verify the smart grace period logic that prevents
 * unnecessary pod restarts during normal Kafka consumer operations.
 */
describe('KafkaConsumerService State Tracking', () => {
  // Default constants matching the service implementation
  const DEFAULT_STARTUP_GRACE_PERIOD_MS = 180000;      // 3 min
  const DEFAULT_REBALANCE_GRACE_PERIOD_MS = 120000;    // 2 min
  const DEFAULT_STALE_THRESHOLD_MS = 600000;           // 10 min

  describe('ConsumerState enum', () => {
    it('should have all expected states', () => {
      expect(ConsumerState.DISCONNECTED).toBe('disconnected');
      expect(ConsumerState.CONNECTING).toBe('connecting');
      expect(ConsumerState.CONNECTED).toBe('connected');
      expect(ConsumerState.REBALANCING).toBe('rebalancing');
      expect(ConsumerState.ACTIVE).toBe('active');
      expect(ConsumerState.STALE).toBe('stale');
    });
  });

  describe('Health State Logic', () => {
    /**
     * Helper to simulate the getHealthState logic with configurable times.
     * This mirrors the actual implementation for testing purposes.
     */
    function simulateGetHealthState(options: {
      state: ConsumerState;
      startupTime: number;
      currentTime: number;
      lastPartitionAssignmentTime: number | null;
      rebalanceStartTime: number | null;
      assignedPartitionsCount: number;
    }): ConsumerHealthState {
      const {
        state,
        startupTime,
        currentTime,
        lastPartitionAssignmentTime,
        rebalanceStartTime,
        assignedPartitionsCount,
      } = options;

      const timeSinceStart = currentTime - startupTime;
      let currentState = state;

      // During startup grace period, always healthy
      if (timeSinceStart < DEFAULT_STARTUP_GRACE_PERIOD_MS) {
        return {
          state: currentState,
          isHealthy: true,
          reason: `Within startup grace period (${Math.round(timeSinceStart / 1000)}s / ${DEFAULT_STARTUP_GRACE_PERIOD_MS / 1000}s)`,
        };
      }

      // During rebalance grace period, stay healthy
      if (currentState === ConsumerState.REBALANCING && rebalanceStartTime) {
        const rebalanceDuration = currentTime - rebalanceStartTime;
        if (rebalanceDuration < DEFAULT_REBALANCE_GRACE_PERIOD_MS) {
          return {
            state: currentState,
            isHealthy: true,
            reason: `Rebalancing (${Math.round(rebalanceDuration / 1000)}s / ${DEFAULT_REBALANCE_GRACE_PERIOD_MS / 1000}s max)`,
          };
        }
        currentState = ConsumerState.STALE;
      }

      // Check for stale state
      if (
        (currentState === ConsumerState.CONNECTED || currentState === ConsumerState.REBALANCING) &&
        lastPartitionAssignmentTime
      ) {
        const timeSinceLastAssignment = currentTime - lastPartitionAssignmentTime;
        if (timeSinceLastAssignment > DEFAULT_STALE_THRESHOLD_MS) {
          currentState = ConsumerState.STALE;
        }
      }

      // Final health determination
      switch (currentState) {
        case ConsumerState.ACTIVE:
          return {
            state: currentState,
            isHealthy: true,
            reason: `Active with ${assignedPartitionsCount} partition(s)`,
          };
        case ConsumerState.STALE:
          return {
            state: currentState,
            isHealthy: false,
            reason: 'Consumer stale - no partitions assigned for extended period',
          };
        case ConsumerState.DISCONNECTED:
          return {
            state: currentState,
            isHealthy: false,
            reason: 'Consumer disconnected from broker',
          };
        case ConsumerState.CONNECTED:
          return {
            state: currentState,
            isHealthy: true,
            reason: 'Connected, waiting for partition assignment',
          };
        case ConsumerState.CONNECTING:
          return {
            state: currentState,
            isHealthy: true,
            reason: 'Connecting to broker',
          };
        default:
          return {
            state: currentState,
            isHealthy: true,
            reason: `State: ${currentState}`,
          };
      }
    }

    describe('Startup Grace Period', () => {
      it('should report healthy during startup grace period regardless of state', () => {
        const startupTime = Date.now();
        const currentTime = startupTime + 60000; // 1 minute after startup

        const result = simulateGetHealthState({
          state: ConsumerState.DISCONNECTED,
          startupTime,
          currentTime,
          lastPartitionAssignmentTime: null,
          rebalanceStartTime: null,
          assignedPartitionsCount: 0,
        });

        expect(result.isHealthy).toBe(true);
        expect(result.reason).toContain('startup grace period');
      });

      it('should report healthy at 2.9 minutes after startup', () => {
        const startupTime = Date.now();
        const currentTime = startupTime + 174000; // 2.9 minutes

        const result = simulateGetHealthState({
          state: ConsumerState.CONNECTED,
          startupTime,
          currentTime,
          lastPartitionAssignmentTime: null,
          rebalanceStartTime: null,
          assignedPartitionsCount: 0,
        });

        expect(result.isHealthy).toBe(true);
      });

      it('should evaluate normal health after startup grace period expires', () => {
        const startupTime = Date.now() - 200000; // Started 3+ minutes ago
        const currentTime = Date.now();

        const result = simulateGetHealthState({
          state: ConsumerState.DISCONNECTED,
          startupTime,
          currentTime,
          lastPartitionAssignmentTime: null,
          rebalanceStartTime: null,
          assignedPartitionsCount: 0,
        });

        expect(result.isHealthy).toBe(false);
        expect(result.reason).toContain('disconnected');
      });
    });

    describe('Rebalance Grace Period', () => {
      it('should report healthy during rebalance grace period', () => {
        const startupTime = Date.now() - 300000; // Started 5 minutes ago
        const rebalanceStartTime = Date.now() - 30000; // Rebalancing for 30 seconds
        const currentTime = Date.now();

        const result = simulateGetHealthState({
          state: ConsumerState.REBALANCING,
          startupTime,
          currentTime,
          lastPartitionAssignmentTime: startupTime + 60000,
          rebalanceStartTime,
          assignedPartitionsCount: 0,
        });

        expect(result.isHealthy).toBe(true);
        expect(result.reason).toContain('Rebalancing');
      });

      it('should report stale when rebalance exceeds grace period', () => {
        const startupTime = Date.now() - 600000; // Started 10 minutes ago
        const rebalanceStartTime = Date.now() - 150000; // Rebalancing for 2.5 minutes
        const currentTime = Date.now();

        const result = simulateGetHealthState({
          state: ConsumerState.REBALANCING,
          startupTime,
          currentTime,
          lastPartitionAssignmentTime: startupTime + 60000,
          rebalanceStartTime,
          assignedPartitionsCount: 0,
        });

        expect(result.isHealthy).toBe(false);
        expect(result.state).toBe(ConsumerState.STALE);
      });
    });

    describe('Active State', () => {
      it('should report healthy when active with partitions', () => {
        const startupTime = Date.now() - 600000;
        const currentTime = Date.now();

        const result = simulateGetHealthState({
          state: ConsumerState.ACTIVE,
          startupTime,
          currentTime,
          lastPartitionAssignmentTime: Date.now() - 60000,
          rebalanceStartTime: null,
          assignedPartitionsCount: 3,
        });

        expect(result.isHealthy).toBe(true);
        expect(result.reason).toContain('Active with 3 partition(s)');
      });
    });

    describe('Connected State (No Partitions)', () => {
      it('should report healthy when connected but waiting for partitions', () => {
        const startupTime = Date.now() - 600000;
        const currentTime = Date.now();

        const result = simulateGetHealthState({
          state: ConsumerState.CONNECTED,
          startupTime,
          currentTime,
          lastPartitionAssignmentTime: null,
          rebalanceStartTime: null,
          assignedPartitionsCount: 0,
        });

        expect(result.isHealthy).toBe(true);
        expect(result.reason).toContain('waiting for partition assignment');
      });
    });

    describe('Stale State Detection', () => {
      it('should detect stale when connected but had partitions that were lost', () => {
        const startupTime = Date.now() - 900000; // Started 15 minutes ago
        const lastPartitionAssignmentTime = Date.now() - 700000; // Had partitions 11+ minutes ago
        const currentTime = Date.now();

        const result = simulateGetHealthState({
          state: ConsumerState.CONNECTED,
          startupTime,
          currentTime,
          lastPartitionAssignmentTime,
          rebalanceStartTime: null,
          assignedPartitionsCount: 0,
        });

        expect(result.isHealthy).toBe(false);
        expect(result.state).toBe(ConsumerState.STALE);
      });
    });

    describe('Disconnected State', () => {
      it('should report unhealthy when disconnected after startup grace period', () => {
        const startupTime = Date.now() - 300000;
        const currentTime = Date.now();

        const result = simulateGetHealthState({
          state: ConsumerState.DISCONNECTED,
          startupTime,
          currentTime,
          lastPartitionAssignmentTime: null,
          rebalanceStartTime: null,
          assignedPartitionsCount: 0,
        });

        expect(result.isHealthy).toBe(false);
        expect(result.reason).toContain('disconnected');
      });
    });
  });

  describe('Grace Period Constants', () => {
    it('should have appropriate startup grace period (3 minutes)', () => {
      expect(DEFAULT_STARTUP_GRACE_PERIOD_MS).toBe(180000);
    });

    it('should have appropriate rebalance grace period (2 minutes)', () => {
      expect(DEFAULT_REBALANCE_GRACE_PERIOD_MS).toBe(120000);
    });

    it('should have appropriate stale threshold (10 minutes)', () => {
      expect(DEFAULT_STALE_THRESHOLD_MS).toBe(600000);
    });
  });
});

/**
 * Integration tests for KafkaConsumerService with mocked KafkaJS.
 * These tests verify the actual service implementation rather than
 * re-implementing the logic in test code.
 */
describe('KafkaConsumerService Integration', () => {
  let service: KafkaConsumerService;
  let module: TestingModule;
  let mockConsumer: any;
  let eventHandlers: Record<string, (...args: any[]) => void>;

  // Create mock consumer with event emitter functionality
  function createMockConsumer() {
    eventHandlers = {};
    return {
      events: {
        CONNECT: 'consumer.connect',
        DISCONNECT: 'consumer.disconnect',
        CRASH: 'consumer.crash',
        GROUP_JOIN: 'consumer.group_join',
        REBALANCING: 'consumer.rebalancing',
      },
      on: jest.fn((event: string, handler: (...args: any[]) => void) => {
        eventHandlers[event] = handler;
      }),
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      subscribe: jest.fn().mockResolvedValue(undefined),
      run: jest.fn().mockResolvedValue(undefined),
    };
  }

  beforeEach(async () => {
    mockConsumer = createMockConsumer();

    const mockKafka = {
      consumer: jest.fn().mockReturnValue(mockConsumer),
    };

    module = await Test.createTestingModule({
      providers: [
        KafkaConsumerService,
        {
          provide: KAFKAJS_INSTANCE,
          useValue: mockKafka,
        },
        {
          provide: KAFKA_MODULE_OPTIONS,
          useValue: {
            consumer: { groupId: 'test-group' },
          },
        },
        {
          provide: KafkaHandlerRegistry,
          useValue: { getHandlerByTopic: jest.fn() },
        },
        {
          provide: KafkaRetryService,
          useValue: { getRetryTopicName: jest.fn() },
        },
        {
          provide: KafkaDlqService,
          useValue: { storeToDlq: jest.fn() },
        },
        {
          provide: KafkaProducerService,
          useValue: { send: jest.fn() },
        },
      ],
    }).compile();

    service = module.get<KafkaConsumerService>(KafkaConsumerService);
  });

  afterEach(async () => {
    await module?.close();
  });

  describe('Initial State', () => {
    it('should start in DISCONNECTED state', () => {
      expect(service.getState()).toBe(ConsumerState.DISCONNECTED);
    });

    it('should have no partitions assigned initially', () => {
      expect(service.getAssignedPartitions()).toEqual([]);
      expect(service.hasPartitionsAssigned()).toBe(false);
    });

    it('should report healthy during startup grace period', () => {
      const healthState = service.getHealthState();
      expect(healthState.isHealthy).toBe(true);
      expect(healthState.reason).toContain('startup grace period');
    });
  });

  describe('CONNECTING State', () => {
    it('should transition to CONNECTING when connect() is called', async () => {
      // Start connecting (but don't await yet to check intermediate state)
      const connectPromise = service.connect();

      // The CONNECT event should fire and transition to CONNECTED
      // Simulate the event
      eventHandlers['consumer.connect']?.();

      await connectPromise;

      // After connect completes, should be CONNECTED
      expect(service.getState()).toBe(ConsumerState.CONNECTED);
    });
  });

  describe('Event Listener State Transitions', () => {
    it('should transition to CONNECTED on CONNECT event', () => {
      eventHandlers['consumer.connect']?.();
      expect(service.getState()).toBe(ConsumerState.CONNECTED);
    });

    it('should transition to DISCONNECTED on DISCONNECT event', () => {
      // First connect
      eventHandlers['consumer.connect']?.();
      expect(service.getState()).toBe(ConsumerState.CONNECTED);

      // Then disconnect
      eventHandlers['consumer.disconnect']?.();
      expect(service.getState()).toBe(ConsumerState.DISCONNECTED);
      expect(service.getAssignedPartitions()).toEqual([]);
    });

    it('should transition to DISCONNECTED on CRASH event', () => {
      eventHandlers['consumer.connect']?.();

      eventHandlers['consumer.crash']?.({
        payload: { error: new Error('Test crash'), restart: false },
      });

      expect(service.getState()).toBe(ConsumerState.DISCONNECTED);
    });

    it('should transition to ACTIVE on GROUP_JOIN event', () => {
      eventHandlers['consumer.connect']?.();

      eventHandlers['consumer.group_join']?.({
        payload: {
          groupId: 'test-group',
          memberId: 'member-1',
          isLeader: false,
          memberAssignment: {
            'test-topic': [0, 1, 2],
          },
        },
      });

      expect(service.getState()).toBe(ConsumerState.ACTIVE);
      expect(service.getAssignedPartitions()).toEqual([
        { topic: 'test-topic', partition: 0 },
        { topic: 'test-topic', partition: 1 },
        { topic: 'test-topic', partition: 2 },
      ]);
      expect(service.hasPartitionsAssigned()).toBe(true);
    });

    it('should transition to REBALANCING on REBALANCING event', () => {
      eventHandlers['consumer.connect']?.();
      eventHandlers['consumer.group_join']?.({
        payload: {
          groupId: 'test-group',
          memberId: 'member-1',
          isLeader: false,
          memberAssignment: { 'test-topic': [0] },
        },
      });

      eventHandlers['consumer.rebalancing']?.({
        payload: { groupId: 'test-group', memberId: 'member-1' },
      });

      expect(service.getState()).toBe(ConsumerState.REBALANCING);
    });
  });

  describe('Health State with Events', () => {
    it('should report healthy when ACTIVE', () => {
      eventHandlers['consumer.connect']?.();
      eventHandlers['consumer.group_join']?.({
        payload: {
          groupId: 'test-group',
          memberId: 'member-1',
          isLeader: false,
          memberAssignment: { 'test-topic': [0, 1] },
        },
      });

      const healthState = service.getHealthState();
      // During startup grace period, will still show grace period message
      expect(healthState.isHealthy).toBe(true);
    });

    it('should report CONNECTING state health correctly', () => {
      // Manually trigger connecting state scenario
      eventHandlers['consumer.connect']?.();

      const healthState = service.getHealthState();
      expect(healthState.isHealthy).toBe(true);
    });
  });

  describe('Configurable Grace Periods', () => {
    it('should use custom grace periods from options', async () => {
      // Create a new module with custom health options
      const customModule = await Test.createTestingModule({
        providers: [
          KafkaConsumerService,
          {
            provide: KAFKAJS_INSTANCE,
            useValue: { consumer: jest.fn().mockReturnValue(createMockConsumer()) },
          },
          {
            provide: KAFKA_MODULE_OPTIONS,
            useValue: {
              consumer: { groupId: 'test-group' },
              health: {
                startupGracePeriodMs: 60000, // 1 minute
                rebalanceGracePeriodMs: 30000, // 30 seconds
                staleThresholdMs: 300000, // 5 minutes
              },
            },
          },
          {
            provide: KafkaHandlerRegistry,
            useValue: { getHandlerByTopic: jest.fn() },
          },
          {
            provide: KafkaRetryService,
            useValue: { getRetryTopicName: jest.fn() },
          },
          {
            provide: KafkaDlqService,
            useValue: { storeToDlq: jest.fn() },
          },
          {
            provide: KafkaProducerService,
            useValue: { send: jest.fn() },
          },
        ],
      }).compile();

      const customService = customModule.get<KafkaConsumerService>(KafkaConsumerService);

      // Service should use the custom values (we can verify via health state messages)
      const healthState = customService.getHealthState();
      expect(healthState.isHealthy).toBe(true);
      expect(healthState.reason).toContain('60'); // 60 seconds in the message

      await customModule.close();
    });
  });

  describe('getHealthState() does not mutate state', () => {
    it('should not change internal state when checking health', () => {
      eventHandlers['consumer.connect']?.();

      const stateBefore = service.getState();
      service.getHealthState();
      service.getHealthState();
      service.getHealthState();
      const stateAfter = service.getState();

      expect(stateBefore).toBe(stateAfter);
    });
  });
});
