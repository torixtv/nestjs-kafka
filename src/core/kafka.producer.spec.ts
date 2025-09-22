import { Test, TestingModule } from '@nestjs/testing';
import { Kafka, Producer, ProducerRecord } from 'kafkajs';
import { KafkaProducerService } from './kafka.producer';
import { KAFKAJS_INSTANCE, KAFKA_PLUGINS } from './kafka.constants';

describe('KafkaProducerService', () => {
  let service: KafkaProducerService;
  let mockProducer: jest.Mocked<Producer>;
  let mockKafka: jest.Mocked<Kafka>;

  beforeEach(async () => {
    mockProducer = {
      connect: jest.fn(),
      disconnect: jest.fn(),
      send: jest.fn(),
      transaction: jest.fn(),
    } as any;

    mockKafka = {
      producer: jest.fn().mockReturnValue(mockProducer),
    } as any;

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        KafkaProducerService,
        {
          provide: KAFKAJS_INSTANCE,
          useValue: mockKafka,
        },
        {
          provide: KAFKA_PLUGINS,
          useValue: [],
        },
      ],
    }).compile();

    service = module.get<KafkaProducerService>(KafkaProducerService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('initialization', () => {
    it('should be defined', () => {
      expect(service).toBeDefined();
    });

    it('should create a producer instance', () => {
      expect(mockKafka.producer).toHaveBeenCalled();
    });
  });

  describe('connect', () => {
    it('should connect the producer', async () => {
      await service.connect();

      expect(mockProducer.connect).toHaveBeenCalled();
    });

    it('should not connect if already connected', async () => {
      await service.connect();
      await service.connect();

      expect(mockProducer.connect).toHaveBeenCalledTimes(1);
    });

    it('should handle connection errors', async () => {
      const error = new Error('Connection failed');
      mockProducer.connect.mockRejectedValue(error);

      await expect(service.connect()).rejects.toThrow('Connection failed');
    });
  });

  describe('send', () => {
    beforeEach(async () => {
      await service.connect();
    });

    it('should send a single message', async () => {
      const topic = 'test-topic';
      const message = {
        key: 'test-key',
        value: { data: 'test' },
        headers: { 'test-header': 'value' },
      };

      mockProducer.send.mockResolvedValue([{ partition: 0, errorCode: 0, offset: '123' }]);

      await service.send(topic, message);

      expect(mockProducer.send).toHaveBeenCalledWith({
        topic,
        messages: [
          {
            key: 'test-key',
            value: JSON.stringify({ data: 'test' }),
            headers: { 'test-header': 'value' },
            partition: undefined,
            timestamp: undefined,
          },
        ],
      });
    });

    it('should handle string values without serialization', async () => {
      const topic = 'test-topic';
      const message = {
        key: 'test-key',
        value: 'string-value',
      };

      mockProducer.send.mockResolvedValue([{ partition: 0, errorCode: 0, offset: '123' }]);

      await service.send(topic, message);

      expect(mockProducer.send).toHaveBeenCalledWith({
        topic,
        messages: [
          {
            key: 'test-key',
            value: 'string-value',
            headers: undefined,
            partition: undefined,
            timestamp: undefined,
          },
        ],
      });
    });

    it('should auto-connect if not connected', async () => {
      const freshService = new KafkaProducerService(mockKafka, []);
      mockProducer.connect.mockClear();

      await freshService.send('topic', { value: 'test' });

      expect(mockProducer.connect).toHaveBeenCalled();
    });

    it('should handle send errors', async () => {
      const error = new Error('Send failed');
      mockProducer.send.mockRejectedValue(error);

      await expect(service.send('topic', { value: 'test' })).rejects.toThrow('Send failed');
    });
  });

  describe('sendBatch', () => {
    beforeEach(async () => {
      await service.connect();
    });

    it('should send multiple messages', async () => {
      const topic = 'test-topic';
      const messages = [
        { key: 'key1', value: { data: 'test1' } },
        { key: 'key2', value: { data: 'test2' } },
      ];

      mockProducer.send.mockResolvedValue([
        { partition: 0, errorCode: 0, offset: '123' },
        { partition: 0, errorCode: 0, offset: '124' },
      ]);

      await service.sendBatch(topic, messages);

      expect(mockProducer.send).toHaveBeenCalledWith({
        topic,
        messages: [
          {
            key: 'key1',
            value: JSON.stringify({ data: 'test1' }),
            headers: undefined,
            partition: undefined,
            timestamp: undefined,
          },
          {
            key: 'key2',
            value: JSON.stringify({ data: 'test2' }),
            headers: undefined,
            partition: undefined,
            timestamp: undefined,
          },
        ],
      });
    });
  });

  describe('sendTransaction', () => {
    let mockTransaction: any;

    beforeEach(async () => {
      await service.connect();

      mockTransaction = {
        send: jest.fn(),
        commit: jest.fn(),
        abort: jest.fn(),
      };

      mockProducer.transaction.mockResolvedValue(mockTransaction);
    });

    it('should send transactional messages', async () => {
      const records = [
        {
          topic: 'topic1',
          messages: [{ key: 'key1', value: { data: 'test1' } }],
        },
        {
          topic: 'topic2',
          messages: [{ key: 'key2', value: { data: 'test2' } }],
        },
      ];

      await service.sendTransaction(records);

      expect(mockProducer.transaction).toHaveBeenCalled();
      expect(mockTransaction.send).toHaveBeenCalledTimes(2);
      expect(mockTransaction.commit).toHaveBeenCalled();
      expect(mockTransaction.abort).not.toHaveBeenCalled();
    });

    it('should abort transaction on error', async () => {
      const records = [
        {
          topic: 'topic1',
          messages: [{ key: 'key1', value: { data: 'test1' } }],
        },
      ];

      const error = new Error('Transaction failed');
      mockTransaction.send.mockRejectedValue(error);

      await expect(service.sendTransaction(records)).rejects.toThrow('Transaction failed');

      expect(mockTransaction.abort).toHaveBeenCalled();
      expect(mockTransaction.commit).not.toHaveBeenCalled();
    });
  });

  describe('onModuleDestroy', () => {
    it('should disconnect the producer when module is destroyed', async () => {
      await service.connect();
      await service.onModuleDestroy();

      expect(mockProducer.disconnect).toHaveBeenCalled();
    });

    it('should not fail if producer was not connected', async () => {
      await expect(service.onModuleDestroy()).resolves.not.toThrow();
      expect(mockProducer.disconnect).not.toHaveBeenCalled();
    });
  });

  describe('isReady', () => {
    it('should return false when not connected', async () => {
      const isReady = await service.isReady();
      expect(isReady).toBe(false);
    });

    it('should return true when connected', async () => {
      await service.connect();
      const isReady = await service.isReady();
      expect(isReady).toBe(true);
    });
  });
});