import { Test, TestingModule } from '@nestjs/testing';
import { DLQPlugin, DLQPluginOptions } from './dlq.plugin';
import { KafkaProducerService } from '../../core/kafka.producer';

describe('DLQPlugin', () => {
  let plugin: DLQPlugin;
  let mockProducer: jest.Mocked<KafkaProducerService>;

  beforeEach(async () => {
    mockProducer = {
      send: jest.fn(),
      sendBatch: jest.fn(),
      sendTransaction: jest.fn(),
      connect: jest.fn(),
      onModuleDestroy: jest.fn(),
      isReady: jest.fn(),
    } as any;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('initialization', () => {
    it('should initialize with default options', async () => {
      plugin = new DLQPlugin(mockProducer);

      await plugin.initialize();

      expect(plugin.name).toBe('dlq');
    });

    it('should initialize with custom options', async () => {
      const options: DLQPluginOptions = {
        topicPrefix: 'custom-dlq',
        includeErrorDetails: false,
        includeOriginalMessage: false,
        customDlqTopic: 'my-custom-dlq',
      };

      plugin = new DLQPlugin(mockProducer, options);

      await plugin.initialize();

      expect(plugin.name).toBe('dlq');
    });
  });

  describe('handleFailure', () => {
    beforeEach(() => {
      plugin = new DLQPlugin(mockProducer);
    });

    it('should send failed message to DLQ with default options', async () => {
      const message = {
        key: 'test-key',
        value: { orderId: '123', customerId: '456' },
        headers: { 'original-header': 'value' },
        topic: 'orders.created',
        offset: '789',
        timestamp: '1640995200000',
        partition: 0,
      };

      const error = new Error('Processing failed');

      mockProducer.send.mockResolvedValue();

      await plugin.handleFailure(message, error);

      expect(mockProducer.send).toHaveBeenCalledWith(
        'dlq.orders.created',
        {
          key: 'test-key',
          value: {
            originalMessage: {
              value: { orderId: '123', customerId: '456' },
              headers: { 'original-header': 'value' },
              timestamp: '1640995200000',
              offset: '789',
              partition: 0,
            },
            error: {
              message: 'Processing failed',
              name: 'Error',
              stack: error.stack,
              timestamp: expect.any(String),
            },
            dlqMetadata: {
              processedAt: expect.any(String),
              plugin: 'dlq',
              version: '1.0.0',
            },
          },
          headers: {
            'original-header': 'value',
            'x-dlq-timestamp': expect.any(String),
            'x-dlq-reason': 'Processing failed',
            'x-original-topic': 'orders.created',
          },
        }
      );
    });

    it('should use custom DLQ topic when specified', async () => {
      const options: DLQPluginOptions = {
        customDlqTopic: 'my-custom-dlq-topic',
      };

      plugin = new DLQPlugin(mockProducer, options);

      const message = {
        key: 'test-key',
        value: { data: 'test' },
        topic: 'any.topic',
      };

      const error = new Error('Test error');

      await plugin.handleFailure(message, error);

      expect(mockProducer.send).toHaveBeenCalledWith(
        'my-custom-dlq-topic',
        expect.any(Object)
      );
    });

    it('should use custom topic prefix', async () => {
      const options: DLQPluginOptions = {
        topicPrefix: 'failed-messages',
      };

      plugin = new DLQPlugin(mockProducer, options);

      const message = {
        key: 'test-key',
        value: { data: 'test' },
        topic: 'user.registered',
      };

      const error = new Error('Test error');

      await plugin.handleFailure(message, error);

      expect(mockProducer.send).toHaveBeenCalledWith(
        'failed-messages.user.registered',
        expect.any(Object)
      );
    });

    it('should exclude error details when configured', async () => {
      const options: DLQPluginOptions = {
        includeErrorDetails: false,
      };

      plugin = new DLQPlugin(mockProducer, options);

      const message = {
        key: 'test-key',
        value: { data: 'test' },
        topic: 'test.topic',
      };

      const error = new Error('Test error');

      await plugin.handleFailure(message, error);

      const [, sentMessage] = mockProducer.send.mock.calls[0];

      expect(sentMessage.value.error).toBeUndefined();
      expect(sentMessage.value.dlqMetadata).toBeDefined();
    });

    it('should exclude original message when configured', async () => {
      const options: DLQPluginOptions = {
        includeOriginalMessage: false,
      };

      plugin = new DLQPlugin(mockProducer, options);

      const message = {
        key: 'test-key',
        value: { data: 'test' },
        topic: 'test.topic',
      };

      const error = new Error('Test error');

      await plugin.handleFailure(message, error);

      const [, sentMessage] = mockProducer.send.mock.calls[0];

      expect(sentMessage.value.originalMessage).toBeUndefined();
      expect(sentMessage.value.error).toBeDefined();
      expect(sentMessage.value.dlqMetadata).toBeDefined();
    });

    it('should handle messages without topic gracefully', async () => {
      const message = {
        key: 'test-key',
        value: { data: 'test' },
        // No topic field
      };

      const error = new Error('Test error');

      await plugin.handleFailure(message, error);

      expect(mockProducer.send).toHaveBeenCalledWith(
        'dlq.unknown',
        expect.objectContaining({
          headers: expect.objectContaining({
            'x-original-topic': 'unknown',
          }),
        })
      );
    });

    it('should handle messages without headers gracefully', async () => {
      const message = {
        key: 'test-key',
        value: { data: 'test' },
        topic: 'test.topic',
        // No headers field
      };

      const error = new Error('Test error');

      await plugin.handleFailure(message, error);

      expect(mockProducer.send).toHaveBeenCalledWith(
        'dlq.test.topic',
        expect.objectContaining({
          headers: expect.objectContaining({
            'x-dlq-timestamp': expect.any(String),
            'x-dlq-reason': 'Test error',
            'x-original-topic': 'test.topic',
          }),
        })
      );
    });

    it('should preserve existing headers and add DLQ headers', async () => {
      const message = {
        key: 'test-key',
        value: { data: 'test' },
        headers: {
          'custom-header-1': 'value1',
          'custom-header-2': 'value2',
        },
        topic: 'test.topic',
      };

      const error = new Error('Test error');

      await plugin.handleFailure(message, error);

      const [, sentMessage] = mockProducer.send.mock.calls[0];

      expect(sentMessage.headers).toEqual({
        'custom-header-1': 'value1',
        'custom-header-2': 'value2',
        'x-dlq-timestamp': expect.any(String),
        'x-dlq-reason': 'Test error',
        'x-original-topic': 'test.topic',
      });
    });
  });

  describe('error handling', () => {
    beforeEach(() => {
      plugin = new DLQPlugin(mockProducer);
    });

    it('should handle producer send failures gracefully', async () => {
      const message = {
        key: 'test-key',
        value: { data: 'test' },
        topic: 'test.topic',
      };

      const error = new Error('Original error');
      const sendError = new Error('DLQ send failed');

      mockProducer.send.mockRejectedValue(sendError);

      // Should not throw even if DLQ send fails
      await expect(plugin.handleFailure(message, error)).resolves.not.toThrow();

      expect(mockProducer.send).toHaveBeenCalled();
    });

    it('should handle errors with missing properties', async () => {
      const message = {
        key: 'test-key',
        value: { data: 'test' },
        topic: 'test.topic',
      };

      const error = {
        message: 'Custom error',
        // Missing name and stack properties
      } as Error;

      await plugin.handleFailure(message, error);

      const [, sentMessage] = mockProducer.send.mock.calls[0];

      expect(sentMessage.value.error).toEqual({
        message: 'Custom error',
        name: undefined,
        stack: undefined,
        timestamp: expect.any(String),
      });
    });
  });

  describe('payload building', () => {
    beforeEach(() => {
      plugin = new DLQPlugin(mockProducer);
    });

    it('should build complete payload with all options enabled', async () => {
      const options: DLQPluginOptions = {
        includeErrorDetails: true,
        includeOriginalMessage: true,
      };

      plugin = new DLQPlugin(mockProducer, options);

      const message = {
        key: 'test-key',
        value: { orderId: '123' },
        headers: { 'correlation-id': 'abc-123' },
        timestamp: '1640995200000',
        offset: '456',
        partition: 2,
        topic: 'orders.created',
      };

      const error = new Error('Processing failed');

      await plugin.handleFailure(message, error);

      const [, sentMessage] = mockProducer.send.mock.calls[0];

      expect(sentMessage.value).toEqual({
        originalMessage: {
          value: { orderId: '123' },
          headers: { 'correlation-id': 'abc-123' },
          timestamp: '1640995200000',
          offset: '456',
          partition: 2,
        },
        error: {
          message: 'Processing failed',
          name: 'Error',
          stack: expect.any(String),
          timestamp: expect.any(String),
        },
        dlqMetadata: {
          processedAt: expect.any(String),
          plugin: 'dlq',
          version: '1.0.0',
        },
      });
    });

    it('should build minimal payload when options are disabled', async () => {
      const options: DLQPluginOptions = {
        includeErrorDetails: false,
        includeOriginalMessage: false,
      };

      plugin = new DLQPlugin(mockProducer, options);

      const message = {
        key: 'test-key',
        value: { data: 'test' },
        topic: 'test.topic',
      };

      const error = new Error('Test error');

      await plugin.handleFailure(message, error);

      const [, sentMessage] = mockProducer.send.mock.calls[0];

      expect(sentMessage.value).toEqual({
        dlqMetadata: {
          processedAt: expect.any(String),
          plugin: 'dlq',
          version: '1.0.0',
        },
      });
    });
  });

  describe('topic name building', () => {
    it('should build topic name with default prefix', () => {
      plugin = new DLQPlugin(mockProducer);

      const message = { topic: 'user.profile.updated' };

      // Access private method for testing
      const topicName = (plugin as any).buildDlqTopicName(message);

      expect(topicName).toBe('dlq.user.profile.updated');
    });

    it('should build topic name with custom prefix', () => {
      const options: DLQPluginOptions = {
        topicPrefix: 'dead-letters',
      };

      plugin = new DLQPlugin(mockProducer, options);

      const message = { topic: 'payment.processed' };

      const topicName = (plugin as any).buildDlqTopicName(message);

      expect(topicName).toBe('dead-letters.payment.processed');
    });

    it('should handle missing topic', () => {
      plugin = new DLQPlugin(mockProducer);

      const message = {};

      const topicName = (plugin as any).buildDlqTopicName(message);

      expect(topicName).toBe('dlq.unknown');
    });
  });
});