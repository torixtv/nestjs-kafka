import { EventHandler, SimpleEventHandler } from './event-handler.decorator';
import { EVENT_HANDLER_METADATA } from '../core/kafka.constants';
import 'reflect-metadata';

describe('EventHandler Decorator', () => {
  class TestController {
    @EventHandler('test.topic')
    handleBasicEvent(payload: any) {
      return payload;
    }

    @EventHandler('test.retry.topic', {
      retry: {
        enabled: true,
        attempts: 5,
        backoff: 'linear',
        maxDelay: 60000,
        baseDelay: 2000,
      },
    })
    handleRetryEvent(payload: any) {
      return payload;
    }

    @EventHandler('test.dlq.topic', {
      dlq: {
        enabled: true,
        topic: 'custom.dlq.topic',
      },
    })
    handleDlqEvent(payload: any) {
      return payload;
    }

    @EventHandler('test.full.topic', {
      retry: {
        enabled: true,
        attempts: 3,
        backoff: 'exponential',
      },
      dlq: {
        enabled: true,
        topic: 'full.dlq.topic',
      },
    })
    handleFullConfigEvent(payload: any) {
      return payload;
    }

    @SimpleEventHandler('test.simple.topic')
    handleSimpleEvent(payload: any) {
      return payload;
    }
  }

  let controller: TestController;

  beforeEach(() => {
    controller = new TestController();
  });

  describe('EventHandler', () => {
    it('should set metadata for basic event handler', () => {
      const metadata = Reflect.getMetadata(
        EVENT_HANDLER_METADATA,
        controller.handleBasicEvent
      );

      expect(metadata).toBeDefined();
      expect(metadata.pattern).toBe('test.topic');
      expect(metadata.options.retry.enabled).toBe(false);
      expect(metadata.options.dlq.enabled).toBe(false);
    });

    it('should set metadata with retry configuration', () => {
      const metadata = Reflect.getMetadata(
        EVENT_HANDLER_METADATA,
        controller.handleRetryEvent
      );

      expect(metadata).toBeDefined();
      expect(metadata.pattern).toBe('test.retry.topic');
      expect(metadata.options.retry).toEqual({
        enabled: true,
        attempts: 5,
        backoff: 'linear',
        maxDelay: 60000,
        baseDelay: 2000,
      });
    });

    it('should set metadata with DLQ configuration', () => {
      const metadata = Reflect.getMetadata(
        EVENT_HANDLER_METADATA,
        controller.handleDlqEvent
      );

      expect(metadata).toBeDefined();
      expect(metadata.pattern).toBe('test.dlq.topic');
      expect(metadata.options.dlq).toEqual({
        enabled: true,
        topic: 'custom.dlq.topic',
      });
    });

    it('should set metadata with full configuration', () => {
      const metadata = Reflect.getMetadata(
        EVENT_HANDLER_METADATA,
        controller.handleFullConfigEvent
      );

      expect(metadata).toBeDefined();
      expect(metadata.pattern).toBe('test.full.topic');
      expect(metadata.options.retry).toEqual({
        enabled: true,
        attempts: 3,
        backoff: 'exponential',
        maxDelay: 30000, // default
        baseDelay: 1000, // default
      });
      expect(metadata.options.dlq).toEqual({
        enabled: true,
        topic: 'full.dlq.topic',
      });
    });

    it('should apply default retry configuration when not specified', () => {
      const metadata = Reflect.getMetadata(
        EVENT_HANDLER_METADATA,
        controller.handleBasicEvent
      );

      expect(metadata.options.retry).toEqual({
        enabled: false,
        attempts: 3,
        backoff: 'exponential',
        maxDelay: 30000,
        baseDelay: 1000,
      });
    });

    it('should apply default DLQ configuration when not specified', () => {
      const metadata = Reflect.getMetadata(
        EVENT_HANDLER_METADATA,
        controller.handleBasicEvent
      );

      expect(metadata.options.dlq).toEqual({
        enabled: false,
        topic: undefined,
      });
    });

    it('should merge partial retry configuration with defaults', () => {
      class PartialRetryController {
        @EventHandler('test.partial.topic', {
          retry: {
            enabled: true,
            attempts: 7, // Only specify attempts
          },
        })
        handlePartialRetry(payload: any) {
          return payload;
        }
      }

      const partialController = new PartialRetryController();
      const metadata = Reflect.getMetadata(
        EVENT_HANDLER_METADATA,
        partialController.handlePartialRetry
      );

      expect(metadata.options.retry).toEqual({
        enabled: true,
        attempts: 7, // custom value
        backoff: 'exponential', // default
        maxDelay: 30000, // default
        baseDelay: 1000, // default
      });
    });
  });

  describe('SimpleEventHandler', () => {
    it('should set metadata with retry and DLQ disabled', () => {
      const metadata = Reflect.getMetadata(
        EVENT_HANDLER_METADATA,
        controller.handleSimpleEvent
      );

      expect(metadata).toBeDefined();
      expect(metadata.pattern).toBe('test.simple.topic');
      expect(metadata.options.retry.enabled).toBe(false);
      expect(metadata.options.dlq.enabled).toBe(false);
    });

    it('should be equivalent to EventHandler with defaults', () => {
      class ComparisonController {
        @EventHandler('test.comparison.topic')
        handleWithDefaults(payload: any) {
          return payload;
        }

        @SimpleEventHandler('test.comparison.topic')
        handleSimple(payload: any) {
          return payload;
        }
      }

      const comparisonController = new ComparisonController();

      const defaultMetadata = Reflect.getMetadata(
        EVENT_HANDLER_METADATA,
        comparisonController.handleWithDefaults
      );

      const simpleMetadata = Reflect.getMetadata(
        EVENT_HANDLER_METADATA,
        comparisonController.handleSimple
      );

      expect(simpleMetadata.options.retry.enabled).toBe(
        defaultMetadata.options.retry.enabled
      );
      expect(simpleMetadata.options.dlq.enabled).toBe(
        defaultMetadata.options.dlq.enabled
      );
    });
  });

  describe('decorator composition', () => {
    it('should work with multiple decorators on the same class', () => {
      class MultipleHandlersController {
        @EventHandler('topic.1')
        handler1(payload: any) {}

        @EventHandler('topic.2', { retry: { enabled: true } })
        handler2(payload: any) {}

        @SimpleEventHandler('topic.3')
        handler3(payload: any) {}
      }

      const controller = new MultipleHandlersController();

      const metadata1 = Reflect.getMetadata(EVENT_HANDLER_METADATA, controller.handler1);
      const metadata2 = Reflect.getMetadata(EVENT_HANDLER_METADATA, controller.handler2);
      const metadata3 = Reflect.getMetadata(EVENT_HANDLER_METADATA, controller.handler3);

      expect(metadata1.pattern).toBe('topic.1');
      expect(metadata2.pattern).toBe('topic.2');
      expect(metadata3.pattern).toBe('topic.3');

      expect(metadata1.options.retry.enabled).toBe(false);
      expect(metadata2.options.retry.enabled).toBe(true);
      expect(metadata3.options.retry.enabled).toBe(false);
    });
  });

  describe('edge cases', () => {
    it('should handle empty options object', () => {
      class EmptyOptionsController {
        @EventHandler('test.empty.topic', {})
        handleEmpty(payload: any) {}
      }

      const controller = new EmptyOptionsController();
      const metadata = Reflect.getMetadata(EVENT_HANDLER_METADATA, controller.handleEmpty);

      expect(metadata).toBeDefined();
      expect(metadata.options.retry.enabled).toBe(false);
      expect(metadata.options.dlq.enabled).toBe(false);
    });

    it('should handle null/undefined values in configuration', () => {
      class NullConfigController {
        @EventHandler('test.null.topic', {
          retry: {
            enabled: true,
            attempts: null as any,
            backoff: undefined as any,
          },
        })
        handleNull(payload: any) {}
      }

      const controller = new NullConfigController();
      const metadata = Reflect.getMetadata(EVENT_HANDLER_METADATA, controller.handleNull);

      expect(metadata.options.retry.enabled).toBe(true);
      expect(metadata.options.retry.attempts).toBeNull();
      // Since backoff is undefined, it gets the default value
      expect(metadata.options.retry.backoff).toBe('exponential');
    });
  });
});