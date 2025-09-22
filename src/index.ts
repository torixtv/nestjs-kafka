// Module exports
export { KafkaModule } from './core/kafka.module';

// Service exports
export { KafkaProducerService } from './core/kafka.producer';
export { KafkaController } from './core/kafka.controller';
export { KafkaHandlerRegistry } from './services/kafka.registry';
export { KafkaRetryManager } from './services/kafka.retry-manager';
// KafkaDelayManager removed - using interval-based polling instead
export { KafkaRetryConsumer } from './services/kafka.retry-consumer';

// Internal bootstrap service (not typically needed by consumers, but available if needed)
// Note: The bootstrap service is automatically instantiated when KafkaModule is imported

// Bootstrap exports
export {
  registerKafkaMicroservice,
  startKafkaMicroservice,
} from './core/kafka.bootstrap';

// Decorator exports
export {
  EventHandler,
  SimpleEventHandler,
} from './decorators/event-handler.decorator';

// Interceptor exports
export { RetryInterceptor } from './interceptors/retry.interceptor';

// Plugin exports
export { DLQPlugin } from './plugins/dlq/dlq.plugin';

// Utility exports
export {
  calculateRetryDelay,
  getRetryCountFromHeaders,
  getCorrelationIdFromHeaders,
  createMessageId,
} from './utils/retry.utils';

// Constants exports
export {
  KAFKA_MODULE_OPTIONS,
  KAFKAJS_INSTANCE,
  KAFKA_PLUGINS,
  EVENT_HANDLER_METADATA,
  EVENT_PATTERN_METADATA,
} from './core/kafka.constants';

// Interface exports
export type {
  KafkaModuleOptions,
  KafkaModuleAsyncOptions,
  KafkaModuleOptionsFactory,
  KafkaPlugin,
  KafkaRetryOptions,
  KafkaDlqOptions,
  EventHandlerOptions,
  EventHandlerMetadata,
  KafkaConsumerRegistration,
} from './interfaces/kafka.interfaces';

// Type exports
export type { MessagePayload } from './core/kafka.producer';
export type { RegisterKafkaMicroserviceOptions } from './core/kafka.bootstrap';
export type { DLQPluginOptions } from './plugins/dlq/dlq.plugin';
export type { RetryCalculationOptions } from './utils/retry.utils';

// Service interface exports
export type { RegisteredHandler } from './services/kafka.registry';
// DelayedMessage interface removed with KafkaDelayManager
export type { RetryMessageHeaders } from './services/kafka.retry-consumer';
