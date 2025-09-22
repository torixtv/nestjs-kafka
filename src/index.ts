// Core exports
export { KafkaModule } from './core/kafka.module';
export { KafkaProducerService } from './core/kafka.producer';

// Decorator exports
export {
  EventHandler,
  SimpleEventHandler,
} from './decorators/event-handler.decorator';

// Interface exports
export type {
  KafkaModuleOptions,
  KafkaModuleAsyncOptions,
  KafkaModuleOptionsFactory,
  KafkaRetryOptions,
  KafkaDlqOptions,
  EventHandlerOptions,
  KafkaConsumerRegistration,
} from './interfaces/kafka.interfaces';

// Type exports
export type { MessagePayload } from './core/kafka.producer';
