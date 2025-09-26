// Core exports
export { KafkaModule } from './core/kafka.module';
export { KafkaProducerService } from './core/kafka.producer';

// Service exports
export { KafkaDlqService } from './services/kafka.dlq.service';
export { KafkaRetryService } from './services/kafka.retry.service';
export { KafkaHandlerRegistry } from './services/kafka.registry';

// Interceptor exports
export { RetryInterceptor } from './interceptors/retry.interceptor';

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
} from './interfaces/kafka.interfaces';

// Service type exports
export type {
  DlqMessageHeaders,
  DlqReprocessingOptions,
} from './services/kafka.dlq.service';

export type {
  RetryMessageHeaders,
  RetryContext,
  RetryConfiguration,
  RetryDecision,
} from './services/kafka.retry.service';

// Core type exports
export type { MessagePayload } from './core/kafka.producer';

// Tracing utilities (optional - only works if @opentelemetry/api is available)
export {
  createRetrySpan,
  createDlqSpan,
  executeWithSpan,
  getCorrelationId,
  getOrGenerateCorrelationId,
  setSpanError,
} from './utils/tracing.utils';
