import { Controller, UseInterceptors } from '@nestjs/common';
import { RetryInterceptor } from '../interceptors/retry.interceptor';

/**
 * Base controller class for Kafka event handlers.
 * Automatically applies retry interceptor to all methods.
 */
@Controller()
@UseInterceptors(RetryInterceptor)
export abstract class KafkaController {
  // Base controller provides common functionality for Kafka event handlers
  // Interceptor handles all retry and DLQ logic
}
