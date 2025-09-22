# @torix/kafka-events

A simplified, reusable NestJS Kafka events package with retry and DLQ support.

## Features

- **Simple API**: Clean, intuitive decorators and services
- **Retry Support**: Configurable retry with exponential/linear backoff
- **DLQ Support**: Optional dead letter queue handling via plugins
- **Plugin Architecture**: Extensible via plugins
- **Production Ready**: Based on battle-tested implementations
- **TypeScript**: Full TypeScript support with strict typing

## Installation

```bash
npm install @torix/kafka-events kafkajs
```

## Quick Start

### 1. Import the Module

```typescript
import { Module } from '@nestjs/common';
import { KafkaModule } from '@torix/kafka-events';

@Module({
  imports: [
    KafkaModule.forRoot({
      client: {
        clientId: 'my-service',
        brokers: ['localhost:9092'],
      },
      consumer: {
        groupId: 'my-service-group',
      },
    }),
  ],
})
export class AppModule {}
```

### 2. Create Event Handlers

```typescript
import { Injectable } from '@nestjs/common';
import { EventHandler, KafkaController } from '@torix/kafka-events';

@Injectable()
export class OrderController extends KafkaController {
  @EventHandler('order.created', {
    retry: {
      enabled: true,
      attempts: 3,
      backoff: 'exponential',
    },
  })
  async handleOrderCreated(payload: any) {
    console.log('Order created:', payload);
    // Process the order
  }

  @EventHandler('user.registered')
  async handleUserRegistered(payload: any) {
    console.log('User registered:', payload);
    // Process the user registration
  }
}
```

### 3. Produce Messages

```typescript
import { Injectable } from '@nestjs/common';
import { KafkaProducerService } from '@torix/kafka-events';

@Injectable()
export class OrderService {
  constructor(private readonly producer: KafkaProducerService) {}

  async createOrder(orderData: any) {
    // Create order logic...

    await this.producer.send('order.created', {
      key: orderData.id,
      value: orderData,
    });
  }
}
```

## Configuration

### Basic Configuration

```typescript
KafkaModule.forRoot({
  client: {
    clientId: 'my-service',
    brokers: ['localhost:9092'],
  },
  consumer: {
    groupId: 'my-service-group',
  },
  retry: {
    enabled: true,
    attempts: 3,
    backoff: 'exponential',
    maxDelay: 30000,
    baseDelay: 1000,
  },
  dlq: {
    enabled: true,
    topic: 'my-service.dlq',
  },
})
```

### Async Configuration

```typescript
KafkaModule.forRootAsync({
  useFactory: async (configService: ConfigService) => ({
    client: {
      clientId: configService.get('KAFKA_CLIENT_ID'),
      brokers: configService.get('KAFKA_BROKERS').split(','),
    },
    consumer: {
      groupId: configService.get('KAFKA_CONSUMER_GROUP'),
    },
  }),
  inject: [ConfigService],
})
```

## Plugins

### DLQ Plugin

```typescript
import { DLQPlugin } from '@torix/kafka-events';

KafkaModule.forRoot({
  // ... other config
  plugins: [
    new DLQPlugin({
      topicPrefix: 'dlq',
      includeErrorDetails: true,
      includeOriginalMessage: true,
    }),
  ],
})
```

## API Reference

### Decorators

- `@EventHandler(pattern, options?)` - Handle Kafka events with optional retry/DLQ
- `@SimpleEventHandler(pattern)` - Handle events without retry/DLQ

### Services

- `KafkaProducerService` - Send messages to Kafka topics
- `KafkaController` - Base class for event handlers

### Utilities

- `calculateRetryDelay()` - Calculate retry delays
- `getRetryCountFromHeaders()` - Extract retry count from message headers
- `createMessageId()` - Create unique message identifiers

## Migration from Complex Implementation

This package simplifies the previous complex implementation by:

1. **Removing 4 overlapping retry mechanisms** → Single clean interceptor
2. **Simplifying DLQ from complex processor** → Simple plugin-based publishing
3. **Consolidating configuration** → Single, flat configuration object
4. **Plugin architecture** → Optional features that can be added as needed

See `EXTRACTION_PLAN.md` for detailed migration information.

## License

MIT