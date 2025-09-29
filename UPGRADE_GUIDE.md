# Upgrade Guide

## Upgrading from 0.1.x to 0.2.0

Version 0.2.0 represents a significant architectural improvement that simplifies the package by removing the dependency on NestJS microservices infrastructure. The package now manages Kafka consumption internally through a single KafkaJS instance.

### Overview of Changes

**Before (0.1.x):** Required two Kafka instances - one for the microservice consumer and one for the producer/retry/DLQ operations.

**After (0.2.0):** Single Kafka instance handles all operations. The `KafkaModule` now manages consumption internally via `KafkaConsumerService` and `KafkaBootstrapService`.

### Breaking Changes

#### 1. Removed NestJS Microservices Dependency

**Removed:**
- No longer requires `app.connectMicroservice()`
- No longer requires `app.startAllMicroservices()`
- `@EventHandler` no longer wraps `@EventPattern` from `@nestjs/microservices`

**Impact:** You must remove microservice setup code from your `main.ts` file.

#### 2. New Required Configuration: `subscriptions`

The `KafkaModule.forRoot()` now requires a `subscriptions` field to specify which topics to consume.

```typescript
// NEW REQUIRED FIELD
subscriptions: {
  topics: string[];        // List of topics to consume
  fromBeginning?: boolean; // Optional, defaults to false
}
```

**Impact:** You must add the `subscriptions` configuration to your module setup.

#### 3. Removed Configuration Options

**Removed from `KafkaModuleOptions`:**
- `requireBroker?: boolean` - No longer supported
- `serviceName?: string` - No longer used

**Impact:** Remove these options from your configuration if present.

#### 4. Renamed Interface

- `KafkaConsumerRegistration` → `KafkaConsumerSubscription`

**Impact:** If you're using this type directly, update the import.

### Step-by-Step Migration

#### Step 1: Update `main.ts` - Remove Microservice Setup

**Before (0.1.x):**
```typescript
import { NestFactory } from '@nestjs/core';
import { Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // ❌ Remove this entire block
  app.connectMicroservice({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'my-service',
        brokers: ['localhost:9092'],
      },
      consumer: {
        groupId: 'my-service-group',
      },
      subscribe: {
        topics: ['user.created', 'order.processed'],
        fromBeginning: false,
      },
    },
  });

  await app.listen(3000);
  await app.startAllMicroservices(); // ❌ Remove this
}
```

**After (0.2.0):**
```typescript
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // ✅ That's it! Kafka consumption is handled by KafkaModule
  await app.listen(3000);
}
```

#### Step 2: Update `app.module.ts` - Add Subscriptions Configuration

**Before (0.1.x):**
```typescript
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
      retry: {
        enabled: true,
        attempts: 3,
      },
      dlq: {
        enabled: true,
      },
      requireBroker: false, // ❌ Remove this
    }),
  ],
})
export class AppModule {}
```

**After (0.2.0):**
```typescript
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
      // ✅ Add subscriptions configuration
      subscriptions: {
        topics: ['user.created', 'order.processed'],
        fromBeginning: false,
      },
      retry: {
        enabled: true,
        attempts: 3,
      },
      dlq: {
        enabled: true,
      },
      // ❌ requireBroker removed - no longer supported
    }),
  ],
})
export class AppModule {}
```

#### Step 3: Update Controllers (If Needed)

Most controllers won't need changes. However, if you explicitly added `@UseInterceptors(RetryInterceptor)` at the class level, you can remove it since it's now auto-applied when retry is enabled.

**Before (0.1.x):**
```typescript
@Injectable()
@Controller()
@UseInterceptors(RetryInterceptor) // ❌ Can remove if retry enabled
export class MyController {
  @EventHandler('user.created', {
    retry: { enabled: true, attempts: 3 }
  })
  async handleUserCreated(payload: any) {
    // Handler logic
  }
}
```

**After (0.2.0):**
```typescript
@Injectable()
@Controller()
// ✅ No interceptor needed - auto-applied by @EventHandler
export class MyController {
  @EventHandler('user.created', {
    retry: { enabled: true, attempts: 3 }
  })
  async handleUserCreated(payload: any) {
    // Handler logic - no changes needed
  }
}
```

#### Step 4: Update Async Configuration (If Used)

If you're using `forRootAsync`, add the `subscriptions` field to your factory:

**Before (0.1.x):**
```typescript
KafkaModule.forRootAsync({
  useFactory: (configService: ConfigService) => ({
    client: {
      clientId: configService.get('KAFKA_CLIENT_ID'),
      brokers: configService.get('KAFKA_BROKERS').split(','),
    },
    consumer: {
      groupId: configService.get('KAFKA_CONSUMER_GROUP'),
    },
    requireBroker: configService.get('KAFKA_REQUIRE_BROKER', true), // ❌
  }),
  inject: [ConfigService],
})
```

**After (0.2.0):**
```typescript
KafkaModule.forRootAsync({
  useFactory: (configService: ConfigService) => ({
    client: {
      clientId: configService.get('KAFKA_CLIENT_ID'),
      brokers: configService.get('KAFKA_BROKERS').split(','),
    },
    consumer: {
      groupId: configService.get('KAFKA_CONSUMER_GROUP'),
    },
    // ✅ Add subscriptions
    subscriptions: {
      topics: configService.get('KAFKA_TOPICS').split(','),
      fromBeginning: configService.get('KAFKA_FROM_BEGINNING', false),
    },
    // ❌ requireBroker removed
  }),
  inject: [ConfigService],
})
```

### New Features in 0.2.0

#### 1. Simplified Architecture

- Single KafkaJS instance for all operations
- No microservice infrastructure required
- Unified configuration in one place

#### 2. New Public Services

You now have access to:
- `KafkaConsumerService` - Direct access to consumer operations
- `KafkaBootstrapService` - Bootstrap lifecycle management

```typescript
import { KafkaConsumerService, KafkaBootstrapService } from '@torixtv/nestjs-kafka';
```

#### 3. Improved Bootstrap Sequence

Automatic initialization order:
1. Handler registry discovery
2. Retry system initialization
3. DLQ system initialization
4. Main consumer startup

### Troubleshooting

#### Issue: "Consumer configuration is required"

**Cause:** Missing `consumer` configuration in `KafkaModule.forRoot()`.

**Solution:** Ensure you have both `client` and `consumer` configuration:
```typescript
KafkaModule.forRoot({
  client: { clientId: 'my-service', brokers: ['localhost:9092'] },
  consumer: { groupId: 'my-service-group' }, // Required
  subscriptions: { topics: ['my.topic'] },
})
```

#### Issue: Handlers not receiving messages

**Cause:** `subscriptions.topics` not configured or doesn't match handler patterns.

**Solution:** Verify your `subscriptions.topics` includes all topics your handlers listen to:
```typescript
// Module configuration
subscriptions: {
  topics: ['user.created', 'order.processed'], // Must match handler patterns
}

// Controller
@EventHandler('user.created') // Must be in subscriptions.topics
async handleUserCreated(payload: any) { }
```

#### Issue: "Handler not found for retry message"

**Cause:** Bootstrap service hasn't run, or handlers not discovered.

**Solution:** This is automatically handled in 0.2.0 via `OnApplicationBootstrap`. If testing, ensure you're creating a full NestJS application:
```typescript
const app = await NestFactory.create(AppModule);
await app.init(); // Triggers OnApplicationBootstrap
```

#### Issue: Duplicate consumer groups created

**Cause:** In 0.2.0, internal consumers (retry, DLQ) now use `client.clientId` as the base for consumer group names.

**Solution:** No action needed - this is intentional to avoid conflicts. Internal groups are named:
- Retry: `{clientId}.retry.consumer`
- DLQ Reprocessing: `{clientId}.dlq.reprocessing`

### Environment Variables Update

If you're using environment variables, update them:

**Before (0.1.x):**
```bash
KAFKA_CLIENT_ID=my-service
KAFKA_BROKERS=localhost:9092
KAFKA_CONSUMER_GROUP=my-service-group
KAFKA_REQUIRE_BROKER=false  # ❌ Remove
```

**After (0.2.0):**
```bash
KAFKA_CLIENT_ID=my-service
KAFKA_BROKERS=localhost:9092
KAFKA_CONSUMER_GROUP=my-service-group
KAFKA_TOPICS=user.created,order.processed  # ✅ Add
KAFKA_FROM_BEGINNING=false  # ✅ Add (optional)
```

### Testing Your Migration

1. **Remove old microservice code** from `main.ts`
2. **Add `subscriptions`** to your module configuration
3. **Remove `requireBroker`** if present
4. **Start your application**
5. **Verify** handlers receive messages:
   ```bash
   # Send a test message
   curl -X POST http://localhost:3000/send-message

   # Check logs for handler execution
   ```

### Getting Help

If you encounter issues during migration:

1. Check the [README](./README.md) for updated examples
2. Review the [example application](./example) for a complete working setup
3. Open an issue at https://github.com/torixtv/nestjs-kafka/issues

### Summary Checklist

- [ ] Remove `app.connectMicroservice()` from `main.ts`
- [ ] Remove `app.startAllMicroservices()` from `main.ts`
- [ ] Add `subscriptions` field to `KafkaModule.forRoot()`
- [ ] Remove `requireBroker` option if present
- [ ] Remove `serviceName` option if present
- [ ] Update environment variables to include topics
- [ ] Test message consumption
- [ ] Verify retry and DLQ functionality (if enabled)