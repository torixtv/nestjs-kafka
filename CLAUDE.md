# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is **@torix/nestjs-kafka** - a simplified, reusable NestJS Kafka events package with retry and DLQ support. It provides a clean API for handling Kafka events with built-in retry mechanisms and dead letter queue support via plugins.

## Development Commands

### Build & Development
```bash
npm run build          # Build the library for distribution
npm run build:lib      # Build using TypeScript compiler
npm run format         # Format code with Prettier
npm run lint           # Lint and fix code issues
```

### Testing
```bash
npm test               # Run unit tests
npm run test:watch     # Run tests in watch mode
npm run test:cov       # Run tests with coverage
npm run test:integration  # Run integration tests only
npm run test:unit      # Run unit tests only (excludes integration)
npm run test:e2e       # Run e2e tests
```

### Example Application
```bash
cd example
npm run build          # Build example app
npm run start:dev      # Start example app in dev mode
npm run test:e2e       # Run e2e tests against real service
npm run docker:up      # Start Kafka infrastructure
npm run docker:down    # Stop Kafka infrastructure
```

### Kafka Operations
```bash
# From example directory
npm run kafka:topics   # List Kafka topics
npm run kafka:messages # Consume messages from topics
```

## Architecture Overview

### Core Architecture

The package follows a **centralized bootstrap pattern** to ensure proper initialization order in both regular and microservice environments:

1. **KafkaBootstrapService** - Coordinates initialization sequence
2. **KafkaHandlerRegistry** - Discovers and registers `@EventHandler` decorators
3. **KafkaRetryManager** - Creates and manages retry topics
4. **KafkaRetryConsumer** - Processes delayed retry messages
5. **RetryInterceptor** - Handles failures and schedules retries

### Key Architectural Decisions

**Bootstrap Service Pattern**: The `KafkaBootstrapService` solves a critical initialization race condition where retry messages would fail with "Handler not found" because the handler registry wasn't initialized before the retry consumer started. It enforces sequential initialization: Registry → RetryManager → RetryConsumer.

**Timestamp-Based Delay Processing**: Retry messages include `x-process-after` headers with timestamps. The retry consumer checks these timestamps and only processes messages when their delay period has elapsed.

**Plugin Architecture**: DLQ and other features are implemented as optional plugins that can be registered with the KafkaModule, keeping the core lightweight.

### Module Structure

```
src/
├── core/                    # Core module, producer, controller
│   ├── kafka.module.ts      # Main NestJS module with DI setup
│   ├── kafka.producer.ts    # Message publishing service
│   ├── kafka.controller.ts  # Base class for event handlers
│   └── kafka.bootstrap.ts   # Microservice registration helpers
├── services/                # Core business logic services
│   ├── kafka.bootstrap.service.ts  # Initialization coordinator
│   ├── kafka.registry.ts           # Handler discovery/registration
│   ├── kafka.retry-manager.ts      # Retry topic management
│   └── kafka.retry-consumer.ts     # Retry message processing
├── decorators/              # Decorator implementations
│   └── event-handler.decorator.ts  # @EventHandler decorator
├── interceptors/            # NestJS interceptors
│   └── retry.interceptor.ts        # Retry logic interceptor
├── plugins/                 # Optional feature plugins
│   └── dlq/                 # Dead Letter Queue plugin
└── utils/                   # Utility functions
    └── retry.utils.ts       # Retry calculation helpers
```

## Critical Implementation Details

### Microservice vs Regular NestJS Applications

**Regular NestJS Apps**: Use `KafkaModule.forRoot()` in your AppModule and create controllers extending `KafkaController` with `@EventHandler` decorators.

**Microservice Setup**: Use the bootstrap helpers from `kafka.bootstrap.ts`:
```typescript
// In main.ts
await registerKafkaMicroservice(app, options);
await startKafkaMicroservice(app); // Triggers bootstrap service
```

### Handler Registration Lifecycle

The bootstrap service **must** run for handlers to be discovered. In test environments using `Test.createTestingModule()`, you may need to manually trigger:
```typescript
const bootstrapService = app.get(KafkaBootstrapService);
await bootstrapService.forceInitialization();
```

### Retry Configuration Hierarchy

1. **Global defaults** in `KafkaModule.forRoot({ retry: {...} })`
2. **Handler-specific overrides** in `@EventHandler(pattern, { retry: {...} })`
3. **Runtime headers** from previous retry attempts

### Cloud Kafka Configuration (Automatic SASL + SSL)

The package automatically configures SASL and SSL for cloud Kafka providers using environment variables:

```bash
# Environment variables
KAFKA_SASL_MECHANISM=plain          # or scram-sha-256, scram-sha-512
KAFKA_SASL_USERNAME=your-username
KAFKA_SASL_PASSWORD=your-password
KAFKA_SSL_ENABLED=true              # Optional - auto-enabled when SASL is configured
```

**Configuration Precedence**:
1. Explicit config in `KafkaModule.forRoot()` (highest priority)
2. Environment variables (`KAFKA_SASL_*`)
3. Smart defaults (SSL auto-enabled with SASL)

**Smart Defaults**: When SASL credentials are configured (either explicitly or via environment), SSL is automatically enabled unless explicitly set to `false`.

**Implementation**: See [config.utils.ts](src/utils/config.utils.ts) for the configuration merge logic.

### Testing Strategy

**Unit Tests**: Test individual services/utilities in isolation
**Integration Tests**: Test with real Kafka broker (requires Docker)
**E2E Tests**: Test against running NestJS application in `example/` directory

The example application demonstrates the **complete lifecycle** including proper NestJS bootstrap, real Kafka consumer groups, and HTTP monitoring endpoints.

## Important Notes

### Bootstrap Service Dependency

The retry mechanism **requires** the `KafkaBootstrapService` to initialize properly. Without it:
- Handler registry won't discover `@EventHandler` decorators
- Retry consumer will fail with "Handler not found" errors
- Retry topics may not be created

### Testing Against Real Services

The `example/` directory contains a complete NestJS application that demonstrates the retry mechanism working end-to-end. This is the preferred way to verify retry behavior because it uses real NestJS lifecycle hooks (`OnApplicationBootstrap`) that trigger the bootstrap service automatically.

### Configuration Validation

The module requires at minimum:
- `client.brokers` - Kafka broker addresses
- `consumer.groupId` - Consumer group identifier

Retry and DLQ features are **disabled by default** and must be explicitly enabled.

### Package Export Strategy

The package exports everything needed for consumers through a clean public API in `src/index.ts`. Internal services like `KafkaBootstrapService` are exported but typically not needed by consumers since they're automatically instantiated by the DI container.

## Common Issues

1. **"Handler not found for retry message"** - Bootstrap service hasn't run or handler registry not initialized
2. **Tests timing out** - Integration tests require Kafka broker; use Docker setup
3. **Retry messages not processing** - Check `x-process-after` timestamps and retry consumer logs
4. **Module initialization errors** - Verify client configuration and broker connectivity