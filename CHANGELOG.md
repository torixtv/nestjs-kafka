# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.2] - 2025-01-30

### 🚨 Breaking Changes

#### DLQ Reprocessing Now Requires Topic Parameter
- **BREAKING:** `DlqReprocessingOptions.topic` is now **required** (was optional)
- **BREAKING:** Removed `handlerId` parameter from `DlqReprocessingOptions`
- **BREAKING:** DLQ reprocessing no longer supports "reprocess all" mode
- **BREAKING:** Monitoring endpoint `POST /kafka/dlq/reprocess` now requires `topic` in request body

**Migration Required:**
```typescript
// ❌ Before (no longer supported)
await dlqService.startReprocessing({
  batchSize: 100
});

// ✅ After (topic required)
await dlqService.startReprocessing({
  topic: 'user.created',  // Required
  batchSize: 100
});

// ❌ Before (handlerId no longer supported)
await dlqService.startReprocessing({
  handlerId: 'UserService.handleUserCreated',
  batchSize: 100
});

// ✅ After (filter by topic only)
await dlqService.startReprocessing({
  topic: 'user.created',  // Filter by topic
  batchSize: 100
});
```

### ✨ Added

#### Topic-Specific DLQ Reprocessing
- Persistent consumer groups per topic for efficient reprocessing
- Consumer group naming: `${service}.dlq.reprocess.${topic}`
- Each topic maintains independent offsets
- Non-matching messages safely skipped without losing data

#### Monitoring Controller (Auto-Registered)
- **Automatically registered** by default - no manual import needed
- Health checks: `/kafka/health`, `/kafka/health/ready`, `/kafka/health/live`
- Metrics: `/kafka/metrics`
- DLQ operations: `/kafka/dlq/reprocess`, `/kafka/dlq/stop`
- Handler inspection: `/kafka/handlers`
- Metrics reset: `/kafka/metrics/reset`
- Can be disabled via `monitoring: { enabled: false }` in module options

**Usage (enabled by default):**
```typescript
@Module({
  imports: [KafkaModule.forRoot({ ... })],
  // KafkaMonitoringController is automatically registered!
})
export class AppModule {}
```

**To disable:**
```typescript
@Module({
  imports: [
    KafkaModule.forRoot({
      // ... other config
      monitoring: { enabled: false },
    }),
  ],
})
export class AppModule {}
```

### 🔧 Changed

#### DLQ Reprocessing Behavior
- Changed from temporary to persistent consumer groups
- Resume from last committed offset instead of reading from beginning
- Filter by topic only (simplified from handler-based filtering)
- Auto-commit enabled for safe offset management

#### DLQ Topic Naming
- DLQ topic now includes service name: `${service}.dlq`
- Ensures isolation between different services

### 🐛 Fixed

- Fixed issue where DLQ reprocessing would re-read entire DLQ on each invocation
- Fixed consumer group proliferation from temporary reprocessing consumers

### 📚 Documentation

- Updated README with topic-specific DLQ reprocessing examples
- Added explanation of persistent consumer group behavior
- Updated API reference with new `DlqReprocessingOptions` interface
- Added monitoring controller usage examples

---

## [0.2.0] - 2025-01-XX

### 🚨 Breaking Changes

#### Architecture Redesign: Single Kafka Instance
- **BREAKING:** Removed dependency on `@nestjs/microservices` for consumption
- **BREAKING:** `@EventHandler` decorator no longer wraps `@EventPattern`
- **BREAKING:** Removed `app.connectMicroservice()` and `app.startAllMicroservices()` requirement
- **BREAKING:** Added required `subscriptions` field to `KafkaModuleOptions`
- **BREAKING:** Removed `requireBroker` option from `KafkaModuleOptions`
- **BREAKING:** Removed `serviceName` option from `KafkaModuleOptions`
- **BREAKING:** Renamed `KafkaConsumerRegistration` interface to `KafkaConsumerSubscription`

**Migration Required:** See [UPGRADE_GUIDE.md](./UPGRADE_GUIDE.md) for step-by-step instructions.

### ✨ Added

#### New Services
- `KafkaConsumerService` - Manages Kafka message consumption internally
- `KafkaBootstrapService` - Orchestrates initialization sequence automatically

#### New Configuration
- `subscriptions` field in `KafkaModuleOptions`:
  ```typescript
  subscriptions: {
    topics: string[];        // Required: topics to consume
    fromBeginning?: boolean; // Optional: start from beginning
  }
  ```

#### New Registry Methods
- `KafkaHandlerRegistry.discoverHandlers()` - Public method for handler discovery
- `KafkaHandlerRegistry.getHandlerByTopic()` - Find handler for specific topic
- `KafkaHandlerRegistry.getHandlerCount()` - Get total registered handler count

#### New Exports
- Exported `KafkaConsumerService` in public API
- Exported `KafkaBootstrapService` in public API
- Exported `KafkaConsumerSubscription` type

### 🔧 Changed

#### Bootstrap Sequence
- Automatic initialization via `OnApplicationBootstrap` lifecycle hook
- Sequential initialization: Registry → Retry Manager → Retry Consumer → Main Consumer
- Eliminates "Handler not found" race conditions

#### Internal Consumer Groups
- Retry consumer now uses `client.clientId` instead of `consumer.groupId` as base
- DLQ reprocessing consumer now uses `client.clientId` instead of `consumer.groupId` as base
- Prevents consumer group conflicts between main and internal consumers

#### Decorator Behavior
- `@EventHandler` automatically applies `RetryInterceptor` when retry enabled
- No need to manually add `@UseInterceptors(RetryInterceptor)` at class level

#### Message Handling
- DLQ messages now store Buffer values as strings for better persistence
- Improved retry message parsing to handle multiple Buffer formats

### 📚 Documentation

- Added comprehensive [UPGRADE_GUIDE.md](./UPGRADE_GUIDE.md) with migration steps
- Updated README.md with new architecture section
- Added troubleshooting guide for common migration issues
- Updated configuration examples throughout README

### 🐛 Fixed

- Fixed race condition where retry messages failed with "Handler not found"
- Fixed duplicate consumer group proliferation in DLQ reprocessing
- Fixed message value handling for retry operations from DLQ

### 🏗️ Architecture Benefits

- **Simplified Setup:** No microservice configuration needed
- **Single Source of Truth:** All Kafka config in `KafkaModule.forRoot()`
- **Better Resource Management:** One Kafka client instead of two
- **Clearer Initialization:** Explicit bootstrap sequence prevents race conditions
- **Easier Testing:** Can test handlers without microservice infrastructure

---

## [0.1.1] - 2024-XX-XX

### 🐛 Fixed
- Updated package name and repository links to reflect new `@torixtv/nestjs-kafka` branding
- Improved package verification step in publish workflow

### 🔧 Changed
- Updated peer dependencies to support both NestJS 10.x and 11.x

---

## [0.1.0] - 2024-XX-XX

### ✨ Initial Release

#### Core Features
- NestJS integration for Kafka event handling
- `@EventHandler` decorator for message consumption
- Automatic retry mechanism with configurable backoff strategies
- Dead Letter Queue (DLQ) support for failed messages
- OpenTelemetry tracing integration
- Correlation ID propagation across services

#### Modules & Services
- `KafkaModule` - Main module with forRoot/forRootAsync configuration
- `KafkaProducerService` - Message publishing with retry/DLQ support
- `KafkaRetryService` - Automatic retry handling with delay queues
- `KafkaDlqService` - Dead letter queue management and reprocessing
- `KafkaHandlerRegistry` - Automatic handler discovery

#### Configuration
- Flexible retry configuration (linear/exponential backoff)
- Topic-level and handler-level retry overrides
- Configurable DLQ behavior with custom failure callbacks
- Support for async configuration via `forRootAsync`

#### Observability
- OpenTelemetry span creation for all operations
- Automatic correlation ID generation and propagation
- Detailed logging at each stage of message lifecycle
- Metrics tracking for retries, DLQ, and reprocessing

---

## Legend

- 🚨 Breaking Changes
- ✨ Added
- 🔧 Changed
- 🐛 Fixed
- 📚 Documentation
- 🏗️ Architecture