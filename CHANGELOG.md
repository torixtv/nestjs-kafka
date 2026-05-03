# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-05-03

### ✨ Added

#### `health.manualReconnect` option — recover from non-restartable consumer crashes

KafkaJS 2.x consults `consumer.retry.restartOnFailure` only for *already-retriable* errors (the `&&` gate at `node_modules/kafkajs/src/consumer/index.js:267-289`). Crashes wrapped as `KafkaJSNonRetriableError` — including transient `BROKER_NOT_AVAILABLE` from Redpanda Serverless leader elections — bypass that callback entirely and emit `CRASH` with `payload.restart === false`. Until now, the only recovery path was a Kubernetes pod restart.

The new `KafkaHealthOptions.manualReconnect` makes `KafkaConsumerService` rebuild the dead `kafkajs` Consumer instance in-process — disconnecting the old one, creating a new one, re-subscribing, and resuming consumption — with exponential backoff between attempts. The disconnect grace period applies while reconnect runs, so readiness probes stay green.

```typescript
KafkaModule.forRootAsync({
  useFactory: () => ({
    consumer: { groupId: 'my-service-group' /* ... */ },
    health: {
      manualReconnect: {
        enabled: true,        // default: false (preserves prior fail-fast)
        maxAttempts: 5,       // default: 5
        baseDelayMs: 1000,    // default: 1000 (1s)
        maxDelayMs: 30000,    // default: 30000 (30s)
      },
    },
  }),
});
```

- New interface: `KafkaManualReconnectOptions`
- New field: `KafkaHealthOptions.manualReconnect`
- New defaults: `DEFAULT_MANUAL_RECONNECT_MAX_ATTEMPTS = 5`, `DEFAULT_MANUAL_RECONNECT_BASE_DELAY_MS = 1000`, `DEFAULT_MANUAL_RECONNECT_MAX_DELAY_MS = 30000`
- Concurrent reconnect triggers are coalesced via an internal `manualReconnectInProgress` latch.
- When all attempts fail, `disconnectedAt` is cleared so the readiness probe flips unhealthy and Kubernetes can restart the pod (final fallback).

### 🐛 Fixed

- The `CRASH` handler's error log was using the NestJS `Logger.error(message, traceObject)` shape, so the `{ error, restart }` metadata was being stringified to `[object Object]` and lost in pino output. The new format passes the underlying error's `stack` as the trace, surfacing the `KafkaJSNonRetriableError` details (and its message) in production logs for the first time.

## [0.3.3] - 2026-04-25

### ✨ Added

#### `health.disconnectGracePeriodMs` option (default 60s)

The consumer health indicator now treats `DISCONNECTED` as a self-recovering state during a configurable grace window, mirroring the existing `STARTUP` and `REBALANCING` grace periods. This prevents Kubernetes liveness probes from killing pods during transient broker churn (Redpanda Serverless decommissions, leader transitions, etc.) where KafkaJS auto-reconnects in ~25–30s.

- New field: `KafkaHealthOptions.disconnectGracePeriodMs` (default `60000`)
- New default: `KafkaConsumerService.DEFAULT_DISCONNECT_GRACE_PERIOD_MS = 60000`
- New health reason: `Recovering from disconnect (Xs / 60s max)` while within grace
- New health reason: `Consumer disconnected from broker (exceeded grace period)` once grace elapses
- Fail-fast crashes (`CRASH` with `restart=false`) bypass the grace period and report unhealthy immediately, preserving opt-out via `consumer.retry.restartOnFailure`

### 🔄 Changed

- `CRASH` events with `restart=true` no longer transition the consumer state to `REBALANCING`. The state now stays `DISCONNECTED` (semantically accurate) and the new `disconnectGracePeriodMs` provides the recovery window. Logs and external monitoring of `getState()` / `getHealthState().state` now correctly report `disconnected` during transient crash recovery instead of `rebalancing`.

### 🐛 Fixed

- ENG-593: Health indicator no longer flips to `down` for the full 25–30s KafkaJS auto-reconnect window after a transient broker disconnect, eliminating the root cause of pod restart loops in services with active consumer fetch sessions (e.g., `mux-gateway-service`).

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