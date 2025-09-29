# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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