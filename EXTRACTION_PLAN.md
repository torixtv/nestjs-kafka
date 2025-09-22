# Torix Kafka Events Package - Extraction Plan

## Overview
This document outlines the comprehensive plan for extracting the Kafka implementation from the existing microservice template into a reusable NestJS package. The goal is to preserve valuable code while simplifying overly complex features.

## Current State Analysis

### Existing Files to Extract From
```
../../src/events/
├── decorators/
│   └── retryable-event-pattern.decorator.ts    # Extract simplified version
├── interceptors/
│   └── kafka-retry.interceptor.ts             # Simplify and extract
├── kafka-enhanced/
│   ├── dlq-processor.service.ts               # Simplify significantly
│   ├── kafka-enhanced.service.ts              # Extract core functionality
│   └── retry-topic-consumer.service.ts        # Merge into core
├── services/
│   ├── kafka-producer.service.ts              # Extract as-is (valuable)
│   └── retry-wrapper.service.ts               # Remove (duplicate logic)
├── test/
│   ├── event-processing-tracker.service.ts    # Move to test utilities
│   └── kafka-retry-test.controller.ts         # Move to test utilities
├── event-consumer.controller.ts               # Extract base class
├── kafka-enhanced.controller.ts               # Extract simplified version
├── kafka.bootstrap.ts                         # Extract as-is (valuable)
├── kafka.interfaces.ts                        # Extract simplified
└── kafka.module.ts                           # Extract as main module
```

## Package Structure

### Target Directory Structure
```
src/
├── core/
│   ├── kafka.module.ts                    # Main module
│   ├── kafka.bootstrap.ts                 # Connection management
│   ├── kafka.producer.ts                  # Producer service
│   ├── kafka.consumer.ts                  # Consumer base
│   └── kafka.controller.ts                # Base controller
├── decorators/
│   ├── event-handler.decorator.ts         # Simplified decorator
│   └── event-pattern.decorator.ts         # Pattern matching
├── interceptors/
│   └── retry.interceptor.ts               # Single retry mechanism
├── interfaces/
│   ├── kafka.interfaces.ts                # Core interfaces
│   ├── retry.interfaces.ts                # Retry configuration
│   └── plugin.interfaces.ts               # Plugin architecture
├── plugins/
│   ├── dlq/
│   │   ├── dlq.plugin.ts                  # Simple DLQ plugin
│   │   └── dlq.interfaces.ts              # DLQ configuration
│   └── telemetry/
│       ├── telemetry.plugin.ts            # OpenTelemetry plugin
│       └── telemetry.interfaces.ts        # Telemetry configuration
├── utils/
│   ├── retry.utils.ts                     # Retry calculation logic
│   └── topic.utils.ts                     # Topic naming utilities
└── index.ts                               # Public API exports
```

## Extraction Details

### 1. Core Files to Extract (High Value)

#### `kafka.bootstrap.ts` → `src/core/kafka.bootstrap.ts`
**Source:** `../../src/events/kafka.bootstrap.ts`
**Action:** Extract as-is with minor cleanup
**Value:** Solid connection management and configuration loading
**Changes:**
- Remove test-specific configurations
- Make configuration more generic
- Add plugin support hooks

#### `kafka-producer.service.ts` → `src/core/kafka.producer.ts`
**Source:** `../../src/events/services/kafka-producer.service.ts`
**Action:** Extract with minimal changes
**Value:** Production-ready producer with batching and error handling
**Changes:**
- Remove hardcoded service name references
- Make telemetry optional (plugin-based)
- Simplify configuration injection

#### `kafka.interfaces.ts` → `src/interfaces/kafka.interfaces.ts`
**Source:** `../../src/events/kafka.interfaces.ts`
**Action:** Extract and simplify
**Value:** Well-defined interfaces for configuration
**Changes:**
- Remove complex DLQ configuration
- Simplify retry configuration
- Add plugin interface definitions

### 2. Files to Significantly Simplify

#### Event Handler Decorators
**Source:** `../../src/events/decorators/retryable-event-pattern.decorator.ts`
**Target:** `src/decorators/event-handler.decorator.ts`
**Current Issues:**
- Complex method wrapping logic (lines 119-237)
- Overlaps with interceptor retry logic
- Tight coupling to specific retry implementations

**Simplification Plan:**
- Remove method wrapping entirely
- Keep only metadata decoration
- Let interceptor handle all retry logic
- Simple API: `@EventHandler('topic', { retry: { attempts: 3 } })`

#### Retry Interceptor
**Source:** `../../src/events/interceptors/kafka-retry.interceptor.ts`
**Target:** `src/interceptors/retry.interceptor.ts`
**Current Issues:**
- Complex interaction with multiple retry services
- In-memory state management
- Overlapping with decorator logic

**Simplification Plan:**
- Single source of truth for retry logic
- Stateless operation (no in-memory counters)
- Simple exponential backoff calculation
- Clean error handling and DLQ integration

#### DLQ Implementation
**Source:** `../../src/events/kafka-enhanced/dlq-processor.service.ts`
**Target:** `src/plugins/dlq/dlq.plugin.ts`
**Current Issues:**
- Over-engineered with leader election
- Complex background processing
- Automatic replay mechanisms
- Per-consumer-group isolation

**Simplification Plan:**
- Simple dead-letter publishing only
- No background processing
- No automatic replay (manual via API)
- Plugin architecture for opt-in usage

### 3. Files to Remove/Merge

#### Remove Entirely
- `retry-wrapper.service.ts` - Duplicate retry logic
- `kafka-enhanced-dlq.module.ts` - Over-complex DLQ module
- `dlq-handler.service.ts` - Merged into simplified DLQ plugin

#### Test Files → Separate Test Package
- `event-processing-tracker.service.ts` → `test/utilities/event-tracker.service.ts`
- `kafka-retry-test.controller.ts` → `test/utilities/test-controllers.ts`

## Simplification Strategy

### 1. Retry Mechanism Consolidation

**Current State (4 overlapping mechanisms):**
1. Decorator method wrapper (`retryable-event-pattern.decorator.ts:119-237`)
2. Interceptor retry (`kafka-retry.interceptor.ts`)
3. Retry wrapper service (`retry-wrapper.service.ts`)
4. Generic retry manager (used by multiple services)

**Target State (1 clean mechanism):**
- Single interceptor-based retry
- Decorator only provides metadata
- Stateless operation
- Simple configuration

**Implementation:**
```typescript
// Simple decorator
@EventHandler('order.created', {
  retry: { attempts: 3, backoff: 'exponential', maxDelay: 30000 }
})

// Single interceptor handles all retry logic
export class RetryInterceptor implements NestInterceptor {
  // Clean, stateless retry implementation
}
```

### 2. DLQ Simplification

**Current State:**
- Complex processor with leader election
- Background retry mechanisms
- Automatic message replay
- Per-consumer-group retry topics

**Target State:**
- Simple dead-letter publishing
- Optional plugin architecture
- Manual replay via API
- Single DLQ topic for all failures

**Implementation:**
```typescript
// Simple DLQ plugin
export class DLQPlugin {
  async handleFailure(message: any, error: Error): Promise<void> {
    // Just publish to DLQ topic with metadata
  }
}
```

### 3. Configuration Simplification

**Current State:**
- Multiple configuration levels
- Complex nested objects
- Legacy and new config coexistence

**Target State:**
- Single, flat configuration object
- Sensible defaults
- Optional plugin configurations

**Implementation:**
```typescript
interface KafkaModuleOptions {
  client: KafkaConfig;
  consumer?: ConsumerConfig;
  producer?: ProducerConfig;
  retry?: {
    enabled: boolean;
    attempts: number;
    backoff: 'linear' | 'exponential';
  };
  plugins?: KafkaPlugin[];
}
```

## Implementation Phases

### Phase 1: Core Setup (Week 1)
1. **Setup package structure**
   - Configure package.json for library
   - Configure TypeScript for library
   - Setup build pipeline

2. **Extract core services**
   - Extract `kafka.bootstrap.ts` (minimal changes)
   - Extract `kafka-producer.service.ts` (cleanup)
   - Create main `kafka.module.ts`

3. **Basic functionality test**
   - Simple producer/consumer setup
   - Basic configuration loading
   - Connection management

### Phase 2: Decorators and Interceptors (Week 2)
1. **Simplify event handlers**
   - Extract decorator (metadata only)
   - Remove method wrapping complexity
   - Create base controller class

2. **Consolidate retry logic**
   - Single retry interceptor
   - Remove duplicate mechanisms
   - Clean error handling

3. **Testing**
   - Unit tests for core functionality
   - Integration tests with test Kafka

### Phase 3: Plugin Architecture (Week 3)
1. **Create plugin system**
   - Plugin interface definition
   - Plugin registration mechanism
   - Lifecycle hooks

2. **DLQ Plugin**
   - Simple dead-letter publishing
   - Configuration options
   - Error handling

3. **Telemetry Plugin**
   - Extract OpenTelemetry integration
   - Optional activation
   - Performance metrics

### Phase 4: Documentation and Migration (Week 4)
1. **Documentation**
   - API documentation
   - Usage examples
   - Migration guide

2. **Migration testing**
   - Test in existing microservice
   - Performance validation
   - Feature parity check

## Migration Strategy

### Gradual Migration Approach
1. **Install new package alongside existing code**
2. **Migrate handlers one by one**
   - Change decorator imports
   - Update configuration
   - Test functionality
3. **Remove old implementation after full migration**

### Breaking Changes
- Decorator API simplified
- Configuration structure changed
- Some advanced DLQ features removed (available as separate plugins)

### Backward Compatibility
- Provide adapter for old configuration format
- Gradual deprecation warnings
- Migration utility scripts

## Success Criteria

### Package Quality
- Clean, simple API
- Comprehensive documentation
- Full test coverage
- TypeScript strict mode compliance

### Performance
- No performance regression
- Memory usage optimization
- Connection pooling efficiency

### Developer Experience
- Easy installation and setup
- Clear error messages
- Intuitive configuration
- Good IDE support

### Maintainability
- Clean separation of concerns
- Plugin architecture for extensions
- Minimal dependencies
- Clear contribution guidelines

## Risk Mitigation

### Technical Risks
- **Risk:** Lost functionality during simplification
- **Mitigation:** Comprehensive testing and gradual migration

### Adoption Risks
- **Risk:** Teams resist migration to new package
- **Mitigation:** Provide clear migration path and benefits documentation

### Maintenance Risks
- **Risk:** Package becomes complex again over time
- **Mitigation:** Establish clear contribution guidelines and architectural principles