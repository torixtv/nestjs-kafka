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

<!-- greptile-rounds:start. The same block lives in AGENTS.md (Codex and other agents) and CLAUDE.md (Claude Code) in every torixtv repository; change both together. -->
## Greptile review rounds

Greptile never reviews a pull request on its own: since 2026-10-25 the organization's automatic
reviews are off, so opening a PR, marking it ready or pushing to it starts nothing. Every review,
the first included, costs $1 and starts only when someone comments `@greptileai` on the PR. A PR that
needs no review (docs only, lockfiles only, a back-merge, a revert) simply never gets that comment
and merges on green CI and the usual human review. A reviewed PR is ready to merge when Greptile's
summary comment shows 5/5, no Greptile thread is left without a reply, and the summary's "Last
reviewed commit" is the PR's head commit. Measured over the month to 2026-10-23: 91% of repeat
reviews came from pushes to PRs that were already open for review, and a quarter of PRs took three
or more reviews. These rules replace that with rounds you start on purpose.

1. Open the PR as a draft and keep it a draft while you work. When the change is complete and CI is
   green, mark it ready and comment `@greptileai` once. That is round 1.
2. Labels do not start or stop reviews. Adding `greptile` to an open PR does nothing, and
   `skip-greptile` does not block a comment request; the comment is the only trigger.
3. While a review is running (the "Greptile Review" check is in progress), do not push and do not
   comment `@greptileai`.
4. When a review lands, read every finding in the summary before changing anything. Fix the whole
   set, run the repository's checks, and push. Reply on every Greptile thread, including findings
   you decline, saying what changed or why not.
5. Then comment `@greptileai` once to start the next round. Never request a round while CI is red,
   while a Greptile thread has no reply, or when nothing changed since the last review.
6. Stop when the summary shows 5/5 with no open threads on the head commit. Do not push after that
   unless the change needs it: any push means another round before merge.
7. If round 3 still has findings, stop and ask a person before requesting a fourth.
8. Never write `at plus` or `at apex` in a request (3 and 10 credits), never run `/greploop`, and
   never close and reopen a PR to get a review.
9. Write the mention in no comment but the one request: not in a thread reply, a review or a PR
   description, not even quoted in backticks. Greptile reads any comment that carries it as a
   request, tier words included. On 2026-10-24 six replies that quoted the tier wording to explain
   a fix each bought an unplanned apex review.

`node .claude/hooks/greptile-round-guard.mjs status [torixtv/<repo> <number>]` checks a PR against
these rules and prints what to do next; any agent can run it. In Claude Code the `greptile-round`
skill wraps it, and a PreToolUse hook in `.claude/settings.json` blocks an `@greptileai` comment
that breaks them.
<!-- greptile-rounds:end -->
