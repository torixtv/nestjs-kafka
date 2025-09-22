# Kafka Retry Mechanism Example

This example demonstrates the Kafka retry mechanism with full NestJS lifecycle support, showcasing how the `@torix/kafka-events` package works in a real application environment.

## ✨ Features Demonstrated

- **Complete NestJS Application**: Real application with proper lifecycle hooks (`OnModuleInit`, `OnApplicationBootstrap`)
- **Kafka Event Handlers**: Multiple event handlers with different retry scenarios
- **Bootstrap Service Integration**: Centralized lifecycle management for Kafka components
- **Real-time Monitoring**: HTTP endpoints for health checks, metrics, and debugging
- **Docker Compose Setup**: Full stack with RedPanda (Kafka) and web UI
- **E2E Testing**: Tests against the running service to verify retry behavior

## 🚀 Quick Start

### Prerequisites

- Node.js 18+
- Docker and Docker Compose
- npm or yarn

### 1. Install Dependencies

```bash
cd example
npm install
```

### 2. Start Kafka (RedPanda)

```bash
npm run docker:up
```

This starts:
- **RedPanda** (Kafka-compatible) on `localhost:9092`
- **RedPanda Console** (Web UI) on `http://localhost:8080`

### 3. Start the Application

```bash
npm run start:dev
```

The application will be available at `http://localhost:3000`

## 📡 API Endpoints

### Health & Monitoring

- `GET /health` - Application health status
- `GET /metrics` - Processing metrics and statistics
- `GET /debug` - Detailed debugging information
- `GET /messages` - All processed messages
- `GET /stats` - Processing statistics

### Testing

- `POST /test/send` - Send test messages
- `POST /reset` - Reset application state

## 🧪 Testing the Retry Mechanism

### Scenario 1: Immediate Success

```bash
curl -X POST http://localhost:3000/test/send \
  -H "Content-Type: application/json" \
  -d '{"scenario": "immediate"}'
```

### Scenario 2: Retry with Eventual Success

```bash
curl -X POST http://localhost:3000/test/send \
  -H "Content-Type: application/json" \
  -d '{"scenario": "retry"}'
```

This message will:
1. **Fail** on attempt 1 (after 0s)
2. **Fail** on attempt 2 (after 2s delay)
3. **Succeed** on attempt 3 (after 4s additional delay)

### Scenario 3: Max Retries Exceeded

```bash
curl -X POST http://localhost:3000/test/send \
  -H "Content-Type: application/json" \
  -d '{"scenario": "fail"}'
```

This message will fail all retry attempts.

### Custom Message

```bash
curl -X POST http://localhost:3000/test/send \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "example.manual.test",
    "payload": {
      "id": "custom-123",
      "action": "success",
      "data": {"key": "value"}
    }
  }'
```

## 📊 Monitoring the Results

### Check Processing Status

```bash
# Get all messages
curl http://localhost:3000/messages

# Get statistics
curl http://localhost:3000/stats

# Get detailed metrics
curl http://localhost:3000/metrics
```

### RedPanda Console

Visit `http://localhost:8080` to view:
- Topic messages
- Consumer groups
- Retry topic contents

## 🔍 Key Components Demonstrated

### 1. Bootstrap Service Integration

The application demonstrates proper initialization order:

```typescript
// In main.ts - Real NestJS lifecycle
await startKafkaMicroservice(app); // Triggers bootstrap service
```

### 2. Event Handlers with Retry Configuration

```typescript
@EventHandler('example.retry.success', {
  retry: {
    enabled: true,
    attempts: 3,
    baseDelay: 2000,
    maxDelay: 8000,
    backoff: 'exponential',
  },
})
async handleRetrySuccess(payload: any) {
  // Fails 2 times, succeeds on 3rd attempt
}
```

### 3. Monitoring and Debugging

Real-time visibility into:
- Handler registry status
- Retry consumer metrics
- Bootstrap service initialization
- Message processing statistics

## 🧪 Running E2E Tests

The E2E tests run against the live application:

```bash
# Start dependencies
npm run docker:up

# Run E2E tests (in separate terminal)
npm run test:e2e
```

Tests verify:
- ✅ Bootstrap service proper initialization
- ✅ Handler registration and discovery
- ✅ Retry mechanism with exponential backoff
- ✅ Max retries exceeded scenarios
- ✅ Real-time metrics and monitoring

## 🐳 Docker Deployment

### Build and Run with Docker

```bash
# Build the application
docker build -t kafka-retry-example .

# Run the full stack
npm run docker:up
```

### View Logs

```bash
# Application logs
docker logs kafka-retry-example-app -f

# Kafka logs
docker logs kafka-retry-example-redpanda -f
```

## 🔧 Configuration

### Environment Variables

- `KAFKA_BROKERS` - Kafka broker addresses (default: `localhost:9092`)
- `PORT` - HTTP server port (default: `3000`)
- `NODE_ENV` - Environment mode

### Kafka Configuration

```typescript
// In app.module.ts
KafkaModule.forRoot({
  client: {
    clientId: 'kafka-retry-example',
    brokers: ['localhost:9092'],
  },
  retry: {
    enabled: true,
    attempts: 3,
    baseDelay: 2000,
    maxDelay: 10000,
    backoff: 'exponential',
  },
})
```

## 🔍 Debugging

### Check Bootstrap Service Status

```bash
curl http://localhost:3000/debug | jq '.bootstrap'
```

### Monitor Retry Consumer

```bash
curl http://localhost:3000/health | jq '.kafka.retryConsumer'
```

### View Handler Registry

```bash
curl http://localhost:3000/debug | jq '.handlers'
```

## 🧹 Cleanup

```bash
# Stop all services
npm run docker:down

# Remove volumes (optional)
docker volume prune
```

## 📈 Expected Results

When testing the retry mechanism, you should observe:

1. **Immediate Success**: Processed in ~100ms
2. **Retry Success**: Takes ~6+ seconds total (2s + 4s delays)
3. **Always Fail**: Takes ~3+ seconds, then exhausts retries

The application logs and metrics endpoints provide real-time visibility into the retry process, demonstrating that the bootstrap service correctly initializes all Kafka components and the retry mechanism works as expected in a real NestJS application environment.

## 🎯 Why This Approach Works

Unlike integration tests that use `Test.createTestingModule()`, this example:

- ✅ **Uses `NestFactory.create()`** - Full NestJS application lifecycle
- ✅ **Triggers `OnApplicationBootstrap`** - Bootstrap service runs automatically
- ✅ **Real microservice registration** - Actual Kafka consumer groups
- ✅ **Production-like environment** - Same code paths as production
- ✅ **Observable behavior** - HTTP endpoints for real-time monitoring

This proves that the bootstrap service architecture successfully coordinates the Kafka retry mechanism in real-world scenarios.