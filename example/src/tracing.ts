import { NodeSDK } from '@opentelemetry/sdk-node';
import { ConsoleSpanExporter } from '@opentelemetry/sdk-trace-node';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { Resource } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions';

// Initialize OpenTelemetry
const sdk = new NodeSDK({
  resource: new Resource({
    [ATTR_SERVICE_NAME]: 'kafka-retry-example',
    [ATTR_SERVICE_VERSION]: '1.0.0',
  }),
  traceExporter: new ConsoleSpanExporter(),
  instrumentations: [
    getNodeAutoInstrumentations({
      // Enable Kafka instrumentation
      '@opentelemetry/instrumentation-kafkajs': {
        enabled: true,
      },
      // Enable HTTP instrumentation
      '@opentelemetry/instrumentation-http': {
        enabled: true,
      },
      // Disable fs instrumentation to reduce noise
      '@opentelemetry/instrumentation-fs': {
        enabled: false,
      },
    }),
  ],
});

// Start the SDK
sdk.start();

console.log('🔍 OpenTelemetry tracing initialized');
console.log('📡 Traces will be exported to: console');

// Graceful shutdown
process.on('SIGTERM', () => {
  sdk.shutdown()
    .then(() => console.log('✅ OpenTelemetry terminated'))
    .catch((error) => console.log('❌ Error terminating OpenTelemetry', error))
    .finally(() => process.exit(0));
});

export default sdk;