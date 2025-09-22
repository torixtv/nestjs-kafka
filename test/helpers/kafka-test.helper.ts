import { Kafka, Consumer, Producer, Admin } from 'kafkajs';
import { randomUUID } from 'crypto';

export interface KafkaTestConfig {
  brokers: string[];
  clientId: string;
  groupId: string;
}

export class KafkaTestHelper {
  private kafka: Kafka;
  private admin: Admin;
  private consumers: Consumer[] = [];
  private producers: Producer[] = [];

  constructor(private config: KafkaTestConfig) {
    this.kafka = new Kafka({
      clientId: config.clientId,
      brokers: config.brokers,
      retry: {
        initialRetryTime: 100,
        retries: 3,
      },
    });

    this.admin = this.kafka.admin();
  }

  async connect(): Promise<void> {
    await this.admin.connect();
  }

  async disconnect(): Promise<void> {
    // Disconnect all consumers and producers
    await Promise.all([
      ...this.consumers.map(consumer => consumer.disconnect()),
      ...this.producers.map(producer => producer.disconnect()),
      this.admin.disconnect(),
    ]);

    this.consumers = [];
    this.producers = [];
  }

  async createTopics(topics: (string | { topic: string; numPartitions?: number; replicationFactor?: number })[]): Promise<void> {
    const existingTopics = await this.admin.listTopics();

    const topicsToCreate = topics
      .map(t => typeof t === 'string' ? { topic: t, numPartitions: 1, replicationFactor: 1 } : { numPartitions: 1, replicationFactor: 1, ...t })
      .filter(t => !existingTopics.includes(t.topic));

    if (topicsToCreate.length > 0) {
      await this.admin.createTopics({
        topics: topicsToCreate,
      });
    }
  }

  async deleteTopics(topics: string[]): Promise<void> {
    const existingTopics = await this.admin.listTopics();
    const topicsToDelete = topics.filter(topic => existingTopics.includes(topic));

    if (topicsToDelete.length > 0) {
      await this.admin.deleteTopics({
        topics: topicsToDelete,
      });
    }
  }

  async createProducer(): Promise<Producer> {
    const producer = this.kafka.producer({
      idempotent: true,
    });

    await producer.connect();
    this.producers.push(producer);
    return producer;
  }

  async createConsumer(groupId?: string): Promise<Consumer> {
    const consumer = this.kafka.consumer({
      groupId: groupId || `${this.config.groupId}-${randomUUID()}`,
      heartbeatInterval: 1000,
      sessionTimeout: 6000,
    });

    await consumer.connect();
    this.consumers.push(consumer);
    return consumer;
  }

  async waitForMessages<T = any>(
    topic: string,
    options: {
      expectedCount?: number;
      timeout?: number;
      groupId?: string;
      fromBeginning?: boolean;
      includeHeaders?: boolean;
    } = {}
  ): Promise<T[]> {
    const {
      expectedCount = 1,
      timeout = 10000,
      groupId,
      fromBeginning = true,
      includeHeaders = false,
    } = options;

    const consumer = await this.createConsumer(groupId);
    const messages: T[] = [];

    return new Promise((resolve, reject) => {
      const timeoutHandle = setTimeout(() => {
        reject(new Error(`Timeout waiting for ${expectedCount} messages from topic ${topic}`));
      }, timeout);

      consumer.subscribe({ topic, fromBeginning });

      consumer.run({
        eachMessage: async ({ message }) => {
          try {
            const value = message.value ? JSON.parse(message.value.toString()) : null;

            if (includeHeaders) {
              // Convert headers from Buffer to string
              const headers: Record<string, string> = {};
              if (message.headers) {
                Object.entries(message.headers).forEach(([key, buffer]) => {
                  if (buffer) {
                    headers[key] = buffer.toString();
                  }
                });
              }

              messages.push({
                value,
                headers,
                key: message.key?.toString(),
                timestamp: message.timestamp,
                offset: message.offset,
              } as T);
            } else {
              messages.push(value);
            }

            if (messages.length >= expectedCount) {
              clearTimeout(timeoutHandle);
              resolve(messages);
            }
          } catch (error) {
            clearTimeout(timeoutHandle);
            reject(error);
          }
        },
      });
    });
  }

  async sendTestMessage(
    topic: string,
    message: any,
    options: {
      key?: string;
      headers?: Record<string, string>;
    } = {}
  ): Promise<void> {
    const producer = await this.createProducer();

    await producer.send({
      topic,
      messages: [
        {
          key: options.key,
          value: JSON.stringify(message),
          headers: options.headers,
        },
      ],
    });
  }

  generateTestTopic(prefix: string = 'test'): string {
    return `${prefix}-${randomUUID()}`;
  }

  static getDefaultConfig(): KafkaTestConfig {
    return {
      brokers: ['localhost:9092'],
      clientId: `test-client-${randomUUID()}`,
      groupId: `test-group-${randomUUID()}`,
    };
  }

  async isKafkaAvailable(): Promise<boolean> {
    try {
      await this.admin.connect();
      await this.admin.listTopics();
      return true;
    } catch (error) {
      return false;
    }
  }

  async waitForKafka(timeout: number = 30000): Promise<void> {
    const start = Date.now();

    while (Date.now() - start < timeout) {
      if (await this.isKafkaAvailable()) {
        return;
      }
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    throw new Error(`Kafka not available after ${timeout}ms`);
  }

  async getTopicMetadata(topic: string) {
    const metadata = await this.admin.fetchTopicMetadata({ topics: [topic] });
    return metadata.topics.find(t => t.name === topic);
  }
}