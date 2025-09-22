import { Injectable, Logger, Inject, OnModuleInit } from '@nestjs/common';
import { Kafka, Admin, ITopicConfig } from 'kafkajs';
import {
  KAFKAJS_INSTANCE,
  KAFKA_MODULE_OPTIONS,
} from '../core/kafka.constants';
import { KafkaModuleOptions } from '../interfaces/kafka.interfaces';

@Injectable()
export class KafkaRetryManager implements OnModuleInit {
  private readonly logger = new Logger(KafkaRetryManager.name);
  private admin: Admin;
  private retryTopicName: string;

  constructor(
    @Inject(KAFKAJS_INSTANCE) private readonly kafka: Kafka,
    @Inject(KAFKA_MODULE_OPTIONS) private readonly options: KafkaModuleOptions,
  ) {
    this.admin = this.kafka.admin();
    this.retryTopicName = this.buildRetryTopicName();
  }

  async onModuleInit(): Promise<void> {
    await this.ensureRetryTopicExists();
  }

  private buildRetryTopicName(): string {
    const clientId = this.options.client?.clientId || 'kafka-service';
    return `${clientId}.retry`;
  }

  /**
   * Ensure the retry topic exists, create it if it doesn't
   */
  private async ensureRetryTopicExists(): Promise<void> {
    try {
      await this.admin.connect();

      const existingTopics = await this.admin.listTopics();

      if (existingTopics.includes(this.retryTopicName)) {
        this.logger.debug(`Retry topic already exists: ${this.retryTopicName}`);
        return;
      }

      const topicConfig: ITopicConfig = {
        topic: this.retryTopicName,
        numPartitions: this.options.retry?.topicPartitions || 3,
        replicationFactor: this.options.retry?.topicReplicationFactor || 1,
        configEntries: [
          // Enable log compaction to prevent topic from growing indefinitely
          {
            name: 'cleanup.policy',
            value: 'delete',
          },
          // Set retention to 24 hours (messages older than this will be deleted)
          {
            name: 'retention.ms',
            value: String(
              this.options.retry?.topicRetentionMs || 24 * 60 * 60 * 1000,
            ),
          },
          // Set segment size for efficient processing
          {
            name: 'segment.ms',
            value: String(this.options.retry?.topicSegmentMs || 60 * 60 * 1000),
          },
        ],
      };

      await this.admin.createTopics({
        topics: [topicConfig],
        waitForLeaders: true,
      });

      this.logger.log(`Created retry topic: ${this.retryTopicName}`);
    } catch (error) {
      this.logger.error(
        `Failed to ensure retry topic exists: ${this.retryTopicName}`,
        error,
      );
      throw error;
    } finally {
      await this.admin.disconnect();
    }
  }

  /**
   * Get the retry topic name
   */
  getRetryTopicName(): string {
    return this.retryTopicName;
  }

  /**
   * Delete the retry topic (mainly for testing)
   */
  async deleteRetryTopic(): Promise<void> {
    try {
      await this.admin.connect();
      await this.admin.deleteTopics({
        topics: [this.retryTopicName],
        timeout: 30000,
      });
      this.logger.log(`Deleted retry topic: ${this.retryTopicName}`);
    } catch (error) {
      this.logger.error(
        `Failed to delete retry topic: ${this.retryTopicName}`,
        error,
      );
      throw error;
    } finally {
      await this.admin.disconnect();
    }
  }

  /**
   * Get topic metadata
   */
  async getTopicMetadata(): Promise<any> {
    try {
      await this.admin.connect();
      const metadata = await this.admin.fetchTopicMetadata({
        topics: [this.retryTopicName],
      });
      return metadata.topics.find(
        (topic) => topic.name === this.retryTopicName,
      );
    } catch (error) {
      this.logger.error(
        `Failed to fetch topic metadata: ${this.retryTopicName}`,
        error,
      );
      throw error;
    } finally {
      await this.admin.disconnect();
    }
  }

  /**
   * Check if retry topic exists
   */
  async retryTopicExists(): Promise<boolean> {
    try {
      await this.admin.connect();
      const topics = await this.admin.listTopics();
      return topics.includes(this.retryTopicName);
    } catch (error) {
      this.logger.error(
        `Failed to check if retry topic exists: ${this.retryTopicName}`,
        error,
      );
      return false;
    } finally {
      await this.admin.disconnect();
    }
  }
}
