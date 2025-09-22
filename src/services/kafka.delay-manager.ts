import { Injectable, Logger } from '@nestjs/common';
import { Consumer } from 'kafkajs';

export interface DelayedMessage {
  messageId: string;
  resumeAt: Date;
  partition: number;
  topic: string;
}

@Injectable()
export class KafkaDelayManager {
  private readonly logger = new Logger(KafkaDelayManager.name);
  private readonly delayedMessages = new Map<string, DelayedMessage>();
  private readonly partitionTimers = new Map<string, NodeJS.Timeout>();

  /**
   * Schedule a message to be processed after a delay using Kafka's pause/resume
   */
  async scheduleMessageDelay(
    consumer: Consumer,
    messageId: string,
    delayMs: number,
    partition: number,
    topic: string,
  ): Promise<void> {
    const resumeAt = new Date(Date.now() + delayMs);
    const partitionKey = `${topic}-${partition}`;

    // Store the delayed message
    const delayedMessage: DelayedMessage = {
      messageId,
      resumeAt,
      partition,
      topic,
    };

    this.delayedMessages.set(messageId, delayedMessage);

    // Pause the specific topic-partition
    await consumer.pause([{ topic, partitions: [partition] }]);

    this.logger.debug(
      `Paused partition ${partition} for topic ${topic}, message ${messageId} will resume at ${resumeAt.toISOString()}`,
    );

    // Clear any existing timer for this partition
    const existingTimer = this.partitionTimers.get(partitionKey);
    if (existingTimer) {
      clearTimeout(existingTimer);
    }

    // Set a timer to resume the partition
    const timer = setTimeout(async () => {
      await this.resumePartition(consumer, topic, partition, messageId);
    }, delayMs);

    this.partitionTimers.set(partitionKey, timer);
  }

  /**
   * Resume a paused partition
   */
  private async resumePartition(
    consumer: Consumer,
    topic: string,
    partition: number,
    messageId: string,
  ): Promise<void> {
    const partitionKey = `${topic}-${partition}`;

    try {
      // Resume the specific topic-partition
      await consumer.resume([{ topic, partitions: [partition] }]);

      this.logger.debug(
        `Resumed partition ${partition} for topic ${topic}, processing message ${messageId}`,
      );

      // Clean up
      this.delayedMessages.delete(messageId);
      this.partitionTimers.delete(partitionKey);
    } catch (error) {
      this.logger.error(
        `Failed to resume partition ${partition} for topic ${topic}:`,
        error,
      );
    }
  }

  /**
   * Get all delayed messages
   */
  getDelayedMessages(): DelayedMessage[] {
    return Array.from(this.delayedMessages.values());
  }

  /**
   * Get delayed message by ID
   */
  getDelayedMessage(messageId: string): DelayedMessage | undefined {
    return this.delayedMessages.get(messageId);
  }

  /**
   * Check if a message is currently delayed
   */
  isMessageDelayed(messageId: string): boolean {
    return this.delayedMessages.has(messageId);
  }

  /**
   * Cancel a delayed message (useful for testing or cleanup)
   */
  async cancelDelay(consumer: Consumer, messageId: string): Promise<boolean> {
    const delayedMessage = this.delayedMessages.get(messageId);
    if (!delayedMessage) {
      return false;
    }

    const partitionKey = `${delayedMessage.topic}-${delayedMessage.partition}`;

    // Clear the timer
    const timer = this.partitionTimers.get(partitionKey);
    if (timer) {
      clearTimeout(timer);
      this.partitionTimers.delete(partitionKey);
    }

    // Resume the partition
    await consumer.resume([
      {
        topic: delayedMessage.topic,
        partitions: [delayedMessage.partition],
      },
    ]);

    // Clean up
    this.delayedMessages.delete(messageId);

    this.logger.debug(`Cancelled delay for message ${messageId}`);
    return true;
  }

  /**
   * Get active timers count (useful for monitoring)
   */
  getActiveTimersCount(): number {
    return this.partitionTimers.size;
  }

  /**
   * Get delayed messages count (useful for monitoring)
   */
  getDelayedMessagesCount(): number {
    return this.delayedMessages.size;
  }

  /**
   * Cleanup all timers (called on module destroy)
   */
  cleanup(): void {
    for (const timer of this.partitionTimers.values()) {
      clearTimeout(timer);
    }

    this.partitionTimers.clear();
    this.delayedMessages.clear();

    this.logger.debug('Cleaned up all delay timers');
  }
}
