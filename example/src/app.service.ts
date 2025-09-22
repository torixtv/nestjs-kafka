import { Injectable, Logger } from '@nestjs/common';

export interface ProcessedMessage {
  id: string;
  topic: string;
  payload: any;
  timestamp: string;
  attempt: number;
  processingTime: number;
  status: 'success' | 'failed';
  error?: string;
}

@Injectable()
export class AppService {
  private readonly logger = new Logger(AppService.name);
  private processedMessages: ProcessedMessage[] = [];
  private failureCounts = new Map<string, number>();
  private startTime = Date.now();

  /**
   * Record a successful message processing
   */
  recordSuccess(topic: string, payload: any): ProcessedMessage {
    const message: ProcessedMessage = {
      id: payload.id || `msg-${Date.now()}`,
      topic,
      payload,
      timestamp: new Date().toISOString(),
      attempt: this.getAttemptCount(payload.id) + 1,
      processingTime: Date.now() - this.startTime,
      status: 'success',
    };

    this.processedMessages.push(message);
    this.logger.log(`✅ Message processed successfully: ${topic} - ${message.id} (attempt ${message.attempt})`);

    return message;
  }

  /**
   * Record a failed message processing
   */
  recordFailure(topic: string, payload: any, error: string): ProcessedMessage {
    const attempt = this.incrementAttemptCount(payload.id);
    const message: ProcessedMessage = {
      id: payload.id || `msg-${Date.now()}`,
      topic,
      payload,
      timestamp: new Date().toISOString(),
      attempt,
      processingTime: Date.now() - this.startTime,
      status: 'failed',
      error,
    };

    this.processedMessages.push(message);
    this.logger.warn(`❌ Message processing failed: ${topic} - ${message.id} (attempt ${attempt}) - ${error}`);

    return message;
  }

  /**
   * Get current attempt count for a message ID
   */
  getAttemptCount(id: string): number {
    return this.failureCounts.get(id) || 0;
  }

  /**
   * Increment attempt count for a message ID
   */
  private incrementAttemptCount(id: string): number {
    const current = this.getAttemptCount(id);
    const newCount = current + 1;
    this.failureCounts.set(id, newCount);
    return newCount;
  }

  /**
   * Get all processed messages
   */
  getAllMessages(): ProcessedMessage[] {
    return [...this.processedMessages];
  }

  /**
   * Get messages by topic
   */
  getMessagesByTopic(topic: string): ProcessedMessage[] {
    return this.processedMessages.filter(msg => msg.topic === topic);
  }

  /**
   * Get successful messages
   */
  getSuccessfulMessages(): ProcessedMessage[] {
    return this.processedMessages.filter(msg => msg.status === 'success');
  }

  /**
   * Get failed messages
   */
  getFailedMessages(): ProcessedMessage[] {
    return this.processedMessages.filter(msg => msg.status === 'failed');
  }

  /**
   * Get statistics
   */
  getStats() {
    const total = this.processedMessages.length;
    const successful = this.getSuccessfulMessages().length;
    const failed = this.getFailedMessages().length;
    const topics = [...new Set(this.processedMessages.map(msg => msg.topic))];

    return {
      total,
      successful,
      failed,
      successRate: total > 0 ? (successful / total * 100).toFixed(2) + '%' : '0%',
      topics,
      averageAttempts: total > 0 ? (this.processedMessages.reduce((sum, msg) => sum + msg.attempt, 0) / total).toFixed(2) : '0',
      uniqueMessages: new Set(this.processedMessages.map(msg => msg.id)).size,
    };
  }

  /**
   * Reset all tracking data
   */
  reset(): void {
    this.processedMessages = [];
    this.failureCounts.clear();
    this.startTime = Date.now();
    this.logger.log('🔄 Message tracking data reset');
  }
}