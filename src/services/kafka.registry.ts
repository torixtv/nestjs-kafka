import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { DiscoveryService, MetadataScanner, Reflector } from '@nestjs/core';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { EVENT_HANDLER_METADATA } from '../core/kafka.constants';
import { EventHandlerMetadata } from '../interfaces/kafka.interfaces';

export interface RegisteredHandler {
  instance: any;
  method: (payload: any) => Promise<any>;
  methodName: string;
  pattern: string;
  metadata: EventHandlerMetadata;
  handlerId: string;
}

@Injectable()
export class KafkaHandlerRegistry implements OnModuleInit {
  private readonly logger = new Logger(KafkaHandlerRegistry.name);
  private readonly handlers = new Map<string, RegisteredHandler>();

  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly metadataScanner: MetadataScanner,
    private readonly reflector: Reflector,
  ) {}

  async onModuleInit(): Promise<void> {
    // Don't auto-discover - let bootstrap service handle this
  }

  async discoverHandlers(): Promise<void> {
    const instanceWrappers: InstanceWrapper[] = [
      ...this.discoveryService.getControllers(),
      ...this.discoveryService.getProviders(),
    ];

    for (const wrapper of instanceWrappers) {
      const { instance } = wrapper;
      if (!instance || typeof instance !== 'object') {
        continue;
      }

      const prototype = Object.getPrototypeOf(instance);
      if (!prototype) {
        continue;
      }

      this.metadataScanner.scanFromPrototype(
        instance,
        prototype,
        (methodName: string) =>
          this.handleMethodDiscovery(instance, methodName),
      );
    }

    this.logger.log(`Discovered ${this.handlers.size} Kafka event handlers`);
  }

  private handleMethodDiscovery(instance: any, methodName: string): void {
    const metadata = this.reflector.get(
      EVENT_HANDLER_METADATA,
      instance[methodName],
    );

    if (!metadata) {
      return;
    }

    const handlerId = this.createHandlerId(instance, methodName);
    const registeredHandler: RegisteredHandler = {
      instance,
      method: instance[methodName].bind(instance),
      methodName,
      pattern: metadata.pattern,
      metadata,
      handlerId,
    };

    this.handlers.set(handlerId, registeredHandler);
  }

  private createHandlerId(instance: any, methodName: string): string {
    const className = instance.constructor.name;
    return `${className}.${methodName}`;
  }

  /**
   * Get handler by its unique ID
   */
  getHandler(handlerId: string): RegisteredHandler | undefined {
    return this.handlers.get(handlerId);
  }

  /**
   * Get all handlers for a specific topic pattern
   */
  getHandlersForPattern(pattern: string): RegisteredHandler[] {
    return Array.from(this.handlers.values()).filter(
      (handler) => handler.pattern === pattern,
    );
  }

  /**
   * Get all registered handlers
   */
  getAllHandlers(): RegisteredHandler[] {
    return Array.from(this.handlers.values());
  }

  /**
   * Get all unique topic patterns
   */
  getAllPatterns(): string[] {
    const patterns = new Set<string>();
    for (const handler of this.handlers.values()) {
      patterns.add(handler.pattern);
    }
    return Array.from(patterns);
  }

  /**
   * Get handler for a specific topic (finds first matching pattern)
   */
  getHandlerByTopic(topic: string): RegisteredHandler | undefined {
    // First try exact match
    for (const handler of this.handlers.values()) {
      if (handler.pattern === topic) {
        return handler;
      }
    }

    // Could add pattern matching here for wildcards if needed
    return undefined;
  }

  /**
   * Get the total number of registered handlers
   */
  getHandlerCount(): number {
    return this.handlers.size;
  }

  /**
   * Execute a handler by its ID with the given payload
   */
  async executeHandler(handlerId: string, payload: any): Promise<any> {
    const handler = this.getHandler(handlerId);
    if (!handler) {
      throw new Error(`Handler not found: ${handlerId}`);
    }

    try {
      const result = await handler.instance[handler.methodName](payload);
      return result;
    } catch (error) {
      this.logger.error(`Handler execution failed: ${handlerId}`, error);
      throw error;
    }
  }

  /**
   * Check if handler exists
   */
  hasHandler(handlerId: string): boolean {
    return this.handlers.has(handlerId);
  }
}
