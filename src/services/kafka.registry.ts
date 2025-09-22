import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { DiscoveryService, MetadataScanner, Reflector } from '@nestjs/core';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { EVENT_HANDLER_METADATA } from '../core/kafka.constants';
import { EventHandlerMetadata } from '../interfaces/kafka.interfaces';

export interface RegisteredHandler {
  instance: any;
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
    await this.discoverHandlers();
  }

  private async discoverHandlers(): Promise<void> {
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

    console.log(
      `🔍 KafkaHandlerRegistry: Discovered ${this.handlers.size} Kafka event handlers`,
    );
    this.logger.log(`Discovered ${this.handlers.size} Kafka event handlers`);

    // Debug: Log all discovered handlers
    this.handlers.forEach((handler, handlerId) => {
      console.log(
        `🔍 Handler registered: ${handlerId} -> pattern: ${handler.pattern}`,
      );
      this.logger.debug(
        `Handler registered: ${handlerId} -> pattern: ${handler.pattern}`,
      );
    });
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
      methodName,
      pattern: metadata.pattern,
      metadata,
      handlerId,
    };

    this.handlers.set(handlerId, registeredHandler);
    this.logger.debug(
      `Registered handler: ${handlerId} for pattern: ${metadata.pattern}`,
    );
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
   * Execute a handler by its ID with the given payload
   */
  async executeHandler(handlerId: string, payload: any): Promise<any> {
    const handler = this.getHandler(handlerId);
    if (!handler) {
      throw new Error(`Handler not found: ${handlerId}`);
    }

    try {
      const result = await handler.instance[handler.methodName](payload);
      this.logger.debug(`Handler executed successfully: ${handlerId}`);
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
