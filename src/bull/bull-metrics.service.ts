import { InjectLogger, LoggerService } from '@app/logger';
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Queue } from 'bullmq';
import { BullMQMetricsFactory } from './bullmq-metrics.factory';
import { EVENT_TYPES } from './bull.enums';
import { QueueCreatedEvent } from './bull.interfaces';
import { BullQueuesService } from './bull-queues.service';

@Injectable()
export class BullMetricsService implements OnModuleInit, OnModuleDestroy {
  private readonly _queues: Record<string, Queue> = {};
  private readonly _updateTimeouts: Map<string, NodeJS.Timeout> = new Map();
  private readonly _lastMetrics: Map<string, any> = new Map();
  private readonly DEBOUNCE_MS = 10000; // 10 second debounce
  private readonly METRICS_CACHE_TTL_MS = 60000; // 1 minute cache TTL

  constructor(
    private readonly bullQueuesService: BullQueuesService,
    private readonly bullMQMetricsFactory: BullMQMetricsFactory,
    @InjectLogger(BullMetricsService)
    private readonly logger: LoggerService,
  ) {}

  onModuleInit() {
    this.bullQueuesService.on(EVENT_TYPES.QUEUE_CREATED, (event: QueueCreatedEvent) => {
      this.logger.debug(`Adding queue metrics: ${event.queueName}`);
      this._queues[event.queueName] = event.queue;
      this.bullMQMetricsFactory.create(event.queuePrefix, event.queueName, event.queue);
    });

    this.bullQueuesService.on(EVENT_TYPES.QUEUE_REMOVED, (queueName: string) => {
      this.logger.debug(`Removing queue metrics: ${queueName}`);
      delete this._queues[queueName];
      const timeout = this._updateTimeouts.get(queueName);
      if (timeout) {
        clearTimeout(timeout);
        this._updateTimeouts.delete(queueName);
      }
      this._lastMetrics.delete(queueName);
    });

    this.bullQueuesService.on(EVENT_TYPES.QUEUE_UPDATED, (event: { queuePrefix: string; queueName: string; counts: { completed: number; failed: number; delayed: number; active: number; waiting: number } }) => {
      const now = Date.now();
      const lastUpdate = this._lastMetrics.get(event.queueName);
      
      // Skip if we've updated recently and counts haven't changed significantly
      if (lastUpdate && 
          now - lastUpdate.timestamp < this.METRICS_CACHE_TTL_MS &&
          this.areCountsSimilar(lastUpdate.counts, event.counts)) {
        return;
      }

      // Debounce updates to reduce CPU usage
      const existingTimeout = this._updateTimeouts.get(event.queueName);
      if (existingTimeout) {
        clearTimeout(existingTimeout);
      }

      const timeout = setTimeout(() => {
        this.logger.debug(`Updating queue metrics: ${event.queueName} - ${JSON.stringify(event.counts)}`);
        this.bullMQMetricsFactory.updateMetrics(event.queuePrefix, event.queueName, event.counts);
        this._lastMetrics.set(event.queueName, {
          counts: event.counts,
          timestamp: now
        });
        this._updateTimeouts.delete(event.queueName);
      }, this.DEBOUNCE_MS);

      this._updateTimeouts.set(event.queueName, timeout);
    });
  }

  private areCountsSimilar(oldCounts: any, newCounts: any): boolean {
    // Consider counts similar if they're within 5% of each other
    const threshold = 0.05;
    return Object.keys(oldCounts).every(key => {
      const oldValue = oldCounts[key] || 0;
      const newValue = newCounts[key] || 0;
      const diff = Math.abs(oldValue - newValue);
      const maxValue = Math.max(oldValue, newValue);
      return maxValue === 0 || (diff / maxValue) < threshold;
    });
  }

  onModuleDestroy() {
    // Clear all timeouts
    for (const timeout of this._updateTimeouts.values()) {
      clearTimeout(timeout);
    }
    this._updateTimeouts.clear();
    this._lastMetrics.clear();
    this.bullQueuesService.removeAllListeners();
  }
}
