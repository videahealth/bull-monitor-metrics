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
    });

    this.bullQueuesService.on(EVENT_TYPES.QUEUE_UPDATED, (event: { queuePrefix: string; queueName: string; counts: { completed: number; failed: number; delayed: number; active: number; waiting: number } }) => {
      this.logger.debug(`Updating queue metrics: ${event.queueName} - ${JSON.stringify(event.counts)}`);
      this.bullMQMetricsFactory.updateMetrics(event.queuePrefix, event.queueName, event.counts);
    });
  }

  onModuleDestroy() {
    this.bullQueuesService.removeAllListeners();
  }
}
