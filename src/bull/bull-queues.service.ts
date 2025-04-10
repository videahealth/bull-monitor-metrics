import { ConfigService } from '@app/config/config.service';
import { InjectLogger, LoggerService } from '@app/logger';
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Mutex, withTimeout } from 'async-mutex';
import { Queue, QueueScheduler } from 'bullmq';
import { RedisService } from 'nestjs-redis';
import { EventEmitter } from 'events';
import {
  EVENT_TYPES,
  REDIS_CLIENTS,
  REDIS_EVENT_TYPES,
  REDIS_KEYSPACE_EVENT_TYPES,
} from './bull.enums';
import {
  BullQueuesServiceEvents,
  QueueCreatedEvent,
  QueueRemovedEvent,
} from './bull.interfaces';

const BULL_QUEUE_REGEX = /(?<queuePrefix>^[^:]+):(?<queueName>[^:]+):/;
const BULL_KEYSPACE_REGEX = /(?<queuePrefix>[^:]+):(?<queueName>[^:]+):meta$/;
const parseBullQueue = (key: string) => {
  const MATCHER = key.match(/:meta$/) ? BULL_KEYSPACE_REGEX : BULL_QUEUE_REGEX;
  const match = key.match(MATCHER);
  return {
    queuePrefix: match.groups?.queuePrefix
      ? match.groups.queuePrefix
      : 'unknown',
    queueName: match.groups?.queueName ? match.groups.queueName : 'unknown',
  };
};

const REDIS_CONFIG_NOTIFY_KEYSPACE_EVENTS = 'notify-keyspace-events';

// Expected configuration flags
const REDIS_CONFIG_NOTIFY_KEYSPACE_EVENTS_FLAGS = 'A$K';

@Injectable()
export class BullQueuesService extends EventEmitter implements OnModuleInit, OnModuleDestroy {
  private _initialized = false;
  private readonly _queues: { [queueName: string]: Queue } = {};
  private readonly _schedulers: { [queueName: string]: QueueScheduler } = {};
  private readonly _redisMutex = withTimeout(new Mutex(), 10000);
  private readonly _bullMutex = withTimeout(new Mutex(), 10000);
  private _metricsInterval: NodeJS.Timeout | null = null;
  private _lastCounts = new Map<string, any>();
  private _lastCheckTime = new Map<string, number>();
  private readonly CACHE_TTL_MS = 30000; // 30 seconds cache TTL
  private readonly MIN_CHECK_INTERVAL_MS = 30000; // Minimum 30 seconds between checks

  constructor(
    @InjectLogger(BullQueuesService)
    private readonly logger: LoggerService,
    private readonly redisService: RedisService,
    private readonly configService: ConfigService,
  ) {
    super();
  }

  private async processMessage(
    eventType: REDIS_KEYSPACE_EVENT_TYPES,
    queuePrefix: string,
    queueName: string,
  ) {
    return await this._redisMutex.runExclusive(async () => {
      this.logger.debug(
        `processMessage(${eventType}): ${queuePrefix}::${queueName}`,
      );
      switch (eventType) {
        case REDIS_KEYSPACE_EVENT_TYPES.HSET:
          await this.addQueue(queuePrefix, queueName);
          break;
        case REDIS_KEYSPACE_EVENT_TYPES.DELETE:
          await this.removeQueue(queuePrefix, queueName);
          break;
        default:
          this.logger.debug(`Ignoring event '${eventType}'`);
      }
    });
  }

  getLoadedQueues(): string[] {
    return Object.keys(this._queues);
  }

  private generateQueueKey(queuePrefix: string, queueName: string): string {
    return `${queuePrefix}:::${queueName}`;
  }

  private splitQueueKey(queueKey: string): {
    queueName: string;
    queuePrefix: string;
  } {
    return queueKey.match(/^(?<queuePrefix>.+):::(?<queueName>.+)$/).groups as {
      queueName: string;
      queuePrefix: string;
    };
  }

  private addQueue(queuePrefix: string, queueName: string) {
    return this._bullMutex.runExclusive(async () => {
      const queueKey = this.generateQueueKey(queuePrefix, queueName);
      this.logger.debug(`Attempting to add queue: ${queueKey}`);
      if (!(queueKey in this._queues)) {
        this.logger.log(`Adding queue: ${queueKey}`);
        // reusing a connection causes issues in the event
        // that a queue is removed during network connectivity
        // issues.
        this._queues[queueKey] = new Queue(queueName, {
          prefix: queuePrefix,
          connection: {
            host: this.configService.config.REDIS_HOST,
            port: this.configService.config.REDIS_PORT,
            password: this.configService.config.REDIS_PASSWORD,
          },
        });
        this._queues[queueKey].on('error', (err) => {
          Error.captureStackTrace(err);
          this.logger.error(err.stack);
          this.removeQueue(queuePrefix, queueName);
        });
        this._queues[queueKey].on('ioredis:close', () => {
          this.removeQueue(queuePrefix, queueName);
        });
        /**
         * From: https://docs.bullmq.io/guide/connections
         *
         * Every class will consume at least one Redis connection, but it
         * is also possible to reuse connections in some situations. For example,
         * the Queue and Worker classes can accept an existing ioredis instance, and
         * by that reusing that connection, however QueueScheduler and QueueEvents
         * cannot do that because they require blocking connections to Redis, which
         * makes it impossible to reuse them.
         */
        this._schedulers[queueKey] = new QueueScheduler(queueName, {
          prefix: queuePrefix,
          connection: {
            host: this.configService.config.REDIS_HOST,
            port: this.configService.config.REDIS_PORT,
            password: this.configService.config.REDIS_PASSWORD,
          },
        });
        this._schedulers[queueKey].on('error', (err) => {
          Error.captureStackTrace(err);
          this.logger.error(err.stack);
          this.removeQueue(queuePrefix, queueName);
        });
        this.emit(
          EVENT_TYPES.QUEUE_CREATED,
          new QueueCreatedEvent(queuePrefix, this._queues[queueKey]),
        );
      }
    });
  }

  private async removeQueue(queuePrefix: string, queueName: string) {
    return this._bullMutex.runExclusive(async () => {
      const queueKey = this.generateQueueKey(queuePrefix, queueName);
      this.logger.debug(`Attempting to remove queue: ${queueKey}`);
      if (queueKey in this._queues) {
        this.logger.log(`Removing queue: ${queueKey}`);

        try {
          await this._queues[queueKey].close();
          await this._schedulers[queueKey].close();
        } catch (err) {
          // in the event of an error just ignore it and move on
          this.logger.error(err);
        }

        delete this._queues[queueKey];
        delete this._schedulers[queueKey];

        this.emit(
          EVENT_TYPES.QUEUE_REMOVED,
          new QueueRemovedEvent(queuePrefix, queueName),
        );

        this.logger.debug(`Queue removed: ${queueKey}`);
      }
    });
  }

  private async registerRedisEventListeners() {
    if (this._initialized) return;

    const subscriber = this.redisService.getClient(REDIS_CLIENTS.SUBSCRIBE);
    const queuePrefixes =
      this.configService.config.BULL_WATCH_QUEUE_PREFIXES.split(',').map(
        (item) => item.trim(),
      );

    // loop through each queue prefix and add anything
    // we find
    for (const queuePrefix of queuePrefixes) {
      // subscribe to keyspace events
      await subscriber.psubscribe(
        `__keyspace@0__:${queuePrefix}:*:meta`,
        (err, count) => {
          if (err) {
            this.logger.error(err.stack);
          }
          this.logger.log(
            `Subscribed to ${count} keyspace event(s) for '${queuePrefix}'`,
          );
        },
      );
    }

    // logic to handle incoming events
    this.logger.log(`Registering ${REDIS_EVENT_TYPES.PMESSAGE} listener`);
    subscriber.on(
      REDIS_EVENT_TYPES.PMESSAGE,
      async (pattern: string, channel: string, message: string) => {
        this.logger.debug(
          `${REDIS_EVENT_TYPES.PMESSAGE}(pattern: ${pattern}, channel: ${channel}): ${message}`,
        );
        const queueMatch = parseBullQueue(channel);
        await this.processMessage(
          message as REDIS_KEYSPACE_EVENT_TYPES,
          queueMatch.queuePrefix,
          queueMatch.queueName,
        );
      },
    );

    this._initialized = true;
  }

  private async deregisterRedisEventListeners() {
    if (!this._initialized) return;

    const subscriber = this.redisService.getClient(REDIS_CLIENTS.SUBSCRIBE);

    this.logger.debug(`Deregistering ${REDIS_EVENT_TYPES.PMESSAGE} listener`);
    subscriber.removeAllListeners(REDIS_EVENT_TYPES.PMESSAGE);

    // we want to make sure we are unsubscribed but if an error gets thrown
    // (e.g. closed connection) that also accomplishes the same goal
    try {
      await subscriber.punsubscribe();
    } catch (err) {
      this.logger.error(err);
    }

    this._initialized = false;
  }

  private async findAndPopulateQueues(match: string): Promise<string[]> {
    const client = await this.redisService.getClient(REDIS_CLIENTS.PUBLISH);
    const loadedQueues = new Set([]);
    return new Promise((resolve, reject) => {
      client
        .scanStream({ type: 'hash', match, count: 100 })
        .on('data', (keys: string[]) => {
          for (const key of keys) {
            const queueMatch = parseBullQueue(key);
            loadedQueues.add(
              this.generateQueueKey(
                queueMatch.queuePrefix,
                queueMatch.queueName,
              ),
            );
            this.addQueue(queueMatch.queuePrefix, queueMatch.queueName);
          }
        })
        .on('end', () => {
          resolve(Array.from(loadedQueues));
        })
        .on('error', (err) => {
          this.logger.error(`${err.name}: ${err.message}`);
          this.logger.error(`${err.stack}`);
          reject(err);
        });
    });
  }

  /**
   * Ensure that are are at least monitoring keyspace events
   *
   * @url https://redis.io/topics/notifications
   *
   * By default keyspace event notifications are disabled because while
   * not very sensible the feature uses some CPU power. Notifications are
   * enabled using the notify-keyspace-events of redis.conf or via the CONFIG SET.
   *
   * Setting the parameter to the empty string disables notifications. In order to
   * enable the feature a non-empty string is used, composed of multiple characters,
   * where every character has a special meaning according to the following table:
   *
   * K     Keyspace events, published with __keyspace@<db>__ prefix.
   * E     Keyevent events, published with __keyevent@<db>__ prefix.
   * g     Generic commands (non-type specific) like DEL, EXPIRE, RENAME, ...
   * $     String commands
   * l     List commands
   * s     Set commands
   * h     Hash commands
   * z     Sorted set commands
   * t     Stream commands
   * d     Module key type events
   * x     Expired events (events generated every time a key expires)
   * e     Evicted events (events generated when a key is evicted for maxmemory)
   * m     Key miss events (events generated when a key that doesn't exist is accessed)
   * A     Alias for "g$lshztxed", so that the "AKE" string means all the events except "m".
   */
  private async configureKeyspaceEventNotifications() {
    if (!this.configService.config.REDIS_CONFIGURE_KEYSPACE_NOTIFICATIONS) {
      this.logger.log('Skipping redis keyspace notification configuration.');
      return;
    }

    const client = await this.redisService.getClient(REDIS_CLIENTS.PUBLISH);
    const keyspaceConfig = (
      (await client.config('GET', REDIS_CONFIG_NOTIFY_KEYSPACE_EVENTS))[1] || ''
    )
      .split('')
      .sort()
      .join('');
    const expectedConfig = [
      ...new Set(keyspaceConfig + REDIS_CONFIG_NOTIFY_KEYSPACE_EVENTS_FLAGS),
    ]
      .sort()
      .join('');
    this.logger.log(
      `Redis config for ${REDIS_CONFIG_NOTIFY_KEYSPACE_EVENTS}: '${keyspaceConfig}'`,
    );

    if (keyspaceConfig !== expectedConfig) {
      this.logger.log(
        `Updating ${REDIS_CONFIG_NOTIFY_KEYSPACE_EVENTS} to '${expectedConfig}'`,
      );
      await client.config(
        'SET',
        REDIS_CONFIG_NOTIFY_KEYSPACE_EVENTS,
        expectedConfig,
      );
    }
  }

  private async initializeSubscriber() {
    return this._redisMutex.runExclusive(async () => {
      return await this.registerRedisEventListeners();
    });
  }

  private async uninitializeSubscriber() {
    return this._redisMutex.runExclusive(async () => {
      // this is to ensure that on connection close we do not listen
      // to redis events until connection is re-established and list
      // of queues is full recreated.
      return await this.deregisterRedisEventListeners();
    });
  }

  private async initializePublisher() {
    return this._redisMutex.runExclusive(async () => {
      this.logger.log(
        'Redis connection READY! Configuring watchers for bull queues.',
      );

      await this.configureKeyspaceEventNotifications();

      const previouslyLoadedQueues = this.getLoadedQueues();
      let newlyLoadedQueues: Array<any> = [];
      this.logger.debug(
        `Queues currently monitored: ${
          previouslyLoadedQueues.length > 0
            ? previouslyLoadedQueues.join(', ')
            : '<none>'
        }`,
      );
      const queuePrefixes =
        this.configService.config.BULL_WATCH_QUEUE_PREFIXES.split(',').map(
          (item) => item.trim(),
        );
      // loop through each queue prefix and add anything
      // we find
      for (const queuePrefix of queuePrefixes) {
        this.logger.log(`Loading queues from queuePrefix: '${queuePrefix}'`);

        newlyLoadedQueues = (
          await Promise.all([
            // this.findAndPopulateQueues(`${queuePrefix}:*:stalled-check`),
            //this.findAndPopulateQueues(`${queuePrefix}:*:id`),
            this.findAndPopulateQueues(`${queuePrefix}:*:meta`),
          ])
        ).flat();
      }

      /**
       * In the event that we are reloading this configuration (perhaps after a loss of
       * connection) we'll want to ensure that we prune any queues that may have been removed
       * during the outage.
       */
      const queuesToPrune = previouslyLoadedQueues.filter(
        (x) => !newlyLoadedQueues.includes(x),
      );
      this.logger.log(
        `Pruning unused queues: ${
          queuesToPrune.length > 0 ? queuesToPrune.join(', ') : '<none>'
        }`,
      );
      for (const queueToPrune of queuesToPrune) {
        const queueDetails = this.splitQueueKey(queueToPrune);
        await this.removeQueue(
          queueDetails.queuePrefix,
          queueDetails.queueName,
        );
      }

      this.emit(EVENT_TYPES.QUEUE_SERVICE_READY);
    });
  }

  async onModuleInit() {
    if (this._initialized) {
      return;
    }

    this.logger.log('Initializing BullQueuesService');
    const client = this.redisService.getClient(REDIS_CLIENTS.SUBSCRIBE);
    const keys = await client.keys('*:*:meta');
    this.logger.log(`Found ${keys.length} queue(s) in Redis`);
    
    for (const key of keys) {
      const { queuePrefix, queueName } = parseBullQueue(key);
      this.logger.log(`Found queue: ${queuePrefix}:${queueName}`);
      if (this.configService.config.BULL_WATCH_QUEUE_PREFIXES.split(',').some(prefix => queuePrefix.startsWith(prefix))) {
        this.logger.log(`Adding queue for monitoring: ${queuePrefix}:${queueName}`);
        await this.addQueue(queuePrefix, queueName);
      }
    }

    // Configure metrics collection interval with batching and caching
    const BATCH_SIZE = 5; // Process 5 queues at a time
    const BATCH_DELAY = 5000; // 5 seconds between batches
    let currentBatch = 0;

    const interval = setInterval(async () => {
      const queueEntries = Object.entries(this._queues);
      const totalBatches = Math.ceil(queueEntries.length / BATCH_SIZE);
      
      if (currentBatch >= totalBatches) {
        currentBatch = 0;
      }

      const startIdx = currentBatch * BATCH_SIZE;
      const endIdx = Math.min(startIdx + BATCH_SIZE, queueEntries.length);
      const batch = queueEntries.slice(startIdx, endIdx);

      for (const [queueKey, queue] of batch) {
        try {
          const { queuePrefix } = this.splitQueueKey(queueKey);
          const now = Date.now();
          const lastCheck = this._lastCheckTime.get(queueKey) || 0;
          
          // Skip if we've checked recently
          if (now - lastCheck < this.MIN_CHECK_INTERVAL_MS) {
            continue;
          }

          const counts = await queue.getJobCounts('completed', 'failed', 'delayed', 'active', 'waiting');
          this._lastCheckTime.set(queueKey, now);
          
          // Only emit if counts have changed and cache has expired
          const lastCounts = this._lastCounts.get(queueKey);
          if (!lastCounts || 
              JSON.stringify(lastCounts) !== JSON.stringify(counts) || 
              now - lastCheck >= this.CACHE_TTL_MS) {
            this._lastCounts.set(queueKey, counts);
            
            this.emit(
              EVENT_TYPES.QUEUE_UPDATED,
              {
                queuePrefix,
                queueName: queue.name,
                queue,
                counts: {
                  completed: counts.completed || 0,
                  failed: counts.failed || 0,
                  delayed: counts.delayed || 0,
                  active: counts.active || 0,
                  waiting: counts.waiting || 0
                }
              }
            );
          }
        } catch (err) {
          this.logger.error(`Error getting job counts for ${queueKey}:`, err);
        }
      }

      currentBatch++;
    }, BATCH_DELAY);

    // Store interval for cleanup
    this._metricsInterval = interval;

    this._initialized = true;
    this.emit(EVENT_TYPES.QUEUE_SERVICE_READY);
    this.logger.log('BullQueuesService initialized');
  }

  async onModuleDestroy() {
    this.logger.log('Destroying module');
    this.removeAllListeners();

    if (this._metricsInterval) {
      clearInterval(this._metricsInterval);
    }

    for (const queue of Object.values(this._queues)) {
      await queue.close();
    }
    for (const scheduler of Object.values(this._schedulers)) {
      await scheduler.close();
    }

    const client = await this.redisService.getClient(REDIS_CLIENTS.SUBSCRIBE);
    await client.quit();

    this.emit(EVENT_TYPES.QUEUE_SERVICE_CLOSED);
  }
}
