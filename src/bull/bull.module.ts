import { ConfigModule } from '@app/config/config.module';
import { ConfigService } from '@app/config/config.service';
import { Module } from '@nestjs/common';
import { RedisModule } from 'nestjs-redis';
import { BullMetricsService } from './bull-metrics.service';
import { BullQueuesService } from './bull-queues.service';
import { REDIS_CLIENTS } from './bull.enums';
import { BullMQMetricsFactory } from './bullmq-metrics.factory';

@Module({
  imports: [
    ConfigModule,
    RedisModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        return [{
          name: REDIS_CLIENTS.SUBSCRIBE,
          host: configService.config.REDIS_HOST,
          password: configService.config.REDIS_PASSWORD,
          port: configService.config.REDIS_PORT,
          db: configService.config.REDIS_DB,
          enableReadyCheck: true,
          reconnectOnError: () => true,
        }];
      },
    }),
  ],
  providers: [
    BullQueuesService,
    BullMetricsService,
    BullMQMetricsFactory,
  ],
})
export class BullModule {}
