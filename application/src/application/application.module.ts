import { Module } from '@nestjs/common';
import { ConsumerController } from '@application/controllers/consumer.controller';

@Module({
  controllers: [ConsumerController],
})
export class ApplicationModule {}
