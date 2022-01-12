import { Events } from '../plugin';
import * as amqplib from 'amqplib';
import { DataType } from '../sec.config';
import { EventEmitter } from 'events';
import { LIB } from './lib';

export class emit {
  private uSelf!: Events;
  private publishChannel!: amqplib.Channel;
  private receiveChannel!: amqplib.Channel;
  private readonly channelKey = "2eq";
  private readonly exchange = {
    type: 'fanout',
    name: 'better-service2-emit'
  };
  private readonly exchangeOpts: amqplib.Options.AssertExchange = {
    durable: true,
    //exclusive: true,
    autoDelete: true,
  };
  private readonly queueOpts: amqplib.Options.AssertQueue = {
    durable: true,
    autoDelete: true,
    messageTtl: (60 * 60 * 6) * 1000 // 6h
    //expires: (60*60*360)*1000 // 360 minutes
  };
  async init(uSelf: Events) {
    this.uSelf = uSelf;
    this.uSelf.log.info(`Open emit channel (${ this.exchange.name })`);
    this.publishChannel = await LIB.setupChannel(uSelf, uSelf.publishConnection, this.channelKey, this.exchange.name, this.exchange.type, this.exchangeOpts);
    this.receiveChannel = await LIB.setupChannel(uSelf, uSelf.receiveConnection, this.channelKey, this.exchange.name, this.exchange.type, this.exchangeOpts, 5);
  }

  async onEvent<T = any>(callerPluginName: string, pluginName: string | null, event: string, listener: { (data: T): Promise<void>; }): Promise<void> {
    const self = this;
    const thisQueueKey = LIB.getQueueKey(this.channelKey, callerPluginName, pluginName, event);
    self.uSelf.log.info(callerPluginName, ` - LISTEN: [${ thisQueueKey }]`);

    await self.receiveChannel.assertQueue(thisQueueKey, self.queueOpts);
    self.uSelf.log.info(callerPluginName, ` - LISTEN: [${ thisQueueKey }] - LISTENING`);
    await self.receiveChannel.consume(thisQueueKey, async (msg: any) => {
      let body = msg.content.toString();
      const bodyObj = JSON.parse(body) as any;
      await listener(bodyObj as T);
      self.receiveChannel.ack(msg);
    }, { noAck: false });
  }
  async emitEvent<T = any>(callerPluginName: string, pluginName: string | null, event: string, data?: T): Promise<void> {
    let dataType: DataType = DataType[typeof data];
    if (data instanceof EventEmitter) {
      this.uSelf.log.fatal('We cannot emit streams. Only emitAndReturn');
      throw new Error('Not supported transmitting streams this way');
    }

    const thisQueueKey = LIB.getQueueKey(this.channelKey, callerPluginName, pluginName, event);
    this.uSelf.log.debug(callerPluginName, ` - EMIT: [${ thisQueueKey }]`);
    await this.publishChannel.assertQueue(thisQueueKey, this.queueOpts);
    if (!this.publishChannel.sendToQueue(thisQueueKey, Buffer.from(JSON.stringify(data)), {
      expiration: this.queueOpts.messageTtl,
      contentType: dataType,
      appId: this.uSelf.myId,
      timestamp: new Date().getTime()
    }))
      throw `Cannot send msg to queue [${ thisQueueKey }]`;
    this.uSelf.log.debug(callerPluginName, ` - EMIT: [${ thisQueueKey }] - EMITTED`);
  }
}