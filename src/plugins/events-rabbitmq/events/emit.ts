import { Events } from '../plugin';
import * as amqplib from 'amqplib';
import { DataType, IPluginConfig } from '../sec.config';
import { EventEmitter } from 'events';
import { LIB } from './lib';

export class emit {
  private uSelf!: Events;
  private channel!: amqplib.Channel;
  private readonly channelKey = "2eq";
  private readonly exchange: any = {
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
    this.channel = await this.uSelf.rabbitQConnection.createChannel();
    const self = this;
    this.channel.on("close", () => {
      self.uSelf.log.error('AMQP rabbitQConnectionEmitChannel close');
      self.uSelf.log.fatal('AMQP Error: rabbitQConnectionEmitChannel close');
    });
    this.channel.on("error", (err: any) => {
      self.uSelf.log.error('AMQP rabbitQConnectionEmitChannel error', err);
      self.uSelf.log.fatal('AMQP Error: rabbitQConnectionEmitChannel error');
    });
    await this.channel.assertExchange(this.exchange.name, this.exchange.type, this.exchangeOpts);
    this.uSelf.log.info(`Open emit channel (${ this.exchange.name }) - PREFETCH x${ (await this.uSelf.getPluginConfig<IPluginConfig>()).prefetch }`);
    this.channel.prefetch((await this.uSelf.getPluginConfig<IPluginConfig>()).prefetch);
    this.uSelf.log.info(`Open emit channel (${ this.exchange.name }) - COMPLETED`);
  }

  async onEvent<T = any>(callerPluginName: string, pluginName: string | null, event: string, listener: (data: T) => void): Promise<void> {
    const self = this;
    const thisQueueKey = LIB.getQueueKey(this.channelKey, callerPluginName, pluginName, event);
    self.uSelf.log.info(callerPluginName, ` - LISTEN: [${ thisQueueKey }]`);

    await self.channel.assertQueue(thisQueueKey, self.queueOpts);
    self.uSelf.log.info(callerPluginName, ` - LISTEN: [${ thisQueueKey }] - LISTENING`);
    await self.channel.consume(thisQueueKey, (msg: any) => {
      let body = msg.content.toString();
      const bodyObj = JSON.parse(body) as any;
      listener(bodyObj as T);
      self.channel.ack(msg);
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
    await this.channel.assertQueue(thisQueueKey, this.queueOpts);
    if (!this.channel.sendToQueue(thisQueueKey, Buffer.from(JSON.stringify(data)), {
      expiration: this.queueOpts.messageTtl,
      contentType: dataType,
      appId: this.uSelf.myId,
      timestamp: new Date().getTime()
    }))
      throw `Cannot send msg to queue [${ thisQueueKey }]`;
    this.uSelf.log.debug(callerPluginName, ` - EMIT: [${ thisQueueKey }] - EMITTED`);
  }
}