import { Events } from '../plugin';
import * as amqplib from 'amqplib';
import { DataType } from '../sec.config';
import { EventEmitter } from 'events';
import { randomUUID } from 'crypto';
import { LIB } from './lib';

export class ear extends EventEmitter {
  private uSelf!: Events;
  private channel!: amqplib.Channel;
  //private myChannel!: amqplib.Channel;
  private readonly channelKey = "2ar";
  private readonly myChannelKey = "2kr";
  private readonly exchange: any = {
    type: 'direct',
    name: 'better-service2-ear'
  };
  private readonly exchangeOpts: amqplib.Options.AssertExchange = {
    durable: false,
    //exclusive: true,
    autoDelete: true,
  };
  private readonly queueOpts: amqplib.Options.AssertQueue = {
    durable: false,
    autoDelete: true,
    messageTtl: (60 * 60) * 1000, // 60 minutes
  };

  private async setupHandler(myEARQueueKey: string, exName: string, exType: string, exOpts: amqplib.Options.AssertExchange, quOpts: amqplib.Options.AssertQueue, listener?: { (msg: amqplib.ConsumeMessage | null): void; }) {
    const self = this;
    this.uSelf.log.info(`Open channel (${ myEARQueueKey })`);
    const channel = await this.uSelf.rabbitQConnection.createChannel();
    channel.on("close", () => {
      self.uSelf.log.error(`AMQP channel (${ myEARQueueKey }) close`);
      self.uSelf.log.fatal(`AMQP Error: channel (${ myEARQueueKey }) close`);
    });
    channel.on("error", (err: any) => {
      self.uSelf.log.error(`AMQP channel (${ myEARQueueKey }) error`, err);
      self.uSelf.log.fatal(`AMQP Error: channel (${ myEARQueueKey }) error`, err);
    });
    channel.assertExchange(exName, exType, exOpts);
    channel.prefetch(2);
    this.uSelf.log.info(` - READY: [${ myEARQueueKey }]`);

    if (listener !== undefined) {
      await channel.assertQueue(myEARQueueKey, quOpts);
      this.uSelf.log.info(` - LISTEN: [${ myEARQueueKey }] - LISTEN`);
      await channel.consume(myEARQueueKey, listener, { noAck: false });
      this.uSelf.log.info(` - LISTEN: [${ myEARQueueKey }] - LISTENING`);
    }
    return channel;
  }

  async init(uSelf: Events) {
    this.uSelf = uSelf;
    const self = this;
    const myEARQueueKey = LIB.getSpecialQueueKey(this.myChannelKey, this.uSelf.myId);
    this.uSelf.log.info(`Ready my events name: ${ myEARQueueKey }`);

    //this.channel = await this.setupHandler(self.channelKey, this.exchange.name, this.exchange.type, this.exchangeOpts, this.queueOpts);
    this.channel = await this.setupHandler(myEARQueueKey, this.exchange.name, this.exchange.type, this.exchangeOpts, this.queueOpts, (msg: amqplib.ConsumeMessage | null): any => {
      if (msg === null) return self.uSelf.log.debug(`[RECEVIED ${ myEARQueueKey }]... as null`);
      try {
        let body = msg.content.toString();
        self.channel.ack(msg);
        self.uSelf.log.debug(`[RECEVIED ${ myEARQueueKey }]`);
        self.emit(msg.properties.correlationId, JSON.parse(body));
      } catch (exc) {
        self.uSelf.log.fatal(exc);
      }
    });
    this.uSelf.log.info(`Ready my events name: ${ myEARQueueKey } OKAY`);
  }

  async onReturnableEvent<ArgsDataType = any, ResolveDataType = any, RejectDataType = any>(callerPluginName: string, pluginName: string, event: string, listener: { (resolve: { (...args: ResolveDataType[]): void; }, reject: { (...args: RejectDataType[]): void; }, data: ArgsDataType): void; }): Promise<void> {
    const self = this;
    const queueKey = LIB.getQueueKey(this.channelKey, callerPluginName, pluginName, event);
    self.uSelf.log.info(callerPluginName, ` EAR: ${ callerPluginName } listen ${ queueKey }`);

    await self.channel.assertQueue(queueKey, self.queueOpts);
    await self.channel.consume(queueKey, (msg: amqplib.ConsumeMessage | null): any => {
      if (msg === null) return self.uSelf.log.error('Message received on my EAR queue was null...');
      const returnQueue = LIB.getSpecialQueueKey(this.myChannelKey, msg.properties.appId);
      self.uSelf.log.info(callerPluginName, `EAR: ${ callerPluginName } Received: ${ queueKey } from ${ returnQueue }`);
      let body = msg.content.toString();
      const bodyObj = JSON.parse(body) as ArgsDataType;
      listener((x: any) => {
        self.channel.ack(msg);
        self.uSelf.log.info(callerPluginName, `EAR: ${ callerPluginName } OKAY: ${ queueKey } -> ${ returnQueue }`);
        if (!self.channel.sendToQueue(returnQueue, Buffer.from(JSON.stringify(x)), {
          expiration: 5000,
          correlationId: `${msg.properties.correlationId}-resolve`,
          contentType: DataType[typeof x],
          appId: self.uSelf.myId,
          timestamp: new Date().getTime()
        }))
          throw `Cannot send msg to queue [${ returnQueue }]`;
      }, (x: any) => {
        self.channel.ack(msg);
        self.uSelf.log.info(callerPluginName, `EAR: ${ callerPluginName } ERROR: ${ queueKey } -> ${ returnQueue }`);
        if (!self.channel.sendToQueue(returnQueue, Buffer.from(JSON.stringify(x)), {
          expiration: 5000,
          correlationId: `${msg.properties.correlationId}-reject`,
          contentType: DataType[typeof x],
          appId: self.uSelf.myId,
          timestamp: new Date().getTime()
        }))
          throw `Cannot send msg to queue [${ returnQueue }]`;
      }, bodyObj);
    }, { noAck: false });
    self.uSelf.log.info(callerPluginName, ` EAR: ${ callerPluginName } listening ${ queueKey }`);
  };
  emitEventAndReturn<ArgsDataType = any, ReturnDataType = any>(callerPluginName: string, pluginName: string, event: string, data: ArgsDataType, timeoutSeconds: number = 5): Promise<ReturnDataType> {
    const self = this;
    const resultKey = `${ randomUUID() }-${ new Date().getTime() }${ Math.random() }`;
    const queueKey = LIB.getQueueKey(this.channelKey, callerPluginName, pluginName, event);
    this.uSelf.log.info(`EAR: ${ callerPluginName } emitting ${ queueKey } (${ resultKey })`);
    return new Promise(async (resolve, reject) => {
      let timeoutHandler = setTimeout(() => {
        self.removeAllListeners(`${ resultKey }-resolve`);
        self.removeAllListeners(`${ resultKey }-reject`);
        reject('Timeout');
      }, timeoutSeconds * 1000);
      self.once(`${ resultKey }-resolve`, (args: ReturnDataType) => {
        clearTimeout(timeoutHandler);
        resolve(args);
      });
      self.once(`${ resultKey }-reject`, (args: ReturnDataType) => {
        clearTimeout(timeoutHandler);
        reject(args);
      });
      await self.channel.assertQueue(queueKey, self.queueOpts);
      if (!self.channel.sendToQueue(queueKey, Buffer.from(JSON.stringify(data)), {
        expiration: (timeoutSeconds * 1000) + 5000,
        correlationId: resultKey,
        contentType: DataType[typeof data],
        appId: self.uSelf.myId,
        timestamp: new Date().getTime()
      }))
        throw `Cannot send msg to queue [${ queueKey }]`;
      self.uSelf.log.info(`EAR: ${ callerPluginName } emitted ${ queueKey } (${ resultKey })`);
    });
  }
}