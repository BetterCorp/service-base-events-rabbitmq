import { Events } from '../plugin';
import * as amqplib from 'amqplib';
import { DataType } from '../sec.config';
import { EventEmitter } from 'events';
import { randomUUID } from 'crypto';
import { LIB } from './lib';

export class emitAndReturn extends EventEmitter {
  private uSelf!: Events;
  private publishChannel!: amqplib.Channel;
  private receiveChannel!: amqplib.Channel;
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
  private readonly myQueueOpts: amqplib.Options.AssertQueue = {
    exclusive: true,
    durable: false,
    autoDelete: true,
    messageTtl: (60 * 60) * 1000, // 60 minutes
  };

  async init(uSelf: Events) {
    this.uSelf = uSelf;
    const self = this;
    const myEARQueueKey = LIB.getSpecialQueueKey(this.myChannelKey, this.uSelf.myId);
    this.uSelf.log.info(`Ready my events name: ${ myEARQueueKey }`);

    this.publishChannel = await LIB.setupChannel(uSelf, uSelf.publishConnection, this.myChannelKey, this.exchange.name, this.exchange.type, this.exchangeOpts);
    this.receiveChannel = await LIB.setupChannel(uSelf, uSelf.receiveConnection, this.myChannelKey, this.exchange.name, this.exchange.type, this.exchangeOpts, 2);
    await this.receiveChannel.assertQueue(myEARQueueKey, this.myQueueOpts);
    this.uSelf.log.info(` - LISTEN: [${ myEARQueueKey }] - LISTEN`);
    await this.receiveChannel.consume(myEARQueueKey, (msg: amqplib.ConsumeMessage | null): any => {
      if (msg === null) return self.uSelf.log.debug(`[RECEVIED ${ myEARQueueKey }]... as null`);
      try {
        let body = msg.content.toString();
        self.uSelf.log.debug(`[RECEVIED ${ myEARQueueKey }]`);
        self.emit(msg.properties.correlationId, JSON.parse(body));
        self.receiveChannel.ack(msg);
      } catch (exc) {
        self.uSelf.log.fatal(exc);
      }
    }, { noAck: false });
    this.uSelf.log.info(` - LISTEN: [${ myEARQueueKey }] - LISTENING`);
    this.uSelf.log.info(`Ready my events name: ${ myEARQueueKey } OKAY`);
  }

  async onReturnableEvent<ArgsDataType = any, ReturnDataType = any>(callerPluginName: string, pluginName: string, event: string, listener: { (data: ArgsDataType): Promise<ReturnDataType>; }): Promise<void> {
    const self = this;
    const queueKey = LIB.getQueueKey(this.channelKey, callerPluginName, pluginName, event);
    self.uSelf.log.info(callerPluginName, ` EAR: ${ callerPluginName } listen ${ queueKey }`);

    await self.receiveChannel.assertQueue(queueKey, self.queueOpts);
    await self.receiveChannel.consume(queueKey, async (msg: amqplib.ConsumeMessage | null): Promise<any> => {
      if (msg === null) return self.uSelf.log.error('Message received on my EAR queue was null...');
      const returnQueue = LIB.getSpecialQueueKey(this.myChannelKey, msg.properties.appId);
      self.uSelf.log.info(callerPluginName, `EAR: ${ callerPluginName } Received: ${ queueKey } from ${ returnQueue }`);
      let body = msg.content.toString();
      const bodyObj = JSON.parse(body) as ArgsDataType;
      try {
        const response = await listener(bodyObj);
        self.receiveChannel.ack(msg);
        self.uSelf.log.info(callerPluginName, `EAR: ${ callerPluginName } OKAY: ${ queueKey } -> ${ returnQueue }`);
        if (!self.publishChannel.sendToQueue(returnQueue, Buffer.from(JSON.stringify(response)), {
          expiration: 5000,
          correlationId: `${ msg.properties.correlationId }-resolve`,
          contentType: DataType[typeof response],
          appId: self.uSelf.myId,
          timestamp: new Date().getTime()
        }))
          throw `Cannot send msg to queue [${ returnQueue }]`;
      } catch (exc) {
        self.receiveChannel.ack(msg);
        self.uSelf.log.info(callerPluginName, `EAR: ${ callerPluginName } ERROR: ${ queueKey } -> ${ returnQueue }`);
        if (!self.publishChannel.sendToQueue(returnQueue, Buffer.from(JSON.stringify(exc)), {
          expiration: 5000,
          correlationId: `${ msg.properties.correlationId }-reject`,
          contentType: DataType[typeof exc],
          appId: self.uSelf.myId,
          timestamp: new Date().getTime()
        }))
          throw `Cannot send msg to queue [${ returnQueue }]`;
      }
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
      await self.publishChannel.assertQueue(queueKey, self.queueOpts);
      if (!self.publishChannel.sendToQueue(queueKey, Buffer.from(JSON.stringify(data)), {
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