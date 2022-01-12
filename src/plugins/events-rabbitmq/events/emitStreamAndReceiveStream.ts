//import { EventEmitter } from 'events';
import { Readable } from 'stream';
//import { pipeline, Writable } from 'stream';
import { randomUUID } from 'crypto';
import { Events } from '../plugin';
import * as amqplib from 'amqplib';
import { LIB } from './lib';
import { decode, encode } from './encode-decode';

export class emitStreamAndReceiveStream { //} extends EventEmitter {
  // If we try receive or send a stream and the other party is not ready for some reason, we will automatically timeout in 5s.
  private readonly staticCommsTimeout = 1000;
  private uSelf!: Events;
  private publishChannel!: amqplib.Channel;
  private receiveChannel!: amqplib.Channel;
  private streamChannel!: amqplib.Channel;
  private readonly eventsChannelKey = "2se";
  private readonly streamChannelKey = "2ss";
  private readonly exchange: any = {
    type: 'direct',
    name: 'better-service2-ers'
  };
  private readonly streamExchange: any = {
    type: 'direct',
    name: 'better-service2-sss'
  };
  private readonly exchangeOpts: amqplib.Options.AssertExchange = {
    durable: false,
    //exclusive: true,
    autoDelete: true,
  };
  private readonly queueOpts: amqplib.Options.AssertQueue = {
    exclusive: true,
    durable: false,
    autoDelete: true,
    messageTtl: 60000, // 60s
    expires: 60000, // 60s
  };
  private readonly streamQueueOpts: amqplib.Options.AssertQueue = {
    //arguments: {
    //  'x-queue-type': 'stream'
    //},
    exclusive: true,
    durable: false,
    autoDelete: true,
    messageTtl: 60000, // 60s
    expires: 60000, // 60s
  };

  async init(uSelf: Events) {
    this.uSelf = uSelf;

    this.publishChannel = await LIB.setupChannel(this.uSelf, this.uSelf.publishConnection, this.eventsChannelKey, this.exchange.name, this.exchange.type, this.exchangeOpts);
    this.receiveChannel = await LIB.setupChannel(this.uSelf, this.uSelf.receiveConnection, this.eventsChannelKey, this.exchange.name, this.exchange.type, this.exchangeOpts, 2);
    this.streamChannel = await LIB.setupChannel(this.uSelf, this.uSelf.receiveConnection, this.streamChannelKey, this.streamExchange.name, this.streamExchange.type, this.exchangeOpts, 1);

    //await this.setupEvents();
  }

  /*private async setupEvents() {
    const self = this;
    const myEventsQueueKey = LIB.getSpecialQueueKey(this.eventsChannelKey, this.uSelf.myId);
    this.uSelf.log.info(`Ready my events name: ${ myEventsQueueKey }`);

    await this.receiveChannel.assertQueue(myEventsQueueKey, this.queueOpts);
    this.uSelf.log.info(` - LISTEN: [${ myEventsQueueKey }] - LISTEN`);
    await this.receiveChannel.consume(myEventsQueueKey, (msg: amqplib.ConsumeMessage | null): any => {
      if (msg === null) return self.uSelf.log.debug(`[RECEVIED ${ myEventsQueueKey }]... as null`);
      try {
        console.log('REC:', msg.properties.correlationId);
        let body = msg.content.toString();
        self.uSelf.log.debug(`[RECEVIED ${ myEventsQueueKey }]`);
        self.emit(msg.properties.correlationId, JSON.parse(body));
        self.receiveChannel.ack(msg);
      } catch (exc) {
        self.uSelf.log.fatal(exc);
      }
    }, { noAck: false });
    this.uSelf.log.info(` - LISTEN: [${ myEventsQueueKey }] - LISTENING`);
    this.uSelf.log.info(`Ready my events name: ${ myEventsQueueKey } OKAY`);
  }*/

  receiveStream(callerPluginName: string, listener: { (error: Error | null, stream: Readable): void; }, timeoutSeconds = 60): Promise<string> {
    const streamId = `${ timeoutSeconds }-${ randomUUID() }`;
    const streamEventsRefId = LIB.getLocalKey(this.eventsChannelKey, streamId);
    const streamRefId = LIB.getLocalKey(this.streamChannelKey, streamId);
    this.uSelf.log.info(`SR: ${ callerPluginName } listening to ${ streamId }`);
    const self = this;
    return new Promise(async (resolve) => {
      console.log('ASSERT');
      await self.receiveChannel.assertQueue(streamEventsRefId, self.queueOpts);
      await self.streamChannel.assertQueue(streamRefId, self.streamQueueOpts);
      let stream: Readable | null = null;
      let replyKey: string | null = null;
      let replyKeyId: string | null = null;
      let receiptTimeoutHandler: NodeJS.Timeout;
      const cleanup = async () => {
        clearTimeout(receiptTimeoutHandler);
        self.removeAllListeners('r' + streamId);
        await self.receiveChannel.deleteQueue(streamEventsRefId);
        await self.streamChannel.deleteQueue(streamRefId);
        if (stream !== null && !stream.destroyed) stream.destroy();
      };
      receiptTimeoutHandler = setTimeout(async () => {
        const err = new Error('Receive Receipt Timeout');
        await cleanup();
        if (replyKey !== null) {
          if (!self.publishChannel.sendToQueue(replyKey!, Buffer.from(JSON.stringify(err)), {
            expiration: self.queueOpts.messageTtl,
            correlationId: 's' + LIB.getLocalKey(replyKeyId!, 'error'),
            appId: self.uSelf.myId,
            timestamp: new Date().getTime()
          }))
            throw `Cannot send msg to queue [${ streamRefId }]`;
        }
        listener(err, null!);
      }, self.staticCommsTimeout);
      console.log('ASSERT:OK');
      /*if (!self.publishChannel.sendToQueue(replyKey!, Buffer.from('""'), {
        expiration: self.queueOpts.messageTtl,
        correlationId: LIB.getLocalKey(streamId!, 'ready'),
        appId: self.uSelf.myId,
        timestamp: new Date().getTime()
      }))
        throw `Cannot send msg to queue [${ replyKey! }] ${ replyKey }`;*/
      await self.receiveChannel.consume(streamEventsRefId, async (baseMsg: amqplib.ConsumeMessage | null): Promise<any> => {
        console.log('RECEIVE INITIAL');
        if (baseMsg === null) return self.uSelf.log.debug(`[RECEVIED ${ streamEventsRefId }]... as null`);
        try {
          clearTimeout(receiptTimeoutHandler);
          replyKey = LIB.getSpecialQueueKey(self.eventsChannelKey, baseMsg.properties.appId);
          replyKeyId = baseMsg.properties.correlationId;
          stream = new Readable({
            objectMode: true,
            read() {
              console.log('REQUEST READ');
              /*if (!self.publishChannel.sendToQueue(replyKey!, Buffer.from('""'), {
                expiration: self.queueOpts.messageTtl,
                correlationId: 's' + LIB.getLocalKey(streamId!, 'read'),
                appId: self.uSelf.myId,
                timestamp: new Date().getTime()
              }))
                throw `Cannot send msg to queue [${ replyKey! }] ${ replyKey }`;*/
            },
          });
          self.once('r' + streamId, (x) => stream!.emit(x.event, x.data || null, 'RECEIVED'));
          self.uSelf.log.debug(`[RECEVIED ${ streamRefId }] ${ baseMsg.properties.correlationId }`);
          /*if (!self.publishChannel.sendToQueue(replyKey!, Buffer.from('""'), {
            expiration: self.queueOpts.messageTtl,
            correlationId: LIB.getLocalKey(replyKeyId!, 'emit'),
            appId: self.uSelf.myId,
            timestamp: new Date().getTime()
          }))
            throw `Cannot send msg to queue [${ replyKey! }] ${ baseMsg.properties.correlationId }`;*/
          let eventsToListenTo = ['error', 'close', 'end'];
          for (let evnt of eventsToListenTo)
            stream.on(evnt, async (e: any, b: any) => {
              await cleanup();
              if (b === 'RECEIVED') return;
              if (!self.publishChannel.sendToQueue(replyKey!, Buffer.from(JSON.stringify({ event: evnt, data: e || null })), {
                expiration: self.queueOpts.messageTtl,
                correlationId: 's' + streamId,
                appId: self.uSelf.myId,
                timestamp: new Date().getTime()
              }))
                throw `Cannot send msg to queue [${ replyKey! }] ${ baseMsg.properties.correlationId }`;
            });
          console.log('EMIT READ:' + replyKey);
          await self.receiveChannel.consume(streamRefId, (msg: amqplib.ConsumeMessage | null): any => {
            console.log('PUSH TO STREAM');
            if (msg === null) return self.uSelf.log.debug(`[RECEVIED ${ streamRefId }]... as null`);
            stream!.push(decode(msg));
            if (!self.publishChannel.sendToQueue(replyKey!, Buffer.from('""'), {
              expiration: self.queueOpts.messageTtl,
              correlationId: 's' + LIB.getLocalKey(streamId!, 'read'),
              appId: self.uSelf.myId,
              timestamp: new Date().getTime()
            }))
              throw `Cannot send msg to queue [${ replyKey! }] ${ replyKey }`;
          });
          if (!self.publishChannel.sendToQueue(replyKey!, Buffer.from('""'), {
            expiration: self.queueOpts.messageTtl,
            correlationId: 's' + LIB.getLocalKey(streamId!, 'read'),
            appId: self.uSelf.myId,
            timestamp: new Date().getTime()
          }))
            throw `Cannot send msg to queue [${ replyKey! }] ${ replyKey }`;
          console.log('START READ');
          listener(null, stream);
        } catch (exc) {
          await cleanup();
          self.uSelf.log.fatal(exc);
        }
      });
      //});
      resolve(streamId);
    });
  }

  sendStream(callerPluginName: string, streamId: string, stream: Readable): Promise<void> {
    const self = this;
    const streamEventsRefId = LIB.getLocalKey(this.eventsChannelKey, streamId);
    const streamRefId = LIB.getLocalKey(this.streamChannelKey, streamId);
    this.uSelf.log.info(`SS: ${ callerPluginName } emitting ${ streamEventsRefId }`);
    return new Promise(async (resolve, rejectI) => {
      const timeout = Number.parseInt(streamId.split('-')[0]);
      let receiptTimeoutHandler: NodeJS.Timeout | null = setTimeout(() => {
        //reject(new Error('Send Receipt Timeout'));
      }, self.staticCommsTimeout);
      let timeoutHandler = setTimeout(() => {
        reject(new Error('Stream Timeout'));
      }, timeout * 1000);
      const cleanup = (eType: string, e?: Error) => {
        console.log('cleanup:', eType);
        stream.destroy(e);
        if (receiptTimeoutHandler !== null)
          clearTimeout(receiptTimeoutHandler);
        receiptTimeoutHandler = null;
        clearTimeout(timeoutHandler);
        self.removeAllListeners('s' + streamId);
        self.removeAllListeners('s' + LIB.getLocalKey(streamId!, 'read'));
      };
      const reject = (e: Error) => {
        cleanup('reject-' + e.message, e);
        rejectI(e);
      };
      /*let wstream = new Writable({
        objectMode: true,
        write(chunck, encoding) {
          if (!self.streamChannel.sendToQueue(streamRefId, encode(chunck), {
            expiration: self.queueOpts.messageTtl,
            correlationId: streamId,
            appId: self.uSelf.myId,
            timestamp: new Date().getTime()
          }))
            throw `Cannot send msg to queue [${ streamRefId }] ${ streamId }`;
        }
      });*/
      console.log('LIST: s' + LIB.getLocalKey(streamId!, 'read'));
      self.on('s' + LIB.getLocalKey(streamId!, 'read'), () => {
        console.log('RREAD DATA');
        if (receiptTimeoutHandler !== null) {
          clearTimeout(receiptTimeoutHandler);
          receiptTimeoutHandler = null;
        }
        if (!stream.readable) return;
        if (!self.streamChannel.sendToQueue(streamRefId, encode(stream.read()), {
          expiration: self.queueOpts.messageTtl,
          correlationId: 'r' + streamId,
          appId: self.uSelf.myId,
          timestamp: new Date().getTime()
        }))
          throw `Cannot send msg to queue [${ streamRefId }] ${ streamId }`;
      });
      //self.once(streamId, (x) => wstream!.emit(x.event, x.data || null, 'RECEIVED'));
      self.once('s' + streamId, (x) => stream!.emit(x.event, x.data || null, 'RECEIVED'));
      let eventsToListenTo = ['error', 'close', 'end'];
      for (let evnt of eventsToListenTo)
        //wstream.on(evnt, async (e: any, b: any) => {
        stream.on(evnt, async (e: any, b: any) => {
          await cleanup(evnt);
          if (b === 'RECEIVED') return;
          if (!self.publishChannel.sendToQueue(streamEventsRefId, Buffer.from(JSON.stringify({ event: evnt, data: e || null })), {
            expiration: self.queueOpts.messageTtl,
            correlationId: 'r' + streamId,
            appId: self.uSelf.myId,
            timestamp: new Date().getTime()
          }))
            throw `Cannot send msg to queue [${ streamEventsRefId }] ${ streamId }`;
          if (evnt === 'error') reject(e);
          if (evnt === 'close') resolve();
        });
      //self.emit(streamRefId, stream);
      self.uSelf.log.info(`SS: ${ callerPluginName } setup, ready ${ streamEventsRefId }`);
      //let queue = await self.streamChannel.assertQueue(streamRefId, self.streamQueueOpts);
      //pipeline(stream, wstream);
      console.log('SEND INITIAL');
      if (!self.publishChannel.sendToQueue(streamEventsRefId, Buffer.from(''), {
        expiration: self.queueOpts.messageTtl,
        correlationId: 'r' + streamId,
        appId: self.uSelf.myId,
        timestamp: new Date().getTime()
      }))
        throw `Cannot send msg to queue [${ streamEventsRefId }]`;
      self.uSelf.log.info(`SS: ${ callerPluginName } emitted ${ streamEventsRefId }`);
    });
  }
}