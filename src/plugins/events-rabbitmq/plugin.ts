import * as amqplib from 'amqplib';
import { CEvents } from '@bettercorp/service-base/lib/ILib';
import { v4 as UUID } from 'uuid';
import { Tools } from '@bettercorp/tools/lib/Tools';
import * as EVENT_EMITTER from 'events';
import * as OS from 'os';
import { IPluginConfig } from './sec.config';

interface internalEvent<T> {
  data: T;
  resultSuccess: Boolean;
  id: string;
}
interface transEAREvent<T> {
  data: T;
  topic: string;
  plugin: string;
  id: string;
}

export class Events extends CEvents {
  private rabbitQConnection: any;
  private rabbitQConnectionEmitChannel: any;
  private readonly rabbitQConnectionEmitChannelKey = "eq";
  private rabbitQConnectionEARChannel: any;
  private readonly rabbitQConnectionEARChannelKey = "ar";
  private readonly rabbitQConnectionEARChannelKeyMine = "kr";
  private builtInEvents: any;

  private readonly emitExchange: any = {
    type: 'fanout',
    name: 'better-service-emit'
  };
  private readonly earExchange: any = {
    type: 'direct',
    name: 'better-service-ear',
    myResponseName: null
  };
  private readonly emitExchangeQueue: any = {
    durable: true,
    messageTtl: (60 * 60 * 60 * 6) * 1000 // 6h
    //expires: (60*60*360)*1000 // 360 minutes
  };
  private readonly earExchangeQueue: any = {
    durable: false,
    //exclusive: true,
    autoDelete: true,
    messageTtl: (60 * 60 * 60) * 1000, // 60 minutes
    //expires: (60 * 60 * 60) * 1000 // 60 minutes
  };

  init(): Promise<void> {
    const self = this;
    return new Promise(async (resolve, reject) => {
      try {
        await self._connectToAMQP();
        resolve();
      } catch (exce) {
        self.log.fatal(exce);
        reject(exce);
      }
    });
  }
  private async _connectToAMQP() {
    this.log.info(`Connect to ${ this.getPluginConfig<IPluginConfig>().endpoint }`);
    if (Tools.isNullOrUndefined(this.getPluginConfig<IPluginConfig>().credentials)) {
      throw new Error('Plugin credentials not defined in sec.config.json');
    }
    this.rabbitQConnection = await amqplib.connect(this.getPluginConfig<IPluginConfig>().endpoint, {
      credentials: amqplib.credentials.plain(this.getPluginConfig<IPluginConfig>().credentials.username, this.getPluginConfig<IPluginConfig>().credentials.password)
    });
    const self = this;
    this.rabbitQConnection.on("error", (err: any) => {
      if (err.message !== "Connection closing") {
        self.log.error('AMQP ERROR', err.message);
      }
    });
    this.rabbitQConnection.on("close", () => {
      self.log.error('AMQP CONNECTION CLOSED');
      self.log.fatal('AMQP Error: Connection closed');
    });
    this.log.info(`Connected to ${ this.getPluginConfig<IPluginConfig>().endpoint }`);

    await this._setupEmitHandler();
    await this._setupEARHandler();
  }

  private _emitEventAsync<T = any>(queueKey: string, queueAttr: any, plugin: string, pluginName: string | null, event: string, channel: any, data?: T, additionalArgs?: any): Promise<void> {
    const self = this;
    return new Promise((resolve, reject) => {
      self.log.debug(plugin, ` - EMIT: [${ queueKey }-${ pluginName || plugin }-${ event }]`, data);
      let qArguments: any = undefined;
      if (!Tools.isNullOrUndefined(additionalArgs)) {
        qArguments = JSON.parse(JSON.stringify(queueAttr));
        for (let iKey of Object.keys(additionalArgs))
          qArguments[iKey] = additionalArgs[iKey];
      }
      var ok = channel.assertQueue(`${ queueKey }-${ pluginName || plugin }-${ event }`, qArguments || queueAttr);

      ok.then(function (_qok: any) {
        // NB: `sentToQueue` and `publish` both return a boolean
        // indicating whether it's OK to send again straight away, or
        // (when `false`) that you should wait for the event `'drain'`
        // to fire before writing again. We're just doing the one write,
        // so we'll ignore it.
        if (!channel.sendToQueue(`${ queueKey }-${ pluginName || plugin }-${ event }`, Buffer.from(JSON.stringify(data))))
          return reject([`Cannot send msg to queue [${ queueKey }-${ pluginName || plugin }-${ event }]`, data]);
        self.log.debug(plugin, ` - EMIT: [${ queueKey }-${ pluginName || plugin }-${ event }] - EMITTED`, data);
        resolve();
      }).catch((x: any) => {
        reject([`Cannot assert queue [${ queueKey }-${ pluginName || plugin }-${ event }]`, x]);
      });
    });
  }

  private async _setupEmitHandler() {
    this.log.info(`Open emit channel (${ this.emitExchange.name })`);
    this.rabbitQConnectionEmitChannel = await this.rabbitQConnection.createChannel();
    const self = this;
    this.rabbitQConnectionEmitChannel.on("close", () => {
      self.log.error('AMQP rabbitQConnectionEmitChannel close');
      self.log.fatal('AMQP Error: rabbitQConnectionEmitChannel close');
    });
    this.rabbitQConnectionEmitChannel.on("error", (err: any) => {
      self.log.error('AMQP rabbitQConnectionEmitChannel error', err);
      self.log.fatal('AMQP Error: rabbitQConnectionEmitChannel error');
    });
    this.rabbitQConnectionEmitChannel.assertExchange(this.emitExchange.name, this.emitExchange.type, this.emitExchangeQueue);
    this.log.info(`Open emit channel (${ this.emitExchange.name }) - PREFETCH x${ this.getPluginConfig<IPluginConfig>().prefetch }`);
    this.rabbitQConnectionEmitChannel.prefetch(this.getPluginConfig<IPluginConfig>().prefetch);
    this.log.info(`Open emit channel (${ this.emitExchange.name }) - COMPLETED`);
  }
  onEvent<T = any>(plugin: string, pluginName: string | null, event: string, listener: (data: T) => void): void {
    const self = this;
    self.log.info(plugin, ` - LISTEN: [${ self.rabbitQConnectionEmitChannelKey }-${ pluginName || plugin }-${ event }]`);

    let ok = self.rabbitQConnectionEmitChannel.assertQueue(`${ self.rabbitQConnectionEmitChannelKey }-${ pluginName || plugin }-${ event }`, self.emitExchangeQueue);
    ok = ok.then(function () {
      self.log.info(plugin, ` - LISTEN: [${ `${ self.rabbitQConnectionEmitChannelKey }-${ pluginName || plugin }-${ event }` }] - LISTENING`);
    });
    //ok = ok.then(function () { channel.prefetch(1); });
    ok = ok.then(function () {
      self.rabbitQConnectionEmitChannel.consume(`${ self.rabbitQConnectionEmitChannelKey }-${ pluginName || plugin }-${ event }`, (msg: any) => {
        let body = msg.content.toString();
        const bodyObj = JSON.parse(body) as any;
        listener(bodyObj as T);
        self.rabbitQConnectionEmitChannel.ack(msg);
      }, { noAck: false });
    });
  }
  emitEvent<T = any>(plugin: string, pluginName: string | null, event: string, data?: T): void {
    this._emitEventAsync<T>(this.rabbitQConnectionEmitChannelKey, this.emitExchangeQueue, plugin, pluginName, event, this.rabbitQConnectionEmitChannel, data).then(this.log.debug).catch(this.log.fatal);
  }


  private async _setupEARHandler() {
    this.log.info(`Ready my events name`);
    this.earExchange.myResponseName = `${ this.cwd }`.replace(/[\W-]/g, '').toLowerCase() +
      ((this.getPluginConfig<IPluginConfig>().noRandomDebugName === true ? '' : this.appConfig.runningInDebug || !this.appConfig.runningLive ? `-${ Math.random() }` : '')) +
      (this.getPluginConfig<IPluginConfig>().uniqueId || '');
    this.log.info(`Ready my events name - ${ this.earExchange.myResponseName }`);

    this.log.info(`Ready internal events`);
    this.builtInEvents = new (EVENT_EMITTER as any)();
    this.log.info(`Ready internal events - COMPLETED`);

    this.log.info(`Open EAR channel (${ this.earExchange.name })`);
    this.rabbitQConnectionEARChannel = await this.rabbitQConnection.createChannel();
    const self = this;
    this.rabbitQConnectionEARChannel.on("close", () => {
      self.log.error('AMQP rabbitQConnectionEARChannel close');
      self.log.fatal('AMQP Error: rabbitQConnectionEARChannel close');
    });
    this.rabbitQConnectionEARChannel.on("error", (err: any) => {
      self.log.error('AMQP rabbitQConnectionEARChannel error', err);
      self.log.fatal('AMQP Error: rabbitQConnectionEARChannel error');
    });
    this.rabbitQConnectionEARChannel.assertExchange(this.earExchange.name, this.earExchange.type, this.earExchangeQueue);
    this.log.info(`Open EAR channel (${ this.earExchange.name }) - PREFETCH x${ this.getPluginConfig<IPluginConfig>().prefetchEAR }`);
    this.rabbitQConnectionEARChannel.prefetch(this.getPluginConfig<IPluginConfig>().prefetchEAR);
    this.log.info(`Open EAR channel (${ this.earExchange.name }) - ${ this.earExchange.myResponseName } - LISTEN`);
    let eventEARQueue = this.rabbitQConnectionEARChannel.assertQueue(`${ this.rabbitQConnectionEARChannelKeyMine }-${ OS.hostname() }-${ this.earExchange.myResponseName }`, this.earExchangeQueue);
    eventEARQueue = eventEARQueue.then(function () {
      self.log.info(` - LISTEN: [${ self.earExchange.myResponseName }] - LISTENING`);
    });
    eventEARQueue = eventEARQueue.then(function () {
      self.rabbitQConnectionEARChannel.consume(`${ self.rabbitQConnectionEARChannelKeyMine }-${ OS.hostname() }-${ self.earExchange.myResponseName }`, (msg: any) => {
        let body = msg.content.toString();
        self.rabbitQConnectionEARChannel.ack(msg);
        self.log.debug(`[RECEVIED ${ self.earExchange.myResponseName }]:`, body);
        const bodyObj = JSON.parse(body) as internalEvent<any>;
        self.builtInEvents.emit(bodyObj.id, bodyObj);
      }, { noAck: false });
    });
    this.log.info(`Open EAR channel (${ this.earExchange.name }) - COMPLETED`);
  }
  onReturnableEvent<T = any>(plugin: string, pluginName: string | null, event: string, listener: (resolve: Function, reject: Function, data: T) => void): void {
    const self = this;
    self.log.info(plugin, ` - LISTEN EAR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }]`);

    let ok = self.rabbitQConnectionEARChannel.assertQueue(`${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }`, self.earExchangeQueue);
    ok = ok.then(function () {
      self.log.info(plugin, ` - LISTEN EAR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }] - LISTENING`);
    });
    //ok = ok.then(function () { channel.prefetch(1); });
    ok = ok.then(function () {
      self.rabbitQConnectionEARChannel.consume(`${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }`, (msg: any) => {
        let body = msg.content.toString();
        const bodyObj = JSON.parse(body) as transEAREvent<T>;
        listener((x: any) => {
          self.rabbitQConnectionEARChannel.ack(msg);
          self.log.debug(plugin, ` - RETURN OKAY: [${ self.rabbitQConnectionEARChannelKeyMine }-${ pluginName || plugin }-${ event }]`, bodyObj);
          self._emitEventAsync(self.rabbitQConnectionEARChannelKeyMine, self.earExchangeQueue, plugin, bodyObj.plugin, bodyObj.topic, self.rabbitQConnectionEARChannel, {
            data: x,
            id: bodyObj.id,
            resultSuccess: true
          } as internalEvent<T>).then(self.log.debug).catch(self.log.fatal);
        }, (x: any) => {
          self.rabbitQConnectionEARChannel.ack(msg);
          self.log.debug(plugin, ` - RETURN ERROR: [${ self.rabbitQConnectionEARChannelKeyMine }-${ pluginName || plugin }-${ event }]`, bodyObj);
          self._emitEventAsync(self.rabbitQConnectionEARChannelKeyMine, self.earExchangeQueue, plugin, bodyObj.plugin, bodyObj.topic, self.rabbitQConnectionEARChannel, {
            data: x,
            id: bodyObj.id,
            resultSuccess: false
          } as internalEvent<T>).then(self.log.debug).catch(self.log.fatal);
        }, bodyObj.data);
      }, { noAck: false });
    });
  }
  emitEventAndReturn<T1 = any, T2 = void>(plugin: string, pluginName: string | null, event: string, data?: T1, timeoutSeconds: number = 10): Promise<T2> {
    const self = this;
    this.log.debug(plugin, ` - EMIT EAR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }]`, data);
    return new Promise((resolve, reject) => {
      const resultKey = `${ UUID() }-${ new Date().getTime() }${ Math.random() }`;
      const xtimeoutSeconds = timeoutSeconds || 10;
      const additionalArgs: any = {
        "$$TIME": new Date().getTime(),
        expiration: (xtimeoutSeconds * 1000) + 5000,
      };
      if (additionalArgs.expiration >= self.earExchangeQueue) return reject(`TTL CANNOT BE GREATER THAN: ${ self.earExchangeQueue - 2 }ms`);
      let qArguments: any = JSON.parse(JSON.stringify(self.earExchangeQueue));
      for (let iKey of Object.keys(additionalArgs))
        qArguments[iKey] = additionalArgs[iKey];

      const listener = (data: internalEvent<T2>) => {
        if (timeoutTimer === null)
          return this.log.debug(plugin, ` - REC EAR TOO LATE: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }]`, data.resultSuccess ? 'SUCCESS' : 'ERRORED', data);
        this.log.debug(plugin, ` - REC EAR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }]`, data.resultSuccess ? 'SUCCESS' : 'ERRORED', data);
        clearTimeout(timeoutTimer);
        timeoutTimer = null;
        if (data.resultSuccess)
          resolve(data.data);
        else
          reject(data.data);
      };

      let timeoutTimer: any = setTimeout(() => {
        if (timeoutTimer === null)
          return;
        clearTimeout(timeoutTimer);
        timeoutTimer = null;
        self.builtInEvents.removeListener(resultKey, listener);
        self.log.debug(plugin, ` - EMIT AR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }-${ resultKey }]`, 'TIMED OUT');
        reject(`NO RESPONSE IN TIME: ${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }/${ resultKey } x${ ((data || { }) as any).timeoutSeconds || 10 }s`);
      }, timeoutSeconds * 1000);

      self.builtInEvents.once(resultKey, listener);

      self._emitEventAsync(self.rabbitQConnectionEARChannelKey, self.earExchangeQueue, plugin, pluginName, event, self.rabbitQConnectionEARChannel, {
        id: resultKey,
        data: data,
        plugin: OS.hostname(),
        topic: self.earExchange.myResponseName
      } as transEAREvent<T1>, qArguments).then(self.log.debug).catch(self.log.fatal);

      self.log.debug(plugin, ` - EMIT EAR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }-${ resultKey }] - EMITTED`, data);
    });
  }
}