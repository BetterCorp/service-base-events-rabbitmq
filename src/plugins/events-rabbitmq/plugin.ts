import * as amqplib from 'amqplib';
import { IEvents, IPluginLogger, PluginFeature } from '@bettercorp/service-base/lib/ILib';
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

export class Events implements IEvents {
  private rabbitQConnection: any;
  private rabbitQConnectionEmitChannel: any;
  private readonly rabbitQConnectionEmitChannelKey = "eq";
  private rabbitQConnectionEARChannel: any;
  private readonly rabbitQConnectionEARChannelKey = "ar";
  private readonly rabbitQConnectionEARChannelKeyMine = "kr";
  private builtInEvents: any;
  private features!: PluginFeature;
  private logger!: IPluginLogger;
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
    messageTtl: (60 * 60 * 360) * 1000 // 360 minutes
    //expires: (60*60*360)*1000 // 360 minutes
  };
  private readonly earExchangeQueue: any = {
    durable: false,
    //exclusive: true,
    autoDelete: true,
    messageTtl: (60 * 60 * 60) * 1000, // 60 minutes
    //expires: (60 * 60 * 60) * 1000 // 60 minutes
  };

  init(features: PluginFeature): Promise<void> {
    const self = this;
    self.features = features;
    self.logger = features.log;

    return new Promise(async (resolve, reject) => {
      try {
        features.log.info(`Connect to ${ features.getPluginConfig<IPluginConfig>().endpoint }`);
        if (Tools.isNullOrUndefined(features.getPluginConfig<IPluginConfig>().credentials)) {
          throw new Error('Plugin credentials not defined in sec.config.json');
        }
        self.rabbitQConnection = await amqplib.connect(features.getPluginConfig<IPluginConfig>().endpoint, {
          credentials: amqplib.credentials.plain(features.getPluginConfig<IPluginConfig>().credentials.username, features.getPluginConfig<IPluginConfig>().credentials.password)
        });
        features.log.info(`Connected to ${ features.getPluginConfig<IPluginConfig>().endpoint }`);

        await self._setupEmitHandler(features);
        await self._setupEARHandler(features);

        resolve();
      } catch (exce) {
        features.log.error(exce);
        reject(exce);
      }
    });
  }
  private _emitEvent<T = any>(queueKey: string, queueAttr: any, plugin: string, pluginName: string | null, event: string, channel: any, data?: T, additionalArgs?: any): void {
    const self = this;
    self.features.log.debug(plugin, ` - EMIT: [${ queueKey }-${ pluginName || plugin }-${ event }]`, data);
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
      channel.sendToQueue(`${ queueKey }-${ pluginName || plugin }-${ event }`, Buffer.from(JSON.stringify(data)));
      self.features.log.debug(plugin, ` - EMIT: [${ queueKey }-${ pluginName || plugin }-${ event }] - EMITTED`, data);
    });
  }

  private async _setupEmitHandler(features: PluginFeature) {
    let self = this;
    features.log.info(`Open emit channel (${ self.emitExchange.name })`);
    self.rabbitQConnectionEmitChannel = await self.rabbitQConnection.createChannel();
    self.rabbitQConnectionEmitChannel.assertExchange(self.emitExchange.name, self.emitExchange.type, self.emitExchangeQueue);
    features.log.info(`Open emit channel (${ self.emitExchange.name }) - PREFETCH x${ features.getPluginConfig<IPluginConfig>().prefetch }`);
    self.rabbitQConnectionEmitChannel.prefetch(features.getPluginConfig<IPluginConfig>().prefetch);
    features.log.info(`Open emit channel (${ self.emitExchange.name }) - COMPLETED`);
  }
  onEvent<T = any>(plugin: string, pluginName: string | null, event: string, listener: (data: T) => void): void {
    const self = this;
    self.features.log.info(plugin, ` - LISTEN: [${ self.rabbitQConnectionEmitChannelKey }-${ pluginName || plugin }-${ event }]`);

    let ok = self.rabbitQConnectionEmitChannel.assertQueue(`${ self.rabbitQConnectionEmitChannelKey }-${ pluginName || plugin }-${ event }`, self.emitExchangeQueue);
    ok = ok.then(function () {
      self.features.log.info(plugin, ` - LISTEN: [${ `${ self.rabbitQConnectionEmitChannelKey }-${ pluginName || plugin }-${ event }` }] - LISTENING`);
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
    this._emitEvent<T>(this.rabbitQConnectionEmitChannelKey, this.emitExchangeQueue, plugin, pluginName, event, this.rabbitQConnectionEmitChannel, data);
  }


  private async _setupEARHandler(features: PluginFeature) {
    let self = this;
    features.log.info(`Ready my events name`);
    self.earExchange.myResponseName = `${ features.cwd }`.replace(/[\W-]/g, '').toLowerCase() +
      ((features.getPluginConfig<IPluginConfig>().noRandomDebugName === true ? '' : features.config.debug ? `-${ Math.random() }` : '')) +
      (features.getPluginConfig<IPluginConfig>().uniqueId || '');
    features.log.info(`Ready my events name - ${ self.earExchange.myResponseName }`);

    features.log.info(`Ready internal events`);
    self.builtInEvents = new (EVENT_EMITTER as any)();
    features.log.info(`Ready internal events - COMPLETED`);

    features.log.info(`Open EAR channel (${ self.earExchange.name })`);
    self.rabbitQConnectionEARChannel = await self.rabbitQConnection.createChannel();
    self.rabbitQConnectionEARChannel.assertExchange(self.earExchange.name, self.earExchange.type, self.earExchangeQueue);
    features.log.info(`Open EAR channel (${ self.earExchange.name }) - PREFETCH x${ features.getPluginConfig<IPluginConfig>().prefetchEAR }`);
    self.rabbitQConnectionEARChannel.prefetch(features.getPluginConfig<IPluginConfig>().prefetchEAR);
    features.log.info(`Open EAR channel (${ self.earExchange.name }) - ${ self.earExchange.myResponseName } - LISTEN`);
    let eventEARQueue = self.rabbitQConnectionEARChannel.assertQueue(`${self.rabbitQConnectionEARChannelKeyMine}-${ OS.hostname() }-${ self.earExchange.myResponseName }`, self.earExchangeQueue);
    eventEARQueue = eventEARQueue.then(function () {
      self.features.log.info(` - LISTEN: [${ self.earExchange.myResponseName }] - LISTENING`);
    });
    eventEARQueue = eventEARQueue.then(function () {
      self.rabbitQConnectionEARChannel.consume(`${self.rabbitQConnectionEARChannelKeyMine}-${ OS.hostname() }-${ self.earExchange.myResponseName }`, (msg: any) => {
        let body = msg.content.toString();
        self.rabbitQConnectionEARChannel.ack(msg);
        self.features.log.debug(`[RECEVIED ${ self.earExchange.myResponseName }]:`, body);
        const bodyObj = JSON.parse(body) as internalEvent<any>;
        self.builtInEvents.emit(bodyObj.id, bodyObj);
      }, { noAck: false });
    });
    features.log.info(`Open EAR channel (${ self.earExchange.name }) - COMPLETED`);
  }
  onReturnableEvent<T = any>(plugin: string, pluginName: string | null, event: string, listener: (resolve: Function, reject: Function, data: T) => void): void {
    const self = this;
    self.features.log.info(plugin, ` - LISTEN EAR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }]`);

    let ok = self.rabbitQConnectionEARChannel.assertQueue(`${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }`, self.earExchangeQueue);
    ok = ok.then(function () {
      self.features.log.info(plugin, ` - LISTEN EAR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }] - LISTENING`);
    });
    //ok = ok.then(function () { channel.prefetch(1); });
    ok = ok.then(function () {
      self.rabbitQConnectionEARChannel.consume(`${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }`, (msg: any) => {
        let body = msg.content.toString();
        const bodyObj = JSON.parse(body) as transEAREvent<T>;
        listener((x: any) => {
          self.rabbitQConnectionEARChannel.ack(msg);
          self.logger.debug(plugin, ` - RETURN OKAY: [${ self.rabbitQConnectionEARChannelKeyMine }-${ pluginName || plugin }-${ event }]`, bodyObj);
          self._emitEvent(self.rabbitQConnectionEARChannelKeyMine, self.earExchangeQueue, plugin, bodyObj.plugin, bodyObj.topic, self.rabbitQConnectionEARChannel, {
            data: x,
            id: bodyObj.id,
            resultSuccess: true
          } as internalEvent<T>);
        }, (x: any) => {
          self.rabbitQConnectionEARChannel.ack(msg);
          self.logger.debug(plugin, ` - RETURN ERROR: [${ self.rabbitQConnectionEARChannelKeyMine }-${ pluginName || plugin }-${ event }]`, bodyObj);
          self._emitEvent(self.rabbitQConnectionEARChannelKeyMine, self.earExchangeQueue, plugin, bodyObj.plugin, bodyObj.topic, self.rabbitQConnectionEARChannel, {
            data: x,
            id: bodyObj.id,
            resultSuccess: false
          } as internalEvent<T>);
        }, bodyObj.data);
      }, { noAck: false });
    });
  }
  emitEventAndReturn<T1 = any, T2 = void>(plugin: string, pluginName: string | null, event: string, data?: T1, timeoutSeconds: number = 10): Promise<T2> {
    const self = this;
    this.logger.debug(plugin, ` - EMIT EAR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }]`, data);
    return new Promise((resolve, reject) => {
      const resultKey = `${ UUID() }-${ new Date().getTime() }${ Math.random() }`;
      const xtimeoutSeconds = timeoutSeconds || 10;
      const additionalArgs: any = {
        "$$TIME": new Date().getTime(),
        expires: (xtimeoutSeconds * 1000) + 5000,
      };
      if (additionalArgs.expires >= self.earExchangeQueue) return reject(`TTL CANNOT BE GREATER THAN: ${self.earExchangeQueue-2}ms`)
      let qArguments: any = JSON.parse(JSON.stringify(self.earExchangeQueue));
      for (let iKey of Object.keys(additionalArgs))
        qArguments[iKey] = additionalArgs[iKey];

      const listener = (data: internalEvent<T2>) => {
        if (timeoutTimer === null)
          return this.logger.debug(plugin, ` - REC EAR TOO LATE: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }]`, data.resultSuccess ? 'SUCCESS' : 'ERRORED', data);
        this.logger.debug(plugin, ` - REC EAR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }]`, data.resultSuccess ? 'SUCCESS' : 'ERRORED', data);
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
        self.features.log.debug(plugin, ` - EMIT AR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }-${ resultKey }]`, 'TIMED OUT');
        reject(`NO RESPONSE IN TIME: ${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }/${ resultKey } x${ ((data || {}) as any).timeoutSeconds || 10 }s`);
      }, timeoutSeconds * 1000);

      self.builtInEvents.once(resultKey, listener);

      self._emitEvent(self.rabbitQConnectionEARChannelKey, self.earExchangeQueue, plugin, pluginName, event, self.rabbitQConnectionEARChannel, {
        id: resultKey,
        data: data,
        plugin: OS.hostname(),
        topic: self.earExchange.myResponseName
      } as transEAREvent<T1>, qArguments);

      self.features.log.debug(plugin, ` - EMIT EAR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }-${ resultKey }] - EMITTED`, data);
    });
  }
}