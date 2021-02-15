import * as amqplib from 'amqplib';
import { IEvents, IPluginLogger, PluginFeature } from '@bettercorp/service-base/lib/ILib';
import { v4 as UUID } from 'uuid';
import { Tools } from '@bettercorp/tools/lib/Tools';
import * as EVENT_EMITTER from 'events';
import * as OS from 'os';

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
  private rabbitQConnectionEARChannel: any;
  private builtInEvents: any;
  private features!: PluginFeature;
  private logger!: IPluginLogger;
  private emitExchange: any = {
    type: 'direct',
    name: 'better-service-emit'
  };
  private earExchange: any = {
    type: 'fanout',
    name: 'better-service-ear',
    myResponseName: null
  };

  init (features: PluginFeature): Promise<void> {
    const self = this;
    self.features = features;
    self.logger = features.log;

    return new Promise(async (resolve, reject) => {
      try {
        features.log.info(`Ready my events name`);
        self.earExchange.myResponseName = `${OS.hostname()}-${features.cwd}`.replace(/[\W-]/g, '').toLowerCase();// + (features.config.debug ? `-${Math.random()}`: '');
        features.log.info(`Ready my events name - ${self.earExchange.myResponseName}`);

        features.log.info(`Ready internal events`);
        self.builtInEvents = new (EVENT_EMITTER as any)();
        features.log.info(`Ready internal events - COMPLETED`);

        features.log.info(`Connect to ${features.getPluginConfig().endpoint}`);
        if (Tools.isNullOrUndefined(features.getPluginConfig().credentials)) {
          throw new Error('Plugin credentials not defined in sec.config.json');
        }
        self.rabbitQConnection = await amqplib.connect(features.getPluginConfig().endpoint, {
          credentials: amqplib.credentials.plain(features.getPluginConfig().credentials.username, features.getPluginConfig().credentials.password)
        });
        features.log.info(`Connected to ${features.getPluginConfig().endpoint}`);

        features.log.info(`Open emit channel (${self.emitExchange.name})`);
        self.rabbitQConnectionEmitChannel = await self.rabbitQConnection.createChannel();
        self.rabbitQConnectionEmitChannel.assertExchange(self.emitExchange.name, self.emitExchange.type, {
          durable: true
        });
        features.log.info(`Open emit channel (${self.emitExchange.name}) - PREFETCH`);
        self.rabbitQConnectionEmitChannel.prefetch(1);
        features.log.info(`Open emit channel (${self.emitExchange.name}) - COMPLETED`);

        features.log.info(`Open EAR channel (${self.earExchange.name})`);
        self.rabbitQConnectionEARChannel = await self.rabbitQConnection.createChannel();
        self.rabbitQConnectionEARChannel.assertExchange(self.earExchange.name, self.earExchange.type, {
          durable: true
        });
        features.log.info(`Open EAR channel (${self.earExchange.name}) - PREFETCH`);
        self.rabbitQConnectionEARChannel.prefetch(1);
        features.log.info(`Open EAR channel (${self.earExchange.name}) - ${self.features.pluginName}-${self.earExchange.myResponseName} - LISTEN`);
        let eventEARQueue = self.rabbitQConnectionEmitChannel.assertQueue(`${self.features.pluginName}-${self.earExchange.myResponseName}`, {
          durable: true
        });
        eventEARQueue = eventEARQueue.then(function () {
          self.features.log.info(` - LISTEN: [${self.features.pluginName}-${self.earExchange.myResponseName}] - LISTENING`);
        });
        eventEARQueue = eventEARQueue.then(function () {
          self.rabbitQConnectionEmitChannel.consume(`${self.features.pluginName}-${self.earExchange.myResponseName}`, (msg: any) => {
            let body = msg.content.toString();
            self.rabbitQConnectionEmitChannel.ack(msg);
            self.features.log.debug(`[RECEVIED ${self.earExchange.myResponseName}]:`, body);
            const bodyObj = JSON.parse(body) as internalEvent<any>;
            self.builtInEvents.emit(bodyObj.id, bodyObj);
          }, { noAck: false });
        });
        features.log.info(`Open EAR channel (${self.earExchange.name}) - COMPLETED`);

        resolve();
      } catch (exce) {
        features.log.error(exce);
        reject(exce);
      }
    });
  }
  onEvent<T = any> (plugin: string, pluginName: string | null, event: string, listener: (data: T) => void): void {
    const self = this;
    self.features.log.info(plugin, ` - LISTEN: [${`${pluginName || plugin}-${event}`}]`);
    let qArguments: any = {
      durable: true
    };

    let ok = self.rabbitQConnectionEmitChannel.assertQueue(`${pluginName || plugin}-${event}`, qArguments);
    ok = ok.then(function () {
      self.features.log.info(plugin, ` - LISTEN: [${`${pluginName || plugin}-${event}`}] - LISTENING`);
    });
    //ok = ok.then(function () { channel.prefetch(1); });
    ok = ok.then(function () {
      self.rabbitQConnectionEmitChannel.consume(`${pluginName || plugin}-${event}`, (msg: any) => {
        let body = msg.content.toString();
        self.rabbitQConnectionEmitChannel.ack(msg);
        const bodyObj = JSON.parse(body) as any;
        listener(bodyObj as T);
      }, { noAck: false });
    });
  }
  private _emitEvent<T = any> (plugin: string, pluginName: string | null, event: string, channel: any, data?: T, additionalArgs?: any): void {
    const self = this;
    self.features.log.debug(plugin, ` - EMIT: [${`${pluginName || plugin}-${event}`}]`, data);
    let qArguments: any = {
      durable: true
    };
    if (!Tools.isNullOrUndefined(additionalArgs)) {
      for (let iKey of Object.keys(additionalArgs))
        qArguments[iKey] = additionalArgs[iKey];
    }
    var ok = channel.assertQueue(`${pluginName || plugin}-${event}`, qArguments);

    ok.then(function (_qok: any) {
      // NB: `sentToQueue` and `publish` both return a boolean
      // indicating whether it's OK to send again straight away, or
      // (when `false`) that you should wait for the event `'drain'`
      // to fire before writing again. We're just doing the one write,
      // so we'll ignore it.
      channel.sendToQueue(`${pluginName || plugin}-${event}`, Buffer.from(JSON.stringify(data)));
      self.features.log.debug(plugin, ` - EMIT: [${`${pluginName || plugin}-${event}`}] - EMITTED`, data);
    });
  }
  emitEvent<T = any> (plugin: string, pluginName: string | null, event: string, data?: T): void {
    this._emitEvent<T>(plugin, pluginName, event, this.rabbitQConnectionEmitChannel, data);
  }
  onReturnableEvent<T = any> (plugin: string, pluginName: string | null, event: string, listener: (resolve: Function, reject: Function, data: T) => void): void {
    const self = this;
    self.features.log.info(plugin, ` - LISTEN EAR: [${`${pluginName || plugin}-${event}`}]`);
    let qArguments: any = {
      durable: true
    };

    let ok = self.rabbitQConnectionEARChannel.assertQueue(`${pluginName || plugin}-${event}`, qArguments);
    ok = ok.then(function () {
      self.features.log.info(plugin, ` - LISTEN EAR: [${`${pluginName || plugin}-${event}`}] - LISTENING`);
    });
    //ok = ok.then(function () { channel.prefetch(1); });
    ok = ok.then(function () {
      self.rabbitQConnectionEARChannel.consume(`${pluginName || plugin}-${event}`, (msg: any) => {
        let body = msg.content.toString();
        self.rabbitQConnectionEARChannel.ack(msg);
        const bodyObj = JSON.parse(body) as transEAREvent<T>;
        listener((x: any) => {
          self.logger.debug(plugin, ` - RETURN OKAY: [${`${pluginName || plugin}-${event}`}]`, bodyObj);
          self._emitEvent(plugin, bodyObj.plugin, bodyObj.topic, self.rabbitQConnectionEARChannel, {
            data: x,
            id: bodyObj.id,
            resultSuccess: true
          } as internalEvent<T>);
        }, (x: any) => {
          self.logger.debug(plugin, ` - RETURN ERROR: [${`${pluginName || plugin}-${event}`}]`, bodyObj);
          self._emitEvent(plugin, bodyObj.plugin, bodyObj.topic, self.rabbitQConnectionEARChannel, {
            data: x,
            id: bodyObj.id,
            resultSuccess: false
          } as internalEvent<T>);
        }, bodyObj.data);
      }, { noAck: false });
    });
  }
  emitEventAndReturn<T1 = any, T2 = void> (plugin: string, pluginName: string | null, event: string, data?: T1): Promise<T2> {
    const self = this;
    this.logger.debug(plugin, ` - EMIT EAR: [${`${pluginName || plugin}-${event}`}]`, data);
    return new Promise((resolve, reject) => {
      const resultKey = `${UUID()}-${new Date().getTime()}${Math.random()}`;
      const timeoutSeconds = ((data || {}) as any).timeoutSeconds || 10;
      const args = {
        durable: true,
        //autoDelete: true,
        "x-expires": (timeoutSeconds * 1000) + 5000,
        "x-message-ttl": timeoutSeconds * 1000,
        "$$TIME": new Date().getTime()
      };

      const listener = (data: internalEvent<T2>) => {
        if (timeoutTimer === null)
          return this.logger.debug(plugin, ` - REC EAR TOO LATE: [${`${pluginName || plugin}-${event}`}]`, data.resultSuccess ? 'SUCCESS' : 'ERRORED', data);
        this.logger.debug(plugin, ` - REC EAR: [${`${pluginName || plugin}-${event}`}]`, data.resultSuccess ? 'SUCCESS' : 'ERRORED', data);
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
        self.features.log.debug(plugin, ` - EMIT AR: [${`${pluginName || plugin}-${event}`}-${resultKey}]`, 'TIMED OUT');
        reject(`NO RESPONSE IN TIME: ${pluginName || plugin}/${resultKey} x${((data || {}) as any).timeoutSeconds || 10}s`);
      }, timeoutSeconds * 1000);

      self.builtInEvents.once(resultKey, listener);

      self._emitEvent(plugin, pluginName, event, self.rabbitQConnectionEARChannel, {
        id: resultKey,
        data: data,
        plugin: plugin,
        topic: self.earExchange.myResponseName
      } as transEAREvent<T1>, args);

      // NB: `sentToQueue` and `publish` both return a boolean
      // indicating whether it's OK to send again straight away, or
      // (when `false`) that you should wait for the event `'drain'`
      // to fire before writing again. We're just doing the one write,
      // so we'll ignore it.
      /*setTimeout(() => {
        self.rabbitQConnectionEmitARChannel.sendToQueue(`${pluginName || plugin}-${event}`, Buffer.from(JSON.stringify({
          resultKey: resultKey,
          resultNames: {
            plugin: pluginName || plugin,
            success: `result`,
            error: `error`
          },
          data: data
        })));
      }, 500);*/
      self.features.log.debug(plugin, ` - EMIT EAR: [${`${pluginName || plugin}-${event}`}-${resultKey}] - EMITTED`, data);
    });
  }
}