import * as amqplib from 'amqplib';
import { IEmitter, IEvents, PluginFeature } from '@bettercorp/service-base/lib/ILib';
import { v4 as UUID } from 'uuid';
import { Tools } from '@bettercorp/tools/lib/Tools';

export class Events implements IEvents {
  private rabbitQConnection: any;
  private rabbitQConnectionEmitChannel: any;
  private rabbitQConnectionEventChannel: any;
  private rabbitQConnectionEmitARChannel: any;
  private rabbitQConnectionARGSChannel: any;
  private features!: PluginFeature;
  private runningArgs: any = {};
  private defaultExchange: any = {
    type: 'direct',
    name: 'better-service-events'
  };
  private coreExchange: any = {
    type: 'fanout',
    name: 'better-service-args'
  };

  init (features: PluginFeature): Promise<void> {
    const self = this;
    self.features = features;

    //self.defaultExchange.name = `${self.features.pluginName.substr(0, self.features.pluginName.length > 20 ? 20 : self.features.pluginName.length)}-events`;
    //self.coreExchange.name = `${self.features.pluginName.substr(0, self.features.pluginName.length > 20 ? 20 : self.features.pluginName.length)}-args`;

    setInterval(() => {
      let timeNow = new Date().getTime() - 1000;
      for (let arg of Object.keys(self.runningArgs)) {
        if (self.runningArgs[arg]["$$TIME"] + self.runningArgs[arg]["x-expires"] < timeNow) {
          features.log.info(`CLEAN ARGS: ${arg}`);
          delete self.runningArgs[arg];
        }
      }
    }, 60000);

    return new Promise(async (resolve, reject) => {
      try {
        features.log.info(`Connect to ${features.getPluginConfig().endpoint}`);
        self.rabbitQConnection = await amqplib.connect(features.getPluginConfig().endpoint, {
          credentials: amqplib.credentials.plain(features.getPluginConfig().credentials.username, features.getPluginConfig().credentials.password)
        });
        features.log.info(`Connected to ${features.getPluginConfig().endpoint}`);

        features.log.info(`Open emit channel`);
        self.rabbitQConnectionEmitChannel = await self.rabbitQConnection.createChannel();
        self.rabbitQConnectionEmitChannel.assertExchange(self.defaultExchange.name, self.defaultExchange.type, {
          durable: false
        });
        self.rabbitQConnectionEmitChannel.prefetch(1);
        features.log.info(`Open emit channel - COMPLETED`);

        features.log.info(`Open event channel`);
        self.rabbitQConnectionEventChannel = await self.rabbitQConnection.createChannel();
        self.rabbitQConnectionEventChannel.assertExchange(self.defaultExchange.name, self.defaultExchange.type, {
          durable: false
        });
        self.rabbitQConnectionEventChannel.prefetch(1);
        features.log.info(`Open event channel - COMPLETED`);

        features.log.info(`Open args channel`);
        self.rabbitQConnectionARGSChannel = await self.rabbitQConnection.createChannel();
        self.rabbitQConnectionARGSChannel.assertExchange(self.coreExchange.name, self.coreExchange.type, {
          durable: false
        });
        self.rabbitQConnectionARGSChannel.prefetch(1);
        features.log.info(`Open args channel - COMPLETED`);

        features.log.info(`Open emitAR channel`);
        self.rabbitQConnectionEmitARChannel = await self.rabbitQConnection.createChannel();
        self.rabbitQConnectionEmitARChannel.assertExchange(self.defaultExchange.name, self.defaultExchange.type, {
          durable: false
        });
        self.rabbitQConnectionEmitARChannel.prefetch(1);
        features.log.info(`Open emitAR channel - COMPLETED`);

        self._onEvent<any>(self.features.pluginName, 'core', 'args', (data) => {
          features.log.info(`NEW ARGS STORED: ${data.data.name}`);
          self.runningArgs[`result-${data.data.name}`] = data.data.data;
          self.runningArgs[`error-${data.data.name}`] = data.data.data;
        }, self.rabbitQConnectionARGSChannel);

        resolve();
      } catch (exce) {
        features.log.error(exce);
        reject(exce);
      }
      /*amqplib.connect(features.getPluginConfig().endpoint, {
        credentials: amqplib.credentials.plain(features.getPluginConfig().credentials.username, features.getPluginConfig().credentials.password)
      }).then((connection: any) => {
        self.rabbitQConnection = connection;
        features.log.info(`Connected to ${features.getPluginConfig().endpoint}`);

        features.log.info(`Open emit channel`);
        (connection || self.rabbitQConnection).createChannel().then(function (ch1: any) {
          self.rabbitQConnectionEmitChannel = ch1;
          features.log.info(`Open emit channel - COMPLETED`);
        }).catch(reject);
        amqplib.connect(features.getPluginConfig().endpoint, {
          credentials: amqplib.credentials.plain(features.getPluginConfig().credentials.username, features.getPluginConfig().credentials.password)
        }).then((connection: any) => {
          self.rabbitQConnectionCore = connection;
          features.log.info(`Connected to ${features.getPluginConfig().endpoint} (2/2)`);
          self._onEvent<any>('core', 'core', 'args', (data) => {
            features.log.info(`NEW ARGS STORED: ${data.data.name}`);
            self.runningArgs[`result-${data.data.name}`] = data.data.data;
            self.runningArgs[`error-${data.data.name}`] = data.data.data;
          }, self.coreExchange, self.rabbitQConnection);
          resolve();
        }).catch(reject);
        resolve();
      }).catch(reject);*/
    });
  }
  private _onEvent<T = any> (plugin: string, pluginName: string | null, event: string, listener: (data: IEmitter<T>) => void, channel: any): void {
    const self = this;
    self.features.log.info(plugin, ` - LISTEN: [${`${pluginName || plugin}-${event}`}]`);
    let qArguments: any = {
      durable: false
    };
    if (!Tools.isNullOrUndefined(self.runningArgs[`${pluginName || plugin}-${event}`])) {
      qArguments = self.runningArgs[`${pluginName || plugin}-${event}`];
      qArguments.arguments = self.runningArgs[`${pluginName || plugin}-${event}`];
    }

    let ok = channel.assertQueue(`${pluginName || plugin}-${event}`, qArguments);
    ok = ok.then(function () {
      self.features.log.info(plugin, ` - LISTEN: [${`${pluginName || plugin}-${event}`}] - LISTENING`);
    });
    //ok = ok.then(function () { channel.prefetch(1); });
    ok = ok.then(function () {
      channel.consume(`${pluginName || plugin}-${event}`, (msg: any) => {
        let body = msg.content.toString();
        channel.ack(msg);
        const bodyObj = JSON.parse(body) as any;
        listener(Tools.isNullOrUndefined(bodyObj.resultKey) ? {
          resultKey: null,
          resultNames: {
            plugin: plugin,
            success: null,
            error: null
          },
          data: bodyObj as T
        } as unknown as IEmitter<T> :
          bodyObj as IEmitter<T>);
      }, { noAck: false });
    });
  }
  onEvent<T = any> (plugin: string, pluginName: string | null, event: string, listener: (data: IEmitter<T>) => void): void {
    this._onEvent(plugin, pluginName, event, listener, this.rabbitQConnectionEventChannel);
  }
  private _emitEvent<T = any> (plugin: string, pluginName: string | null, event: string, channel: any, data?: T): void {
    const self = this;
    self.features.log.debug(plugin, ` - EMIT: [${`${pluginName || plugin}-${event}`}]`, data);
    let qArguments: any = {
      durable: false
    };
    if (!Tools.isNullOrUndefined(self.runningArgs[`${pluginName || plugin}-${event}`])) {
      qArguments = self.runningArgs[`${pluginName || plugin}-${event}`];
      qArguments.arguments = JSON.parse(JSON.stringify(self.runningArgs[`${pluginName || plugin}-${event}`]));
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
  emitEventAndReturn<T1 = any, T2 = any> (plugin: string, pluginName: string | null, event: string, data?: T1): Promise<void | T2> {
    const self = this;
    self.features.log.debug(plugin, ` - EMIT AR: [${`${pluginName || plugin}-${event}`}]`, data);
    return new Promise((resolve, reject) => {
      const resultKey = `${UUID()}-${new Date().getTime()}${Math.random()}`;
      const timeoutSeconds = ((data || {}) as any).timeoutSeconds || 10;
      const args = {
        durable: false,
        autoDelete: true,
        "x-expires": (timeoutSeconds * 1000) + 5000,
        "x-message-ttl": timeoutSeconds * 1000,
        "$$TIME": new Date().getTime()
      };
      self.runningArgs[`result-${resultKey}`] = args;
      self.runningArgs[`error-${resultKey}`] = args;
      self._emitEvent(self.features.pluginName, 'core', 'args', self.rabbitQConnectionARGSChannel, {
        name: resultKey,
        data: args
      });

      const actArgs = {
        arguments: {
          ...args
        },
        ...args
      };
      var ok = self.rabbitQConnectionEmitARChannel.assertQueue(`result-${resultKey}`, actArgs);
      var err = self.rabbitQConnectionEmitARChannel.assertQueue(`error-${resultKey}`, actArgs);

      let endTimeout = (callFunc: Function, param: any) => {
        clearTimeout(timeoutTimer);

        setTimeout(async () => {
          const deleteArgs = {
            ifUnused: false,
            ifEmpty: false,
          };
          await self.rabbitQConnectionEmitARChannel.deleteQueue(`result-${resultKey}`, deleteArgs);
          await self.rabbitQConnectionEmitARChannel.deleteQueue(`error-${resultKey}`, deleteArgs);
        }, 1000);
        callFunc(param);
      };
      let timeoutTimer = setTimeout(() => {
        if (timeoutTimer === null)
          return;
        self.features.log.debug(plugin, ` - EMIT AR: [${`${pluginName || plugin}-${event}`}-${resultKey}]`, 'TIMED OUT');
        endTimeout(reject, `NO RESPONSE IN TIME: ${pluginName || plugin}/${resultKey} x${((data || {}) as any).timeoutSeconds || 10}s`);
      }, timeoutSeconds * 1000);

      ok = ok.then(function () {
        self.features.log.info(plugin, ` - LISTEN AR: [${`${pluginName || plugin}-${event}`}-${resultKey}] - LISTENING`);
      });
      //ok = ok.then(function () { self.rabbitQConnectionEmitARChannel.prefetch(1); });
      ok = ok.then(function () {
        self.rabbitQConnectionEmitARChannel.consume(`result-${resultKey}`, (msg: any) => {
          if (Tools.isNullOrUndefined(msg)) return;
          self.rabbitQConnectionEmitARChannel.ack(msg);
          let body = msg.content.toString();
          self.features.log.debug(`Received msg on Q error-${resultKey}`, body);
          endTimeout(resolve, JSON.parse(body) as T2);
        }, { noAck: false });
      });

      err = err.then(function () {
        self.features.log.info(plugin, ` - LISTEN AR: [${`${pluginName || plugin}-${event}`}-${resultKey}] - LISTENING`);
      });
      //err = err.then(function () { self.rabbitQConnectionEmitARChannel.prefetch(1); });
      err = err.then(function () {
        self.rabbitQConnectionEmitARChannel.consume(`error-${resultKey}`, (msg: any) => {
          if (Tools.isNullOrUndefined(msg)) return;
          self.rabbitQConnectionEmitARChannel.ack(msg);
          let body = msg.content.toString();
          self.features.log.debug(`Received msg on Q error-${resultKey}`, body);
          endTimeout(reject, JSON.parse(body) as T2);
        }, { noAck: false });
      });

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
      self.features.log.debug(plugin, ` - EMIT AR: [${`${pluginName || plugin}-${event}`}-${resultKey}] - EMITTED`, data);
    });
  }
}