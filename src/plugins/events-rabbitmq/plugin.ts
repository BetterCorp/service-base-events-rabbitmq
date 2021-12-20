import * as amqplib from 'amqplib';
import { randomUUID } from 'crypto';
import { CEvents } from '@bettercorp/service-base/lib/ILib';
import { Tools } from '@bettercorp/tools/lib/Tools';
import * as OS from 'os';
import { IPluginConfig } from './sec.config';
import { Rabbit } from 'rabbit-queue';

export class Events extends CEvents {
  private appId!: string;
  private rabbitQConnection!: Rabbit;
  private readonly rabbitQConnectionEmitChannelKey = "eq";
  private readonly rabbitQConnectionEARChannelKey = "ar";

  init(): Promise<void> {
    const self = this;
    return new Promise(async (resolve, reject) => {
      try {
        self.appId = `${ OS.hostname() }-${ randomUUID() }`;
        await self._connectToAMQP();
        resolve();
      } catch (exce) {
        self.log.fatal(exce);
        reject(exce);
      }
    });
  }
  private async _connectToAMQP() {
    const pluginConfig = (await this.getPluginConfig<IPluginConfig>());
    this.log.info(`Connect to ${ pluginConfig.endpoint }`);
    let socketOptions: any = {};
    if (!Tools.isNullOrUndefined(pluginConfig.credentials)) {
      socketOptions.credentials = amqplib.credentials.plain('radmin', 'TLvGnHd9a9ndmo2nBepNxFXFprQ9eCpEvXp6qKN2YPBqUVN2va');
    }

    this.rabbitQConnection = new Rabbit(pluginConfig.endpoint, {
      prefetch: pluginConfig.prefetch,
      replyPattern: true,
      scheduledPublish: false,
      prefix: '',
      socketOptions
    });

    const self = this;
    this.rabbitQConnection.on('log', (component, level, ...args): void | any => {
      if (Tools.isFunction((self.log as any)[level]))
        return (self.log as any)[level](component, args);
      if ('trace' === level)
        return self.log.debug(component, args);
      self.log.info(component, args);
    });

    await new Promise((r: any) => {
      this.rabbitQConnection.on('connected', r);
    });
    this.log.info(`Connected to ${ pluginConfig.endpoint }`);

    this.rabbitQConnection.on('disconnected', (err = new Error('Rabbitmq Disconnected')) => {
      self.log.error(err);
      self.log.error('AMQP CONNECTION CLOSED');
      self.log.fatal('AMQP Error: Connection closed');
    });
  }

  async onEvent<ArgsDataType = any>(callerPluginName: string, pluginName: string, event: string, listener: (data: ArgsDataType) => void): Promise<void> {
    const self = this;
    self.log.info(callerPluginName, ` - LISTEN: [${ self.rabbitQConnectionEmitChannelKey }-${ pluginName || callerPluginName }-${ event }]`);

    this.rabbitQConnection.createQueue(`${ self.rabbitQConnectionEmitChannelKey }-${ pluginName || callerPluginName }-${ event }`, { durable: true }, (msg, ack) => {
      let body = msg.content.toString();
      const bodyObj = JSON.parse(body) as any;
      listener(bodyObj as ArgsDataType);
      ack(null);
    }).then(() => {
      self.log.info(callerPluginName, ` - LISTEN: [${ self.rabbitQConnectionEmitChannelKey }-${ pluginName || callerPluginName }-${ event }] - LISTENING`);
    });
  }
  async emitEvent<T = any>(plugin: string, pluginName: string | null, event: string, data?: T): Promise<void> {
    await this.rabbitQConnection.publish(`${ this.rabbitQConnectionEmitChannelKey }-${ pluginName || plugin }-${ event }`, data, {
      persistent: false,
      expiration: ((60 * 60 * 60 * 6) * 1000), // 6h
      appId: this.appId,
      correlationId: this.appId + randomUUID()
    })
      .then(this.log.debug)
      .catch(this.log.fatal);
  }

  async onReturnableEvent<ArgsDataType = any, ResolveDataType = any, RejectDataType = any>(callerPluginName: string, pluginName: string, event: string, listener: { (resolve: { (...args: ResolveDataType[]): void; }, reject: { (...args: RejectDataType[]): void; }, data: ArgsDataType): void; }): Promise<void> {
    this.log.info(callerPluginName, ` - LISTEN EAR: [${ this.rabbitQConnectionEARChannelKey }-${ pluginName || callerPluginName }-${ event }]`);

    const self = this;
    this.rabbitQConnection
      .createQueue(`${ self.rabbitQConnectionEARChannelKey }-${ pluginName || callerPluginName }-${ event }`, { durable: false }, (msg, ack) => {
        let body = msg.content.toString();
        let bodyObj: ArgsDataType = JSON.parse(body);
        listener((x: any) => {
          self.log.debug(callerPluginName, ` - RETURN OKAY: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || callerPluginName }-${ event }]`, bodyObj);
          ack(null, x);
        }, (x: any) => {
          self.log.debug(callerPluginName, ` - RETURN ERROR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || callerPluginName }-${ event }]`, bodyObj);
          ack(x);
        }, bodyObj);
      })
      .then(() => {
        self.log.info(callerPluginName, ` - LISTEN EAR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || callerPluginName }-${ event }] - LISTENING`);
      });
  }
  emitEventAndReturn<T1 = any, T2 = void>(plugin: string, pluginName: string | null, event: string, data?: T1, timeoutSeconds: number = 10): Promise<T2> {
    const self = this;
    this.log.debug(plugin, ` - EMIT EAR: [${ self.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }]`, data);
    const xtimeoutSeconds = timeoutSeconds || 10;
    return this.rabbitQConnection.getReply(`${ this.rabbitQConnectionEARChannelKey }-${ pluginName || plugin }-${ event }`, data, {
      persistent: false,
      expiration: ((xtimeoutSeconds * 1000) + 5000),
      appId: this.appId,
      correlationId: this.appId + randomUUID()
    }, '', xtimeoutSeconds * 1000);
  }
}