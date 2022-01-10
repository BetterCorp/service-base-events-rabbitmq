import * as amqplib from 'amqplib';
import { CEvents } from '@bettercorp/service-base/lib/interfaces/events';
import { Tools } from '@bettercorp/tools/lib/Tools';
import { IPluginConfig } from './sec.config';
import { emit } from './events/emit';
import { ear } from './events/ear';
import { randomUUID } from 'crypto';
import { hostname } from 'os';

export class Events extends CEvents {
  rabbitQConnection!: amqplib.Connection;
  myId!: string;
  private ear: ear = new ear();
  private emit: emit = new emit();

  async init(): Promise<void> {
    await this._connectToAMQP();
  }
  private async _connectToAMQP() {
    this.log.info(`Connect to ${ (await this.getPluginConfig<IPluginConfig>()).endpoint }`);
    if (Tools.isNullOrUndefined((await this.getPluginConfig<IPluginConfig>()).credentials)) {
      throw new Error('Plugin credentials not defined in sec.config.json');
    }
    this.rabbitQConnection = await amqplib.connect((await this.getPluginConfig<IPluginConfig>()).endpoint, {
      credentials: amqplib.credentials.plain((await this.getPluginConfig<IPluginConfig>()).credentials.username, (await this.getPluginConfig<IPluginConfig>()).credentials.password)
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
    this.log.info(`Connected to ${ (await this.getPluginConfig<IPluginConfig>()).endpoint }`);

    this.myId = `${ (await this.getPluginConfig<IPluginConfig>()).uniqueId || hostname() }-${ randomUUID() }`;
    await this.emit.init(this);
    await this.ear.init(this);
  }

  async onEvent<T = any>(callerPluginName: string, pluginName: string | null, event: string, listener: (data: T) => void): Promise<void> {
    return await this.emit.onEvent(callerPluginName, pluginName, event, listener);
  }
  async emitEvent<T = any>(callerPluginName: string, pluginName: string | null, event: string, data?: T): Promise<void> {
    return await this.emit.emitEvent(callerPluginName, pluginName, event, data);
  }

  async onReturnableEvent<ArgsDataType = any, ResolveDataType = any, RejectDataType = any>(callerPluginName: string, pluginName: string, event: string, listener: { (resolve: { (...args: ResolveDataType[]): void; }, reject: { (...args: RejectDataType[]): void; }, data: ArgsDataType): void; }): Promise<void> {
    return await this.ear.onReturnableEvent(callerPluginName, pluginName, event, listener);
  }
  async emitEventAndReturn<ArgsDataType = any, ReturnDataType = any>(callerPluginName: string, pluginName: string, event: string, data?: ArgsDataType, timeoutSeconds?: number): Promise<ReturnDataType> {
    return this.ear.emitEventAndReturn(callerPluginName, pluginName, event, data, timeoutSeconds);
  }
}
