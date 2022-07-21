import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { CEvents } from "@bettercorp/service-base/lib/interfaces/events";
import { Tools } from "@bettercorp/tools/lib/Tools";
import { IPluginConfig } from "./sec.config";
import { emit } from "./events/emit";
import { emitAndReturn } from "./events/emitAndReturn";
import { emitStreamAndReceiveStream } from "./events/emitStreamAndReceiveStream";
import { randomUUID } from "crypto";
import { hostname } from "os";
import { Readable } from "stream";

export class Events extends CEvents {
  publishConnection!: amqplib.AmqpConnectionManager;
  receiveConnection!: amqplib.AmqpConnectionManager;
  myId!: string;
  private ear: emitAndReturn = new emitAndReturn();
  private emit: emit = new emit();
  private eas: emitStreamAndReceiveStream = new emitStreamAndReceiveStream();

  async init(): Promise<void> {
    await this._connectToAMQP();
  }
  private async _connectToAMQP() {
    const pluginConfig = await this.getPluginConfig<IPluginConfig>();
    this.log.info(`Connect to ${pluginConfig.endpoint}`);
    let socketOptions: amqplib.AmqpConnectionManagerOptions = {
      connectionOptions: {},
    };
    if (!Tools.isNullOrUndefined(pluginConfig.credentials)) {
      socketOptions.connectionOptions!.credentials =
        amqplibCore.credentials.plain(
          pluginConfig.credentials.username,
          pluginConfig.credentials.password
        );
    }
    let endpointList =
      (Tools.isArray(pluginConfig.endpoints)
        ? pluginConfig.endpoints
        : [pluginConfig.endpoint]) || [];
    if (
      endpointList.length === 0 ||
      endpointList.filter((x) => !Tools.isString(x)).length > 0
    ) {
      return this.log.fatal(`Undefined endpoint(s) for rabiitMQ connection!`);
    }
    this.publishConnection = await amqplib.connect(endpointList, socketOptions);
    this.receiveConnection = await amqplib.connect(endpointList, socketOptions);
    const self = this;
    this.publishConnection.on("error", (err: any) => {
      if (err.message !== "Connection closing") {
        self.log.error("AMQP ERROR", err.message);
      }
    });
    this.receiveConnection.on("error", (err: any) => {
      if (err.message !== "Connection closing") {
        self.log.error("AMQP ERROR", err.message);
      }
    });
    this.publishConnection.on("close", () => {
      self.log.error("AMQP CONNECTION CLOSED");
      if (pluginConfig.fatalOnDisconnect)
        self.log.fatal("AMQP Error: Connection closed");
    });
    this.receiveConnection.on("close", () => {
      self.log.error("AMQP CONNECTION CLOSED");
      if (pluginConfig.fatalOnDisconnect)
        self.log.fatal("AMQP Error: Connection closed");
    });
    this.log.info(
      `Connected to ${(await this.getPluginConfig<IPluginConfig>()).endpoint}x2`
    );

    this.myId = `${
      (await this.getPluginConfig<IPluginConfig>()).uniqueId || hostname()
    }-${randomUUID()}`;
    await this.emit.init(this);
    await this.ear.init(this);
    await this.eas.init(this);
  }

  async onEvent<T = any>(
    callerPluginName: string,
    pluginName: string | null,
    event: string,
    listener: (data: T) => Promise<void>
  ): Promise<void> {
    return await this.emit.onEvent(
      callerPluginName,
      pluginName,
      event,
      listener
    );
  }
  async emitEvent<T = any>(
    callerPluginName: string,
    pluginName: string | null,
    event: string,
    data?: T
  ): Promise<void> {
    return await this.emit.emitEvent(callerPluginName, pluginName, event, data);
  }

  async onReturnableEvent<ArgsDataType = any, ReturnDataType = any>(
    callerPluginName: string,
    pluginName: string,
    event: string,
    listener: { (data: ArgsDataType): Promise<ReturnDataType> }
  ): Promise<void> {
    return await this.ear.onReturnableEvent(
      callerPluginName,
      pluginName,
      event,
      listener
    );
  }
  async emitEventAndReturn<ArgsDataType = any, ReturnDataType = any>(
    callerPluginName: string,
    pluginName: string,
    event: string,
    data?: ArgsDataType,
    timeoutSeconds?: number
  ): Promise<ReturnDataType> {
    return this.ear.emitEventAndReturn(
      callerPluginName,
      pluginName,
      event,
      data,
      timeoutSeconds
    );
  }
  async receiveStream(
    callerPluginName: string,
    listener: (error: Error | null, stream: Readable) => Promise<void>,
    timeoutSeconds: number
  ): Promise<string> {
    return this.eas.receiveStream(callerPluginName, listener, timeoutSeconds);
  }
  async sendStream(
    callerPluginName: string,
    streamId: string,
    stream: Readable
  ): Promise<void> {
    return this.eas.sendStream(callerPluginName, streamId, stream);
  }
}
