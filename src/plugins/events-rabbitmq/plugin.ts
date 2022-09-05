import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { Tools } from "@bettercorp/tools/lib/Tools";
import { PluginConfig } from "./sec.config";
import { emit } from "./events/emit";
import { emitAndReturn } from "./events/emitAndReturn";
import { emitStreamAndReceiveStream } from "./events/emitStreamAndReceiveStream";
import { randomUUID } from "crypto";
import { hostname } from "os";
import { Readable } from "stream";
import { EventsBase } from "@bettercorp/service-base";

export class Events extends EventsBase<PluginConfig> {
  public publishConnection!: amqplib.AmqpConnectionManager;
  public receiveConnection!: amqplib.AmqpConnectionManager;
  public myId!: string;
  private ear: emitAndReturn = new emitAndReturn();
  private emit: emit = new emit();
  private eas: emitStreamAndReceiveStream = new emitStreamAndReceiveStream();

  async init(): Promise<void> {
    await this._connectToAMQP();
  }

  private async _connectToAMQP() {
    const pluginConfig = await this.getPluginConfig();
    this.log.info(`Connect to {endpoints}`, {
      endpoints: pluginConfig.endpoints,
    });
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
    this.publishConnection = amqplib.connect(
      pluginConfig.endpoints,
      socketOptions
    );
    this.receiveConnection = amqplib.connect(
      pluginConfig.endpoints,
      socketOptions
    );
    const self = this;
    this.publishConnection.on("connect", (data: any) => {
      self.log.info("AMQP CONNECTED: {url}", { url: data.url });
    });
    this.publishConnection.on("connectFailed", (data: any) => {
      self.log.error("AMQP CONNECT FAIL: {url} ({msg})", { url: data.url, msg: data.err.toString() });
    });
    this.publishConnection.on("error", (err: any) => {
      if (err.message !== "Connection closing") {
        self.log.error("AMQP ERROR: {message}", { message: err.message });
      }
    });
    this.receiveConnection.on("error", (err: any) => {
      if (err.message !== "Connection closing") {
        self.log.error("AMQP ERROR: {message}", { message: err.message });
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

    //await this.receiveConnection.connect();
    //await this.publishConnection.connect();

    this.log.info(`Connected to {endpoints}x2? (s:{sendS}/p:{pubS})`, {
      endpoints: (await this.getPluginConfig()).endpoints,
      sendS: this.receiveConnection.isConnected(),
      pubS: this.publishConnection.isConnected(),
    });


    this.myId = `${
      (await this.getPluginConfig()).uniqueId || hostname()
    }-${randomUUID()}`;
    await this.emit.init(this);
    await this.ear.init(this);
    await this.eas.init(this);
  }

  public dispose() {
    this.emit.dispose();
    this.ear.dispose();
    this.eas.dispose();
    this.publishConnection.close();
    this.receiveConnection.close();
  }

  public async onEvent(
    callerPluginName: string,
    pluginName: string,
    event: string,
    listener: { (args: Array<any>): Promise<void> }
  ): Promise<void> {
    await this.emit.onEvent(callerPluginName, pluginName, event, listener);
  }
  public async emitEvent(
    callerPluginName: string,
    pluginName: string,
    event: string,
    args: Array<any>
  ): Promise<void> {
    await this.emit.emitEvent(callerPluginName, pluginName, event, args);
  }

  public async onReturnableEvent(
    callerPluginName: string,
    pluginName: string,
    event: string,
    listener: { (args: Array<any>): Promise<any> }
  ): Promise<void> {
    await this.ear.onReturnableEvent(callerPluginName, pluginName, event, listener);
  }
  public async emitEventAndReturn(
    callerPluginName: string,
    pluginName: string,
    event: string,
    timeoutSeconds: number,
    args: Array<any>
  ): Promise<any> {
    return await this.ear.emitEventAndReturn(
      callerPluginName,
      pluginName,
      event,
      timeoutSeconds,
      args
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
