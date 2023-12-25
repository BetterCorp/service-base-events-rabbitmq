import { Plugin } from "../plugin";
import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { LIB, SetupChannel } from "./lib";
import {
  IPluginLogger,
  SmartFunctionCallAsync,
} from "@bettercorp/service-base";

export class emit {
  private plugin: Plugin;
  private log: IPluginLogger;
  private publishQueuesSetup: Array<string> = [];
  private publishChannel!: SetupChannel<null>;
  private receiveChannel!: SetupChannel<null>;
  private readonly channelKey = "91eq";
  private readonly queueOpts: amqplib.Options.AssertQueue = {
    durable: false,
    autoDelete: true,
    messageTtl: 60 * 60 * 1000, // 60 min
    expires: 60 * 60 * 1000, // 60 min
  };
  constructor(plugin: Plugin, log: IPluginLogger) {
    this.plugin = plugin;
    this.log = log;
  }
  async init() {
    this.log.debug(`Open broadcast channel ({channelKey})`, {
      channelKey: this.channelKey,
    });
    this.publishChannel = await LIB.setupChannel(
      this.plugin,
      this.log,
      this.plugin.publishConnection,
      this.channelKey,
      null
    );
    this.receiveChannel = await LIB.setupChannel(
      this.plugin,
      this.log,
      this.plugin.receiveConnection,
      this.channelKey,
      null,
      undefined,
      undefined,
      5
    );
  }
  public dispose() {
    this.publishChannel.channel.close();
    this.receiveChannel.channel.close();
  }

  async onEvent(
    pluginName: string,
    event: string,
    listener: { (args: Array<any>): Promise<void> }
  ): Promise<void> {
    const thisQueueKey = LIB.getQueueKey(
      this.plugin,
      this.channelKey,
      pluginName,
      event
    );
    this.log.debug(`LISTEN: [{thisQueueKey}]`, {
      thisQueueKey,
    });

    this.log.debug(` listen: [{thisQueueKey}]`, { thisQueueKey });

    const self = this;
    await this.receiveChannel.channel.addSetup(
      async (iChannel: amqplibCore.ConfirmChannel) => {
        await iChannel.assertQueue(thisQueueKey, self.queueOpts);
        await self.receiveChannel.channel.consume(
          thisQueueKey,
          async (msg: amqplibCore.ConsumeMessage) => {
            let start = new Date().getTime();
            let body = msg.content.toString();
            const bodyObj = JSON.parse(body) as Array<any>;
            try {
              await SmartFunctionCallAsync(self.plugin, listener, bodyObj);
              self.receiveChannel.channel.ack(msg);
              let end = new Date().getTime();
              let time = end - start;
              self.log.reportStat(
                `eventsrec-${self.channelKey}-${pluginName}-${event}-ok`,
                time
              );
            } catch (err: any) {
              self.receiveChannel.channel.nack(msg, true);
              let end = new Date().getTime();
              let time = end - start;
              self.log.reportStat(
                `eventsrec-${self.channelKey}-${pluginName}-${event}-error`,
                time
              );
              self.log.error(err.toString(), {});
            }
          },
          { noAck: false }
        );

        self.log.debug(`listen rabbit: [{thisQueueKey}]`, {
          thisQueueKey: thisQueueKey,
        });
      }
    );
  }

  async emitEvent(
    pluginName: string,
    event: string,
    args: Array<any>
  ): Promise<void> {
    const thisQueueKey = LIB.getQueueKey(
      this.plugin,
      this.channelKey,
      pluginName,
      event
    );
    this.log.debug(`Emit: [{thisQueueKey}]`, {
      thisQueueKey,
    });
    if (this.publishQueuesSetup.indexOf(thisQueueKey) < 0) {
      const self = this;
      self.publishQueuesSetup.push(thisQueueKey);
      await this.publishChannel.channel.addSetup(
        async (iChannel: amqplibCore.ConfirmChannel) => {
          await iChannel.assertQueue(thisQueueKey, self.queueOpts);
          self.log.debug(`emit rabbit: [{thisQueueKey}]`, { thisQueueKey });
        }
      );
    }
    if (
      !this.publishChannel.channel.sendToQueue(thisQueueKey, args, {
        expiration: this.queueOpts.messageTtl,
        contentType: "string",
        appId: this.plugin.myId,
        timestamp: new Date().getTime(),
      })
    )
      throw `Cannot send msg to queue [${thisQueueKey}]`;
    this.log.debug(` - EMIT: [${thisQueueKey}] - EMITTED`);
  }
}
