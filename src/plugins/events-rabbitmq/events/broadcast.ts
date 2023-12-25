import { Plugin } from "../plugin";
import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { LIB, SetupChannel } from "./lib";
import { randomUUID } from "crypto";
import { IPluginLogger, SmartFunctionCallAsync } from "@bettercorp/service-base";

export class broadcast {
  private plugin: Plugin;
  private log: IPluginLogger;
  private publishQueuesSetup: Array<string> = [];
  private publishChannel!: SetupChannel<string>;
  private receiveChannel!: SetupChannel<string>;
  private readonly channelKey = "91eb";
  private readonly exchange = {
    type: "direct",
    name: "better.service9.broadcast",
  };
  private readonly exchangeOpts: amqplib.Options.AssertExchange = {
    durable: false,
    //exclusive: true,
    autoDelete: true,
  };
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
    this.log.debug(`Open broadcast channel ({exchangeName})`, {
      exchangeName: this.exchange.name,
    });
    this.publishChannel = await LIB.setupChannel(
      this.plugin,
      this.log,
      this.plugin.publishConnection,
      this.channelKey,
      this.exchange.name,
      this.exchange.type,
      this.exchangeOpts
    );
    this.receiveChannel = await LIB.setupChannel(
      this.plugin,
      this.log,
      this.plugin.receiveConnection,
      this.channelKey,
      this.exchange.name,
      this.exchange.type,
      this.exchangeOpts,
      5
    );
  }
  public dispose() {
    this.publishChannel.channel.close();
    this.receiveChannel.channel.close();
  }

  async onBroadcast(
    pluginName: string,
    event: string,
    listener: { (args: Array<any>): Promise<void> }
  ): Promise<void> {
    const thisUUID = randomUUID();
    const rawQueueKey = LIB.getQueueKey(
      this.plugin,
      this.channelKey,
      pluginName,
      event
    );
    const thisQueueKey = LIB.getQueueKey(
      this.plugin,
      this.channelKey,
      pluginName,
      event,
      thisUUID
    );
    this.log.debug(`LISTEN: [{thisQueueKey}]`, {
      thisQueueKey: rawQueueKey,
    });

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
        await iChannel.bindQueue(
          thisQueueKey,
          self.receiveChannel.exchangeName,
          rawQueueKey
          /*{
            routing_key: rawQueueKey,
          }*/
        );
        self.log.debug(`listen rabbit: [{thisQueueKey}]`, {
          thisQueueKey: rawQueueKey,
        });
      }
    );
  }

  async emitBroadcast(
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
      this.publishQueuesSetup.push(thisQueueKey);
      await this.publishChannel.channel.addSetup(
        async (iChannel: amqplibCore.ConfirmChannel) => {
          await iChannel.assertQueue(thisQueueKey, self.queueOpts);
          self.log.debug(`emit rabbit: [{thisQueueKey}]`, { thisQueueKey });
        }
      );
    }
    if (
      !this.publishChannel.channel.publish(
        this.exchange.name,
        thisQueueKey,
        args,
        {
          expiration: this.queueOpts.messageTtl,
          contentType: "string",
          appId: this.plugin.myId,
          timestamp: new Date().getTime(),
        }
      )
    )
      throw `Cannot send msg to queue [${thisQueueKey}]`;
    this.log.debug(` - EMIT: [${thisQueueKey}] - EMITTED`);
  }
}
