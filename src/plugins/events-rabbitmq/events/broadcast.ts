import { Events } from "../plugin";
import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { LIB, SetupChannel } from "./lib";
import { randomUUID } from "crypto";

export class broadcast {
  private uSelf!: Events;
  private publishQueuesSetup: Array<string> = [];
  private publishChannel!: SetupChannel<string>;
  private receiveChannel!: SetupChannel<string>;
  private readonly channelKey = "81eb";
  private readonly exchange = {
    type: "direct",
    name: "better.service8.broadcast",
  };
  private readonly exchangeOpts: amqplib.Options.AssertExchange = {
    durable: false,
    //exclusive: true,
    autoDelete: true,
  };
  private readonly queueOpts: amqplib.Options.AssertQueue = {
    durable: false,
    autoDelete: true,
    messageTtl: 60 * 1000, // 60 seconds
    expires: 60 * 1000, // 60s
  };
  async init(uSelf: Events) {
    this.uSelf = uSelf;
    this.uSelf.log.debug(`Open broadcast channel ({exchangeName})`, {
      exchangeName: this.exchange.name,
    });
    this.publishChannel = await LIB.setupChannel(
      uSelf,
      uSelf.publishConnection,
      this.channelKey,
      this.exchange.name,
      this.exchange.type,
      this.exchangeOpts
    );
    this.receiveChannel = await LIB.setupChannel(
      uSelf,
      uSelf.receiveConnection,
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
    callerPluginName: string,
    pluginName: string,
    event: string,
    listener: { (args: Array<any>): Promise<void> }
  ): Promise<void> {
    const thisUUID = randomUUID();
    const self = this;
    const rawQueueKey = await LIB.getQueueKey(
      self.uSelf,
      this.channelKey,
      callerPluginName,
      pluginName,
      event
    );
    const thisQueueKey = await LIB.getQueueKey(
      self.uSelf,
      this.channelKey,
      callerPluginName,
      pluginName,
      event,
      thisUUID
    );
    self.uSelf.log.debug(`{callerPluginName} - LISTEN: [{thisQueueKey}]`, {
      callerPluginName,
      thisQueueKey: rawQueueKey,
    });

    await this.uSelf.log.debug(
      `{callerPluginName} - listen: [{thisQueueKey}]`,
      { callerPluginName, thisQueueKey }
    );

    await self.receiveChannel.channel.addSetup(
      async (iChannel: amqplibCore.ConfirmChannel) => {
        await iChannel.assertQueue(thisQueueKey, self.queueOpts);
        await self.receiveChannel.channel.consume(
          thisQueueKey,
          async (msg: amqplibCore.ConsumeMessage) => {
            let start = new Date().getTime();
            let body = msg.content.toString();
            const bodyObj = JSON.parse(body) as Array<any>;
            try {
              await listener(bodyObj);
              self.receiveChannel.channel.ack(msg);
              let end = new Date().getTime();
              let time = end - start;
              await self.uSelf.log.reportStat(
                `eventsrec-${self.channelKey}-${pluginName || callerPluginName}-${event}-ok`,
                time
              );
            } catch (err: any) {
              self.receiveChannel.channel.nack(msg, true);
              let end = new Date().getTime();
              let time = end - start;
              await self.uSelf.log.reportStat(
                `eventsrec-${self.channelKey}-${pluginName || callerPluginName}-${event}-error`,
                time
              );
              await self.uSelf.log.error(err);
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
        await self.uSelf.log.debug(
          `{callerPluginName} - listen rabbit: [{thisQueueKey}]`,
          { callerPluginName, thisQueueKey: rawQueueKey }
        );
      }
    );
  }

  async emitBroadcast(
    callerPluginName: string,
    pluginName: string,
    event: string,
    args: Array<any>
  ): Promise<void> {
    const self = this;
    const thisQueueKey = await LIB.getQueueKey(
      self.uSelf,
      this.channelKey,
      callerPluginName,
      pluginName,
      event
    );
    this.uSelf.log.debug(`{callerPluginName} - Emit: [{thisQueueKey}]`, {
      callerPluginName,
      thisQueueKey,
    });
    if (self.publishQueuesSetup.indexOf(thisQueueKey) < 0) {
      self.publishQueuesSetup.push(thisQueueKey);
      await self.publishChannel.channel.addSetup(
        async (iChannel: amqplibCore.ConfirmChannel) => {
          await iChannel.assertQueue(thisQueueKey, self.queueOpts);
          await self.uSelf.log.debug(
            `{callerPluginName} - emit rabbit: [{thisQueueKey}]`,
            { callerPluginName, thisQueueKey }
          );
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
          appId: this.uSelf.myId,
          timestamp: new Date().getTime(),
        }
      )
    )
      throw `Cannot send msg to queue [${thisQueueKey}]`;
    this.uSelf.log.debug(
      callerPluginName,
      ` - EMIT: [${thisQueueKey}] - EMITTED`
    );
  }
}
