import { Events } from "../plugin";
import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { LIB } from "./lib";

export class emit {
  private uSelf!: Events;
  private privateQueuesSetup: Array<string> = [];
  private publishChannel!: amqplib.ChannelWrapper;
  private receiveChannel!: amqplib.ChannelWrapper;
  private readonly channelKey = "8eq";
  private readonly exchange = {
    type: "fanout",
    name: "better-service8-emit",
  };
  private readonly exchangeOpts: amqplib.Options.AssertExchange = {
    durable: true,
    //exclusive: true,
    autoDelete: true,
  };
  private readonly queueOpts: amqplib.Options.AssertQueue = {
    durable: true,
    autoDelete: true,
    messageTtl: 60 * 60 * 1000, // 1h
    expires: 60 * 60 * 1000, // 1h
  };
  async init(uSelf: Events) {
    this.uSelf = uSelf;
    this.uSelf.log.debug(`Open emit channel ({exchangeName})`, {
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
    this.publishChannel.close();
    this.receiveChannel.close();
  }

  async onEvent(
    callerPluginName: string,
    pluginName: string,
    event: string,
    listener: { (args: Array<any>): Promise<void> }
  ): Promise<void> {
    const self = this;
    const thisQueueKey = LIB.getQueueKey(
      this.channelKey,
      callerPluginName,
      pluginName,
      event
    );
    self.uSelf.log.debug(`{callerPluginName} - LISTEN: [{thisQueueKey}]`, {
      callerPluginName,
      thisQueueKey,
    });

    await self.receiveChannel.addSetup(
      async (iChannel: amqplibCore.ConfirmChannel) => {
        await iChannel.assertQueue(thisQueueKey, self.queueOpts);
        self.uSelf.log.debug(
          `{callerPluginName} - LISTEN: [{thisQueueKey}] - LISTENING`,
          { callerPluginName, thisQueueKey }
        );
        await self.receiveChannel.consume(
          thisQueueKey,
          async (msg: any) => {
            let body = msg.content.toString();
            const bodyObj = JSON.parse(body) as Array<any>;
            try {
              await listener(bodyObj);
              self.receiveChannel.ack(msg);
            } catch (err) {
              self.receiveChannel.nack(msg, true);
            }
          },
          { noAck: false }
        );
      }
    );
  }
  async emitEvent(
    callerPluginName: string,
    pluginName: string,
    event: string,
    args: Array<any>
  ): Promise<void> {
    const self = this;
    const thisQueueKey = LIB.getQueueKey(
      this.channelKey,
      callerPluginName,
      pluginName,
      event
    );
    this.uSelf.log.debug(`{callerPluginName} - EMIT: [{thisQueueKey}]`, {
      callerPluginName,
      thisQueueKey,
    });
    if (self.privateQueuesSetup.indexOf(thisQueueKey) < 0) {
      self.privateQueuesSetup.push(thisQueueKey);
      await self.publishChannel.addSetup(
        async (iChannel: amqplibCore.ConfirmChannel) => {
          await iChannel.assertQueue(thisQueueKey, self.queueOpts);
        }
      );
    }
    if (
      !this.publishChannel.sendToQueue(
        thisQueueKey,
        args, //Buffer.from(JSON.stringify(args)),
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
