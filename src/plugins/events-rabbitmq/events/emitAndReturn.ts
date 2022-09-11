import { Events } from "../plugin";
import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { EventEmitter } from "events";
import { randomUUID } from "crypto";
import { LIB } from "./lib";

export class emitAndReturn extends EventEmitter {
  private uSelf!: Events;
  private privateQueuesSetup: Array<string> = [];
  private publishChannel!: amqplib.ChannelWrapper;
  private receiveChannel!: amqplib.ChannelWrapper;
  private readonly channelKey = "8ar";
  private readonly myChannelKey = "8kr";
  private readonly exchange: any = {
    type: "direct",
    name: "better-service8-ear",
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
    expires: 60000, // 60s
  };
  private readonly myQueueOpts: amqplib.Options.AssertQueue = {
    exclusive: true,
    durable: false,
    autoDelete: true,
    messageTtl: 60 * 1000, // 60 seconds
    expires: 60000, // 60s
  };

  async init(uSelf: Events) {
    this.uSelf = uSelf;
    const self = this;
    const myEARQueueKey = LIB.getSpecialQueueKey(
      this.myChannelKey,
      this.uSelf.myId
    );
    this.uSelf.log.debug(`Ready my events name: {myEARQueueKey}`, {
      myEARQueueKey,
    });

    this.publishChannel = await LIB.setupChannel(
      uSelf,
      uSelf.publishConnection,
      this.myChannelKey,
      this.exchange.name,
      this.exchange.type,
      this.exchangeOpts
    );
    this.receiveChannel = await LIB.setupChannel(
      uSelf,
      uSelf.receiveConnection,
      this.myChannelKey,
      this.exchange.name,
      this.exchange.type,
      this.exchangeOpts,
      2
    );
    await this.receiveChannel.addSetup(
      async (iChannel: amqplibCore.ConfirmChannel) => {
        await iChannel.assertQueue(myEARQueueKey, self.myQueueOpts);
        self.uSelf.log.debug(`LISTEN: [{myEARQueueKey}]`, { myEARQueueKey });
        await iChannel.consume(
          myEARQueueKey,
          (msg: amqplibCore.ConsumeMessage | null): any => {
            if (msg === null)
              return self.uSelf.log.warn(
                `[RECEVIED {myEARQueueKey}]... as null`,
                { myEARQueueKey }
              );
            try {
              let body = msg.content.toString();
              self.uSelf.log.debug(`[RECEVIED {myEARQueueKey}]`, {
                myEARQueueKey,
              });
              self.emit(msg.properties.correlationId, JSON.parse(body));
              iChannel.ack(msg);
            } catch (exc: any) {
              self.uSelf.log.fatal("AMPQ Consumed exception: {eMsg}", {
                eMsg: exc.message || exc.toString(),
              });
            }
          },
          { noAck: false }
        );
        self.uSelf.log.debug(`LISTEN: [{myEARQueueKey}]`, { myEARQueueKey });
        self.uSelf.log.debug(`Ready my events name: {myEARQueueKey} OKAY`, {
          myEARQueueKey,
        });
      }
    );
  }
  public dispose() {
    this.publishChannel.close();
    this.receiveChannel.close();
  }

  async onReturnableEvent(
    callerPluginName: string,
    pluginName: string,
    event: string,
    listener: { (args: Array<any>): Promise<any> }
  ): Promise<void> {
    const self = this;
    const queueKey = LIB.getQueueKey(
      this.channelKey,
      callerPluginName,
      pluginName,
      event
    );
    self.uSelf.log.debug(` EAR: {callerPluginName} listen {queueKey}`, {
      callerPluginName,
      queueKey,
    });

    await self.receiveChannel.addSetup(
      async (iChannel: amqplibCore.ConfirmChannel) => {
        await iChannel.assertQueue(queueKey, self.queueOpts);
        await iChannel.consume(
          queueKey,
          async (msg: amqplibCore.ConsumeMessage | null): Promise<any> => {
            if (msg === null)
              return self.uSelf.log.error(
                "Message received on my EAR queue was null..."
              );
            const returnQueue = LIB.getSpecialQueueKey(
              this.myChannelKey,
              msg.properties.appId
            );
            self.uSelf.log.debug(
              `EAR: {callerPluginName} Received: {queueKey} from {returnQueue}`,
              { callerPluginName, queueKey, returnQueue }
            );
            let body = msg.content.toString();
            const bodyObj = JSON.parse(body) as Array<any>;
            try {
              const response = (await listener(bodyObj));
              iChannel.ack(msg);
              self.uSelf.log.debug(
                `EAR: {callerPluginName} OKAY: {queueKey} -> {returnQueue}`,
                { callerPluginName, queueKey, returnQueue }
              );
              if (
                !self.publishChannel.sendToQueue(
                  returnQueue,
                  response,
                  {
                    expiration: 5000,
                    correlationId: `${msg.properties.correlationId}-resolve`,
                    contentType: "string",
                    appId: self.uSelf.myId,
                    timestamp: new Date().getTime(),
                  }
                )
              )
                throw `Cannot send msg to queue [${returnQueue}]`;
            } catch (exc) {
              iChannel.ack(msg);
              self.uSelf.log.error(
                `EAR: {callerPluginName} ERROR: {queueKey} -> {returnQueue}`,
                { callerPluginName, queueKey, returnQueue }
              );
              if (
                !self.publishChannel.sendToQueue(
                  returnQueue,
                  exc,
                  {
                    expiration: 5000,
                    correlationId: `${msg.properties.correlationId}-reject`,
                    contentType: "string",
                    appId: self.uSelf.myId,
                    timestamp: new Date().getTime(),
                  }
                )
              )
                throw `Cannot send msg to queue [${returnQueue}]`;
            }
          },
          { noAck: false }
        );
        self.uSelf.log.debug(`EAR: {callerPluginName} listening {queueKey}`, {
          callerPluginName,
          queueKey,
        });
      }
    );
  }

  async emitEventAndReturn(
    callerPluginName: string,
    pluginName: string,
    event: string,
    timeoutSeconds: number,
    args: Array<any>
  ): Promise<any> {
    const self = this;
    const resultKey = `${randomUUID()}-${new Date().getTime()}${Math.random()}`;
    const queueKey = LIB.getQueueKey(
      this.channelKey,
      callerPluginName,
      pluginName,
      event
    );
    this.uSelf.log.debug(
      `EAR: {callerPluginName} emitting {queueKey} ({resultKey})`,
      { callerPluginName, queueKey, resultKey }
    );
    if (self.privateQueuesSetup.indexOf(queueKey) < 0) {
      self.privateQueuesSetup.push(queueKey);
      await self.publishChannel.addSetup(
        async (iChannel: amqplibCore.ConfirmChannel) => {
          await iChannel.assertQueue(queueKey, self.queueOpts);
        }
      );
    }
    return await new Promise(async (resolve, reject) => {
      let timeoutHandler = setTimeout(() => {
        self.removeAllListeners(`${resultKey}-resolve`);
        self.removeAllListeners(`${resultKey}-reject`);
        reject("Timeout");
      }, timeoutSeconds * 1000);
      self.once(`${resultKey}-resolve`, (rargs: string) => {
        clearTimeout(timeoutHandler);
        resolve(rargs);
      });
      self.once(`${resultKey}-reject`, (rargs: any) => {
        clearTimeout(timeoutHandler);
        reject(rargs);
      });
      if (
        !self.publishChannel.sendToQueue(
          queueKey,
          args, 
          {
            expiration: timeoutSeconds * 1000 + 5000,
            correlationId: resultKey,
            contentType: "string",
            appId: self.uSelf.myId,
            timestamp: new Date().getTime(),
          }
        )
      )
        throw `Cannot send msg to queue [${queueKey}]`;
      self.uSelf.log.debug(
        `EAR: {callerPluginName} emitted {queueKey} ({resultKey})`,
        { callerPluginName, queueKey, resultKey }
      );
    });
  }
}
