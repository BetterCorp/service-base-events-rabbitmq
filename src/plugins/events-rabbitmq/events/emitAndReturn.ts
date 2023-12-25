import { Plugin } from "../plugin";
import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { EventEmitter } from "events";
import { randomUUID } from "crypto";
import { LIB, SetupChannel } from "./lib";
import {
  IPluginLogger,
  SmartFunctionCallAsync,
} from "@bettercorp/service-base";

export class emitAndReturn extends EventEmitter {
  private plugin: Plugin;
  private log: IPluginLogger;
  private privateQueuesSetup: Array<string> = [];
  private publishChannel!: SetupChannel;
  private receiveChannel!: SetupChannel;
  private readonly channelKey = "91ar";
  private readonly myChannelKey = "91kr";
  private readonly queueOpts: amqplib.Options.AssertQueue = {
    durable: false,
    autoDelete: true,
    messageTtl: 60 * 1000, // 60 seconds
    expires: 60 * 1000, // 60s
  };
  private readonly myQueueOpts: amqplib.Options.AssertQueue = {
    exclusive: true,
    durable: false,
    autoDelete: true,
    messageTtl: 60 * 1000, // 60 seconds
    expires: 60 * 1000, // 60s
  };

  constructor(plugin: Plugin, log: IPluginLogger) {
    super();
    this.plugin = plugin;
    this.log = log;
  }
  async init() {
    const self = this;
    const myEARQueueKey = LIB.getMyQueueKey(
      this.plugin,
      this.myChannelKey,
      this.plugin.myId
    );
    this.log.debug(`Ready my events name: {myEARQueueKey}`, {
      myEARQueueKey,
    });

    this.publishChannel = await LIB.setupChannel(
      this.plugin,
      this.log,
      this.plugin.publishConnection,
      this.myChannelKey,
      null
    );
    this.receiveChannel = await LIB.setupChannel(
      this.plugin,
      this.log,
      this.plugin.receiveConnection,
      this.myChannelKey,
      null,
      undefined,
      undefined,
      2
    );
    await this.receiveChannel.channel.addSetup(
      async (iChannel: amqplibCore.ConfirmChannel) => {
        await iChannel.assertQueue(myEARQueueKey, self.myQueueOpts);
        self.log.debug(`LISTEN: [{myEARQueueKey}]`, { myEARQueueKey });
        await iChannel.consume(
          myEARQueueKey,
          (msg: amqplibCore.ConsumeMessage | null): any => {
            if (msg === null)
              return self.log.warn(`[RECEVIED {myEARQueueKey}]... as null`, {
                myEARQueueKey,
              });
            try {
              let body = msg.content.toString();
              self.log.debug(`[RECEVIED {myEARQueueKey}]`, {
                myEARQueueKey,
              });
              self.emit(msg.properties.correlationId, JSON.parse(body));
              iChannel.ack(msg);
            } catch (exc: any) {
              self.log.error("AMPQ Consumed exception: {eMsg}", {
                eMsg: exc.message || exc.toString(),
              });
              process.exit(7);
            }
          },
          { noAck: false }
        );
        self.log.debug(`LISTEN: [{myEARQueueKey}]`, { myEARQueueKey });
        self.log.debug(`Ready my events name: {myEARQueueKey} OKAY`, {
          myEARQueueKey,
        });
      }
    );
  }
  public dispose() {
    this.publishChannel.channel.close();
    this.receiveChannel.channel.close();
  }

  async onReturnableEvent(
    pluginName: string,
    event: string,
    listener: { (args: Array<any>): Promise<any> }
  ): Promise<void> {
    const queueKey = LIB.getQueueKey(
      this.plugin,
      this.channelKey,
      pluginName,
      event
    );
    this.log.debug(` EAR: listen {queueKey}`, {
      queueKey,
    });

    const self = this;
    await this.receiveChannel.channel.addSetup(
      async (iChannel: amqplibCore.ConfirmChannel) => {
        await iChannel.assertQueue(queueKey, self.queueOpts);
        await iChannel.consume(
          queueKey,
          async (msg: amqplibCore.ConsumeMessage | null): Promise<any> => {
            let start = new Date().getTime();
            if (msg === null)
              return self.log.error(
                "Message received on my EAR queue was null..."
              );
            const returnQueue = LIB.getMyQueueKey(
              self.plugin,
              this.myChannelKey,
              msg.properties.appId
            );
            self.log.debug(`EAR: Received: {queueKey} from {returnQueue}`, {
              queueKey,
              returnQueue,
            });
            let body = msg.content.toString();
            const bodyObj = JSON.parse(body) as Array<any>;
            try {
              const response = await SmartFunctionCallAsync(
                self.plugin,
                listener,
                bodyObj
              );
              iChannel.ack(msg);
              self.log.debug(`EAR: OKAY: {queueKey} -> {returnQueue}`, {
                queueKey,
                returnQueue,
              });
              if (
                !self.publishChannel.channel.sendToQueue(
                  returnQueue,
                  response,
                  {
                    expiration: 5000,
                    correlationId: `${msg.properties.correlationId}-resolve`,
                    contentType: "string",
                    appId: self.plugin.myId,
                    timestamp: new Date().getTime(),
                  }
                )
              )
                throw `Cannot send msg to queue [${returnQueue}]`;
              let end = new Date().getTime();
              let time = end - start;
              await self.log.reportStat(
                `eventsrec-${self.channelKey}-${pluginName}-${event}-ok`,
                time
              );
            } catch (exc) {
              self.log.error(`EAR: ERROR: {queueKey} -> {returnQueue}`, {
                queueKey,
                returnQueue,
              });
              if (
                !self.publishChannel.channel.sendToQueue(returnQueue, exc, {
                  expiration: 5000,
                  correlationId: `${msg.properties.correlationId}-reject`,
                  contentType: "string",
                  appId: self.plugin.myId,
                  timestamp: new Date().getTime(),
                })
              )
                throw `Cannot send msg to queue [${returnQueue}]`;
              iChannel.ack(msg);
              let end = new Date().getTime();
              let time = end - start;
              self.log.reportStat(
                `eventsrec-${self.channelKey}-${pluginName}-${event}-error`,
                time
              );
            }
          },
          { noAck: false }
        );
        self.log.debug(`EAR: listening {queueKey}`, {
          queueKey,
        });
      }
    );
  }

  async emitEventAndReturn(
    pluginName: string,
    event: string,
    timeoutSeconds: number,
    args: Array<any>
  ): Promise<any> {
    let start = new Date().getTime();
    const resultKey = `${randomUUID()}-${new Date().getTime()}${Math.random()}`;
    const queueKey = LIB.getQueueKey(
      this.plugin,
      this.channelKey,
      pluginName,
      event
    );
    this.log.debug(`EAR: emitting {queueKey} ({resultKey})`, {
      queueKey,
      resultKey,
    });
    const self = this;
    if (this.privateQueuesSetup.indexOf(queueKey) < 0) {
      this.privateQueuesSetup.push(queueKey);
      await this.publishChannel.channel.addSetup(
        async (iChannel: amqplibCore.ConfirmChannel) => {
          await iChannel.assertQueue(queueKey, self.queueOpts);
        }
      );
    }
    return await new Promise(async (resolve, reject) => {
      let timeoutHandler = setTimeout(async () => {
        self.removeAllListeners(`${resultKey}-resolve`);
        self.removeAllListeners(`${resultKey}-reject`);
        let end = new Date().getTime();
        let time = end - start;
        self.log.reportStat(
          `eventssen-${self.channelKey}-${pluginName}-${event}-error`,
          time
        );
        reject("Timeout");
      }, timeoutSeconds * 1000);
      self.once(`${resultKey}-resolve`, async (rargs: string) => {
        clearTimeout(timeoutHandler);
        let end = new Date().getTime();
        let time = end - start;
        await self.log.reportStat(
          `eventssen-${self.channelKey}-${pluginName}-${event}-ok`,
          time
        );
        resolve(rargs);
      });
      self.once(`${resultKey}-reject`, async (rargs: any) => {
        clearTimeout(timeoutHandler);
        let end = new Date().getTime();
        let time = end - start;
        await self.log.reportStat(
          `eventssen-${self.channelKey}-${pluginName}-${event}-error`,
          time
        );
        reject(rargs);
      });
      if (
        !self.publishChannel.channel.sendToQueue(queueKey, args, {
          expiration: timeoutSeconds * 1000 + 5000,
          correlationId: resultKey,
          contentType: "string",
          appId: self.plugin.myId,
          timestamp: new Date().getTime(),
        })
      )
        throw `Cannot send msg to queue [${queueKey}]`;
      self.log.debug(`EAR: emitted {queueKey} ({resultKey})`, {
        queueKey,
        resultKey,
      });
    });
  }
}
