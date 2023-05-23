import { Events } from "../plugin";
import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { EventEmitter } from "events";
import { randomUUID } from "crypto";
import { LIB, SetupChannel } from "./lib";

export class emitAndReturn extends EventEmitter {
  private uSelf!: Events;
  private privateQueuesSetup: Array<string> = [];
  private publishChannel!: SetupChannel;
  private receiveChannel!: SetupChannel;
  private readonly channelKey = "81ar";
  private readonly myChannelKey = "81kr";
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

  async init(uSelf: Events) {
    this.uSelf = uSelf;
    const self = this;
    const myEARQueueKey = await LIB.getMyQueueKey(
      uSelf,
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
      null
    );
    this.receiveChannel = await LIB.setupChannel(
      uSelf,
      uSelf.receiveConnection,
      this.myChannelKey,
      null,
      undefined,
      undefined,
      2
    );
    await this.receiveChannel.channel.addSetup(
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
    this.publishChannel.channel.close();
    this.receiveChannel.channel.close();
  }

  async onReturnableEvent(
    callerPluginName: string,
    pluginName: string,
    event: string,
    listener: { (args: Array<any>): Promise<any> }
  ): Promise<void> {
    const self = this;
    const queueKey = await LIB.getQueueKey(
      self.uSelf,
      this.channelKey,
      callerPluginName,
      pluginName,
      event
    );
    self.uSelf.log.debug(` EAR: {callerPluginName} listen {queueKey}`, {
      callerPluginName,
      queueKey,
    });

    await self.receiveChannel.channel.addSetup(
      async (iChannel: amqplibCore.ConfirmChannel) => {
        await iChannel.assertQueue(queueKey, self.queueOpts);
        await iChannel.consume(
          queueKey,
          async (msg: amqplibCore.ConsumeMessage | null): Promise<any> => {
            let start = new Date().getTime();
            if (msg === null)
              return self.uSelf.log.error(
                "Message received on my EAR queue was null..."
              );
            const returnQueue = await LIB.getMyQueueKey(
              self.uSelf,
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
              const response = await listener(bodyObj);
              iChannel.ack(msg);
              self.uSelf.log.debug(
                `EAR: {callerPluginName} OKAY: {queueKey} -> {returnQueue}`,
                { callerPluginName, queueKey, returnQueue }
              );
              if (
                !self.publishChannel.channel.sendToQueue(
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
              let end = new Date().getTime();
              let time = end - start;
              await self.uSelf.log.reportStat(
                `eventsrec-${self.channelKey}-${
                  pluginName || callerPluginName
                }-${event}-ok`,
                time
              );
            } catch (exc) {
              iChannel.ack(msg);
              self.uSelf.log.error(
                `EAR: {callerPluginName} ERROR: {queueKey} -> {returnQueue}`,
                { callerPluginName, queueKey, returnQueue }
              );
              if (
                !self.publishChannel.channel.sendToQueue(returnQueue, exc, {
                  expiration: 5000,
                  correlationId: `${msg.properties.correlationId}-reject`,
                  contentType: "string",
                  appId: self.uSelf.myId,
                  timestamp: new Date().getTime(),
                })
              )
                throw `Cannot send msg to queue [${returnQueue}]`;
              let end = new Date().getTime();
              let time = end - start;
              await self.uSelf.log.reportStat(
                `eventsrec-${self.channelKey}-${
                  pluginName || callerPluginName
                }-${event}-error`,
                time
              );
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
    let start = new Date().getTime();
    const resultKey = `${randomUUID()}-${new Date().getTime()}${Math.random()}`;
    const queueKey = await LIB.getQueueKey(
      self.uSelf,
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
      await self.publishChannel.channel.addSetup(
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
        await self.uSelf.log.reportStat(
          `eventssen-${self.channelKey}-${
            pluginName || callerPluginName
          }-${event}-error`,
          time
        );
        reject("Timeout");
      }, timeoutSeconds * 1000);
      self.once(`${resultKey}-resolve`, async (rargs: string) => {
        clearTimeout(timeoutHandler);
        let end = new Date().getTime();
        let time = end - start;
        await self.uSelf.log.reportStat(
          `eventssen-${self.channelKey}-${
            pluginName || callerPluginName
          }-${event}-ok`,
          time
        );
        resolve(rargs);
      });
      self.once(`${resultKey}-reject`, async (rargs: any) => {
        clearTimeout(timeoutHandler);
        let end = new Date().getTime();
        let time = end - start;
        await self.uSelf.log.reportStat(
          `eventssen-${self.channelKey}-${
            pluginName || callerPluginName
          }-${event}-error`,
          time
        );
        reject(rargs);
      });
      if (
        !self.publishChannel.channel.sendToQueue(queueKey, args, {
          expiration: timeoutSeconds * 1000 + 5000,
          correlationId: resultKey,
          contentType: "string",
          appId: self.uSelf.myId,
          timestamp: new Date().getTime(),
        })
      )
        throw `Cannot send msg to queue [${queueKey}]`;
      self.uSelf.log.debug(
        `EAR: {callerPluginName} emitted {queueKey} ({resultKey})`,
        { callerPluginName, queueKey, resultKey }
      );
    });
  }
}
