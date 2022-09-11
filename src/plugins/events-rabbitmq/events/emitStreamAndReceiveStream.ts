import { EventEmitter, Readable } from "stream";
import { randomUUID } from "crypto";
import { Events } from "../plugin";
import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { LIB } from "./lib";

export class emitStreamAndReceiveStream extends EventEmitter {
  // If we try receive or send a stream and the other party is not ready for some reason, we will automatically timeout in 5s.
  private readonly staticCommsTimeout = 30000; //1000;
  private uSelf!: Events;
  private eventsChannel!: amqplib.ChannelWrapper;
  private streamChannel!: amqplib.ChannelWrapper;
  private readonly eventsChannelKey = "8se";
  private readonly streamChannelKey = "8sd";
  private readonly exchange: any = {
    type: "direct",
    name: "better-service8-ers",
  };
  private readonly exchangeOpts: amqplib.Options.AssertExchange = {
    durable: false,
    autoDelete: true,
  };
  private readonly queueOpts: amqplib.Options.AssertQueue = {
    durable: false,
    autoDelete: true,
    messageTtl: 60000, // 60s
    expires: 60000, // 60s
  };

  private get myEventsQueueKey() {
    return LIB.getSpecialQueueKey(this.eventsChannelKey, this.uSelf.myId);
  }
  private get myStreamQueueKey() {
    return LIB.getSpecialQueueKey(this.streamChannelKey, this.uSelf.myId);
  }

  private cleanupSelf(streamId: string, key: string) {
    this.removeAllListeners(this.eventsChannelKey + key + streamId);
    this.removeAllListeners(this.streamChannelKey + key + streamId);
  }

  async init(uSelf: Events) {
    this.uSelf = uSelf;
  }
  public dispose() {
    this.removeAllListeners();
    if (this.eventsChannel !== undefined) this.eventsChannel.close();
    if (this.streamChannel !== undefined) this.streamChannel.close();
  }

  async setupChannelsIfNotSetup() {
    const self = this;
    if (this.eventsChannel === undefined) {
      this.eventsChannel = await LIB.setupChannel(
        this.uSelf,
        this.uSelf.receiveConnection,
        this.eventsChannelKey,
        this.exchange.name,
        this.exchange.type,
        this.exchangeOpts,
        2
      );
      this.uSelf.log.debug(`Ready my events name: {myEARQueueKey}`, {
        myEARQueueKey: this.myEventsQueueKey,
      });
      await this.eventsChannel.addSetup(
        async (iChannel: amqplibCore.ConfirmChannel) => {
          await iChannel.assertQueue(self.myEventsQueueKey, self.queueOpts);
          self.uSelf.log.debug(`LISTEN: [{myEARQueueKey}]`, {
            myEARQueueKey: self.myEventsQueueKey,
          });
          await iChannel.consume(
            self.myEventsQueueKey,
            (msg: amqplibCore.ConsumeMessage | null): any => {
              if (msg === null)
                return self.uSelf.log.warn(
                  `[RECEVIED {myEARQueueKey}]... as null`,
                  { myEARQueueKey: self.myEventsQueueKey }
                );
              try {
                let body = JSON.parse(msg.content.toString());
                self.uSelf.log.debug(
                  `[RECEVIED Event {myEARQueueKey}] ({correlationId})`,
                  {
                    myEARQueueKey: self.myEventsQueueKey,
                    correlationId: msg.properties.correlationId,
                  }
                );
                self.emit(
                  self.eventsChannelKey + msg.properties.correlationId,
                  body,
                  () => {
                    //ack
                    iChannel.ack(msg);
                  },
                  () => {
                    //nack
                    iChannel.nack(msg);
                  }
                );
              } catch (exc: any) {
                self.uSelf.log.fatal("AMPQ Consumed exception: {eMsg}", {
                  eMsg: exc.message || exc.toString(),
                });
              }
            },
            { noAck: false }
          );
          self.uSelf.log.debug(`LISTEN: [{myEARQueueKey}]`, {
            myEARQueueKey: self.myEventsQueueKey,
          });
          self.uSelf.log.debug(`Ready my events name: {myEARQueueKey} OKAY`, {
            myEARQueueKey: self.myEventsQueueKey,
          });
        }
      );
    }
    if (this.streamChannel === undefined) {
      this.streamChannel = await LIB.setupChannel(
        this.uSelf,
        this.uSelf.receiveConnection,
        this.streamChannelKey,
        this.exchange.name,
        this.exchange.type,
        this.exchangeOpts,
        2
        //,false
      );
      this.uSelf.log.debug(`Ready my stream name: {myEARQueueKey}`, {
        myEARQueueKey: self.myStreamQueueKey,
      });
      await this.streamChannel.addSetup(
        async (iChannel: amqplibCore.ConfirmChannel) => {
          await iChannel.assertQueue(self.myStreamQueueKey, self.queueOpts);
          self.uSelf.log.debug(`LISTEN: [{myEARQueueKey}]`, {
            myEARQueueKey: self.myStreamQueueKey,
          });
          await iChannel.consume(
            self.myStreamQueueKey,
            (msg: amqplibCore.ConsumeMessage | null): any => {
              if (msg === null)
                return self.uSelf.log.warn(
                  `[RECEVIED {myEARQueueKey}]... as null`,
                  { myEARQueueKey: self.myStreamQueueKey }
                );
              try {
                let body = JSON.parse(msg.content.toString());
                self.uSelf.log.debug(`[RECEVIED Stream {myEARQueueKey}]`, {
                  myEARQueueKey: self.myStreamQueueKey,
                });

                self.emit(
                  self.streamChannelKey + "r-" + msg.properties.correlationId,
                  body,
                  () => {
                    //ack
                    iChannel.ack(msg);
                  },
                  () => {
                    //nack
                    iChannel.nack(msg);
                  }
                );
              } catch (exc: any) {
                self.uSelf.log.fatal("AMPQ Consumed exception: {eMsg}", {
                  eMsg: exc.message || exc.toString(),
                });
              }
            },
            { noAck: false }
          );
          self.uSelf.log.debug(`LISTEN: [{myEARQueueKey}]`, {
            myEARQueueKey: self.myStreamQueueKey,
          });
          self.uSelf.log.debug(`Ready my stream name: {myEARQueueKey} OKAY`, {
            myEARQueueKey: self.myStreamQueueKey,
          });
        }
      );
    }
  }

  receiveStream(
    callerPluginName: string,
    listener: { (error: Error | null, stream: Readable): Promise<void> },
    timeoutSeconds = 5
  ): Promise<string> {
    const streamId = `${randomUUID()}-${new Date().getTime()}`;
    let thisTimeoutMS = this.staticCommsTimeout;
    this.uSelf.log.debug(`SR: {callerPluginName} listening to {streamId}`, {
      callerPluginName,
      streamId,
    });
    const self = this;
    let dstEventsQueueKey: string;
    return new Promise(async (resolve) => {
      await self.setupChannelsIfNotSetup();
      let stream: Readable | null = null;
      let lastResponseTimeoutHandler: NodeJS.Timeout | null = null;
      let lastResponseTimeoutCount: number = 1;
      let receiptTimeoutHandler: NodeJS.Timeout | null;
      let createTimeout = (e: string): void => {
        throw "not setup yet : createTimeout";
      };
      const cleanup = async () => {
        self.cleanupSelf(streamId, "r-");
        createTimeout = (e) => {
          self.uSelf.log.debug("voided timeout creator: {e}", { e });
        };
        self.uSelf.log.debug("Cleanup stuffR");
        if (receiptTimeoutHandler !== null) {
          clearTimeout(receiptTimeoutHandler);
        }
        receiptTimeoutHandler = null;
        if (lastResponseTimeoutHandler !== null) {
          clearTimeout(lastResponseTimeoutHandler);
        }
        lastResponseTimeoutHandler = null;
        lastResponseTimeoutCount = -2;
        if (stream !== null && !stream.destroyed) {
          stream.destroy();
        }
      };
      receiptTimeoutHandler = setTimeout(async () => {
        self.uSelf.log.debug("Receive Receipt Timeout");
        const err = new Error("Receive Receipt Timeout");
        await cleanup();
        if (
          !(await self.eventsChannel.sendToQueue(
            dstEventsQueueKey,
            {
              type: "timeout",
              data: err,
            },
            {
              expiration: self.queueOpts.messageTtl,
              correlationId: "s-" + streamId,
              appId: self.uSelf.myId,
              timestamp: new Date().getTime(),
            }
          ))
        )
          throw `Cannot send msg to queue [${dstEventsQueueKey}]`;
        await listener(err, null!);
      }, thisTimeoutMS);
      const timeoutFunc = async () => {
        if (lastResponseTimeoutHandler === null) return;
        if (lastResponseTimeoutCount === -2) return;
        if (lastResponseTimeoutCount > 0) {
          lastResponseTimeoutCount--;
          createTimeout("timeoutFunc");
          return;
        }
        const err = new Error("Receive Active Timeout");
        self.uSelf.log.error("Receive Active Timeout");
        await cleanup();
        if (
          !(await self.eventsChannel.sendToQueue(
            dstEventsQueueKey,
            {
              type: "timeout",
              data: err,
            },
            {
              expiration: self.queueOpts.messageTtl,
              correlationId: "s-" + streamId,
              appId: self.uSelf.myId,
              timestamp: new Date().getTime(),
            }
          ))
        )
          throw `Cannot send msg to queue [${dstEventsQueueKey}]`;
        await listener(err, null!);
      };
      createTimeout = () => {
        if (lastResponseTimeoutCount === -2) return;
        if (lastResponseTimeoutHandler === null) {
          lastResponseTimeoutHandler = setTimeout(timeoutFunc, thisTimeoutMS);
        }
      };
      const updateLastResponseTimer = () => {
        if (lastResponseTimeoutCount === -2) return;
        lastResponseTimeoutCount = 1;
        createTimeout("updateLastResponseTimer");
      };
      const startStream = async () => {
        self.uSelf.log.debug("START STREAM RECEIVER");
        thisTimeoutMS = timeoutSeconds * 1000;
        if (
          !(await self.eventsChannel.sendToQueue(
            dstEventsQueueKey,
            { type: "receipt", timeout: thisTimeoutMS },
            {
              expiration: self.queueOpts.messageTtl,
              correlationId: "s-" + streamId,
              appId: self.uSelf.myId,
              timestamp: new Date().getTime(),
            }
          ))
        )
          throw `Cannot send msg to queue [${dstEventsQueueKey}] ${streamId}`;
        try {
          stream = new Readable({
            objectMode: true,
            async read() {
              if (
                !(await self.eventsChannel.sendToQueue(
                  dstEventsQueueKey,
                  { type: "read" },
                  {
                    expiration: self.queueOpts.messageTtl,
                    correlationId: "s-" + streamId,
                    appId: self.uSelf.myId,
                    timestamp: new Date().getTime(),
                  }
                ))
              )
                throw `Cannot send msg to queue [${dstEventsQueueKey}] ${streamId}`;
            },
          });
          self.uSelf.log.debug(`[R RECEVIED {streamRefId}] {streamId}`, {
            streamRefId: dstEventsQueueKey,
            streamId,
          });
          let eventsToListenTo = ["error", "end"];
          for (let evnt of eventsToListenTo)
            stream.on(evnt, async (e: any, b: any) => {
              if (
                !(await self.eventsChannel.sendToQueue(
                  dstEventsQueueKey,
                  {
                    type: "event",
                    event: evnt,
                    data: e || null,
                  },
                  {
                    expiration: self.queueOpts.messageTtl,
                    correlationId: "s-" + streamId,
                    appId: self.uSelf.myId,
                    timestamp: new Date().getTime(),
                  }
                ))
              ) {
                throw `Cannot send msg to queue [${dstEventsQueueKey}] ${streamId}`;
              }
              if (evnt === "end") {
                await cleanup();
              }
            });
          self.on(
            self.streamChannelKey + "r-" + streamId,
            async (data: any, ack: { (): void }, nack: { (): void }) => {
              if (data === null) {
                nack();
                return self.uSelf.log.debug(
                  `[R RECEVIED {streamId}]... as null`,
                  { streamId }
                );
              }
              if (
                !(await self.eventsChannel.sendToQueue(
                  dstEventsQueueKey,
                  {
                    type: "receipt",
                    timeout: thisTimeoutMS,
                  },
                  {
                    expiration: self.queueOpts.messageTtl,
                    correlationId: "s-" + streamId,
                    appId: self.uSelf.myId,
                    timestamp: new Date().getTime(),
                  }
                ))
              )
                throw `Cannot send msg to queue [${dstEventsQueueKey}] ${streamId}`;
              if (data.type === "event") {
                stream!.emit(
                  data.event,
                  data.data !== undefined ? data.data : null
                );
                ack();
                return;
              }
              if (data.type === "data") {
                stream!.push(Buffer.from(data.data));
                ack();
                return;
              }
              nack();
            }
          );
          listener(null, stream)
            .then(async () => {
              self.uSelf.log.info("stream OK");
            })
            .catch(async (x: Error) => {
              await cleanup();
              self.uSelf.log.fatal("Stream NOT OK: {e}", { e: x.message });
            });
        } catch (exc: any) {
          await cleanup();
          self.uSelf.log.fatal("Stream NOT OK: {e}", { e: exc.message || exc });
        }
      };
      self.on(
        self.eventsChannelKey + "r-" + streamId,
        async (data: any, ack: { (): void }, nack: { (): void }) => {
          if (receiptTimeoutHandler !== null) {
            clearTimeout(receiptTimeoutHandler);
            receiptTimeoutHandler = null;
          }
          updateLastResponseTimer();
          if (data === null)
            return self.uSelf.log.debug(
              `[R RECEVIED {streamEventsRefId}]... as null`,
              { streamEventsRefId: dstEventsQueueKey }
            );
          if (data.type === "timeout") {
            await cleanup();
            listener(data.data, null!);
            ack();
            return;
          }
          if (data.type === "start") {
            self.uSelf.log.debug("Readying to stream from: {fromId}", {
              fromId: data.myId,
            });
            dstEventsQueueKey = LIB.getSpecialQueueKey(
              this.eventsChannelKey,
              data.myId
            );
            await startStream();
            self.uSelf.log.debug("Starting to stream");
            ack();
            return;
          }
          nack();
        }
      );
      resolve(`${this.uSelf.myId}||${streamId}||${timeoutSeconds}`);
    });
  }

  sendStream(
    callerPluginName: string,
    streamIdf: string,
    stream: Readable
  ): Promise<void> {
    if (streamIdf.split("||").length !== 3) throw "invalid stream ID";
    let streamReceiverId = streamIdf.split("||")[0];
    let streamId = streamIdf.split("||")[1];
    let streamTimeoutS = Number.parseInt(streamIdf.split("||")[2]);
    let thisTimeoutMS = this.staticCommsTimeout;
    const dstEventsQueueKey = LIB.getSpecialQueueKey(
      this.eventsChannelKey,
      streamReceiverId
    );
    const dstStreamQueueKey = LIB.getSpecialQueueKey(
      this.streamChannelKey,
      streamReceiverId
    );
    const self = this;
    this.uSelf.log.info(
      `SS: {callerPluginName} emitting to {dstEventsQueueKey}/{dstStreamQueueKey}`,
      { callerPluginName, dstEventsQueueKey, dstStreamQueueKey }
    );
    return new Promise(async (resolveI, rejectI) => {
      await self.setupChannelsIfNotSetup();
      let lastResponseTimeoutHandler: NodeJS.Timeout | null = null;
      let lastResponseTimeoutCount: number = 1;
      let receiptTimeoutHandler: NodeJS.Timeout | null = setTimeout(() => {
        reject(new Error("Send Receipt Timeout"));
      }, thisTimeoutMS);
      const cleanup = async (eType: string, e?: Error) => {
        self.uSelf.log.debug("cleanup: {eType}", { eType });
        self.cleanupSelf(streamId, "s-");
        stream.destroy(e);

        if (receiptTimeoutHandler !== null) clearTimeout(receiptTimeoutHandler);
        if (lastResponseTimeoutHandler !== null)
          clearTimeout(lastResponseTimeoutHandler);
        receiptTimeoutHandler = null;
        lastResponseTimeoutHandler = null;
      };
      const reject = async (e: Error) => {
        await cleanup("reject-" + e.message, e);
        rejectI(e);
      };
      const resolve = async () => {
        await cleanup("resolved");
        resolveI();
      };
      const updateLastResponseTimer = () => {
        lastResponseTimeoutCount = 1;
        if (lastResponseTimeoutHandler === null) {
          let createTimeout = (): void => {
            throw "not setup yet : createTimeout";
          };
          const timeoutFunc = async () => {
            if (lastResponseTimeoutCount > 0) {
              lastResponseTimeoutCount--;
              createTimeout();
              return;
            }
            self.uSelf.log.debug("Receive Receipt Timeout");
            const err = new Error("Receive Active Timeout");
            await cleanup("active-timeout");
            if (
              !(await self.eventsChannel.sendToQueue(
                dstEventsQueueKey,
                {
                  type: "timeout",
                  data: err,
                },
                {
                  expiration: self.queueOpts.messageTtl,
                  correlationId: "r-" + streamId,
                  appId: self.uSelf.myId,
                  timestamp: new Date().getTime(),
                }
              ))
            )
              throw `Cannot send msg to queue [${dstEventsQueueKey}]`;
            rejectI(err);
          };
          createTimeout = () => {
            lastResponseTimeoutHandler = setTimeout(timeoutFunc, thisTimeoutMS);
          };
          createTimeout();
        }
      };
      let eventsToListenTo: Array<string> = ["error", "end"];
      for (let evnt of eventsToListenTo) {
        stream.on(
          evnt,
          async (e: any, b: any, ack: { (): void }, nack: { (): void }) => {
            if (
              !(await self.streamChannel.sendToQueue(
                dstStreamQueueKey,
                { type: "event", event: evnt, data: e || null },
                {
                  expiration: self.queueOpts.messageTtl,
                  correlationId: /*"r-" + */ streamId,
                  appId: self.uSelf.myId,
                  timestamp: new Date().getTime(),
                }
              ))
            ) {
              nack();
              throw `Cannot send msg to queue [${dstEventsQueueKey}] ${streamId}`;
            }
            ack();
            if (evnt === "error") reject(e);
          }
        );
      }
      let pushingData = false;
      let streamStarted = false;
      const pushData = () => {
        if (pushingData) {
          self.uSelf.log.warn(
            "Stream tried pushing data, but not ready to push data!"
          );
          return;
        }
        pushingData = true;
        self.uSelf.log.warn("Switching to push data model.");
        stream.on("data", async (data: any) => {
          if (
            !(await self.streamChannel.sendToQueue(
              dstStreamQueueKey,
              { type: "data", data },
              {
                expiration: self.queueOpts.messageTtl,
                correlationId: streamId,
                appId: self.uSelf.myId,
                timestamp: new Date().getTime(),
              }
            ))
          ) {
            pushingData = false;
            self.uSelf.log.error(
              `Cannot push msg to queue [{dstStreamQueueKey}] {streamId} / switch back to poll model.`,
              { dstStreamQueueKey, streamId }
            );
          }
        });
      };
      self.on(
        self.eventsChannelKey + "s-" + streamId,
        async (data: any, ack: { (): void }, nack: { (): void }) => {
          if (receiptTimeoutHandler !== null) {
            clearTimeout(receiptTimeoutHandler);
            receiptTimeoutHandler = null;
          }
          updateLastResponseTimer();
          if (data === null) {
            nack();
            return self.uSelf.log.debug(
              `[S RECEVIED {dstEventsQueueKey}]... as null`,
              { dstEventsQueueKey }
            );
          }
          if (data.type === "timeout") {
            await reject(new Error("timeout-receiver"));
            return ack();
          }
          if (data.type === "receipt") {
            thisTimeoutMS = data.timeout;
            return ack();
          }
          if (data.type === "event") {
            if (data.event === "end") {
              ack();
              return resolve();
            }
            stream!.emit(data.event, data.data || null, "RECEIVED");
            return ack();
          }
          if (data.type === "read") {
            if (pushingData) return ack();
            const readData = stream.read();
            if (!stream.readable || readData === null) {
              self.uSelf.log.info("Stream no longer readable.");
              if (!streamStarted) pushData();
              return ack();
            }
            streamStarted = true;
            if (
              !(await self.streamChannel.sendToQueue(
                dstStreamQueueKey,
                { type: "data", data: readData },
                {
                  expiration: self.queueOpts.messageTtl,
                  correlationId: streamId,
                  appId: self.uSelf.myId,
                  timestamp: new Date().getTime(),
                }
              ))
            ) {
              nack();
              throw `Cannot send msg to queue [${dstStreamQueueKey}] ${streamId}`;
            }
            ack();
            return;
          }
          ack();
        }
      );
      self.uSelf.log.info(
        `SS: {callerPluginName} setup, ready {streamEventsRefId}`,
        { callerPluginName, streamEventsRefId: dstEventsQueueKey }
      );
      if (
        !(await self.eventsChannel.sendToQueue(
          dstEventsQueueKey,
          { type: "start", myId: self.uSelf.myId },
          {
            expiration: self.queueOpts.messageTtl,
            correlationId: "r-" + streamId,
            appId: self.uSelf.myId,
            timestamp: new Date().getTime(),
          }
        ))
      )
        throw `Cannot send msg to queue [${dstEventsQueueKey}]`;
      thisTimeoutMS = streamTimeoutS * 1000;
      self.uSelf.log.info(
        `SS: {callerPluginName} emitted {dstEventsQueueKey} with timeout of {thisTimeoutMS}`,
        { callerPluginName, dstEventsQueueKey, thisTimeoutMS }
      );
    });
  }
}
