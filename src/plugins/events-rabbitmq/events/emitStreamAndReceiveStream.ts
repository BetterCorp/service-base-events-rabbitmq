import { Readable } from "stream";
import { randomUUID } from "crypto";
import { Events } from "../plugin";
import * as amqplib from "amqplib";
import { LIB } from "./lib";

export class emitStreamAndReceiveStream {
  // If we try receive or send a stream and the other party is not ready for some reason, we will automatically timeout in 5s.
  private readonly staticCommsTimeout = 30000; //1000;
  private uSelf!: Events;
  private publishChannel!: amqplib.Channel;
  private receiveChannel!: amqplib.Channel;
  private streamChannel!: amqplib.Channel;
  private readonly eventsChannelKey = "2se";
  private readonly senderChannelKey = "2ss";
  private readonly streamChannelKey = "2sc";
  private readonly exchange: any = {
    type: "direct",
    name: "better-service2-ers",
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

  async init(uSelf: Events) {
    this.uSelf = uSelf;
  }

  async setupChannelsIfNotSetup() {
    if (this.publishChannel === undefined)
      this.publishChannel = await LIB.setupChannel(
        this.uSelf,
        this.uSelf.publishConnection,
        this.eventsChannelKey,
        this.exchange.name,
        this.exchange.type,
        this.exchangeOpts
      );
    if (this.receiveChannel === undefined)
      this.receiveChannel = await LIB.setupChannel(
        this.uSelf,
        this.uSelf.receiveConnection,
        this.eventsChannelKey,
        this.exchange.name,
        this.exchange.type,
        this.exchangeOpts,
        2
      );
    if (this.streamChannel === undefined)
      this.streamChannel = await LIB.setupChannel(
        this.uSelf,
        this.uSelf.receiveConnection,
        this.streamChannelKey,
        this.exchange.name,
        this.exchange.type,
        this.exchangeOpts,
        2
      );
  }

  receiveStream(
    callerPluginName: string,
    listener: { (error: Error | null, stream: Readable): Promise<void> },
    timeoutSeconds = 5
  ): Promise<string> {
    const streamId = `${randomUUID()}-${new Date().getTime()}`;
    let thisTimeoutMS = this.staticCommsTimeout;
    const streamReturnRefId = LIB.getLocalKey(this.senderChannelKey, streamId);
    const streamEventsRefId = LIB.getLocalKey(this.eventsChannelKey, streamId);
    const streamRefId = LIB.getLocalKey(this.streamChannelKey, streamId);
    this.uSelf.log.info(`SR: ${callerPluginName} listening to ${streamId}`);
    const self = this;
    return new Promise(async (resolve) => {
      await self.setupChannelsIfNotSetup();
      await self.receiveChannel.assertQueue(streamEventsRefId, self.queueOpts);
      await self.streamChannel.assertQueue(streamRefId, self.queueOpts);
      let stream: Readable | null = null;
      let lastResponseTimeoutHandler: NodeJS.Timeout | null = null;
      let lastResponseTimeoutCount: number = 1;
      let receiptTimeoutHandler: NodeJS.Timeout | null;
      let createTimeout = (e: string): void => {
        throw "not setup yet : createTimeout";
      };
      const cleanup = async () => {
        createTimeout = (e) => {
          self.uSelf.log.debug("voided timeout creator: " + e);
        };
        self.uSelf.log.debug("Cleanup stuff");
        if (receiptTimeoutHandler !== null) {
          clearTimeout(receiptTimeoutHandler);
        }
        receiptTimeoutHandler = null;
        if (lastResponseTimeoutHandler !== null) {
          clearTimeout(lastResponseTimeoutHandler);
        }
        lastResponseTimeoutHandler = null;
        lastResponseTimeoutCount = -2;
        await self.receiveChannel.deleteQueue(streamEventsRefId);
        await self.streamChannel.deleteQueue(streamRefId);
        if (stream !== null && !stream.destroyed) {
          stream.destroy();
        }
      };
      receiptTimeoutHandler = setTimeout(async () => {
        self.uSelf.log.debug("Receive Receipt Timeout");
        const err = new Error("Receive Receipt Timeout");
        await cleanup();
        if (
          !self.publishChannel.sendToQueue(
            streamReturnRefId,
            Buffer.from(
              JSON.stringify({
                type: "timeout",
                data: err,
              })
            ),
            {
              expiration: self.queueOpts.messageTtl,
              correlationId: streamId,
              appId: self.uSelf.myId,
              timestamp: new Date().getTime(),
            }
          )
        )
          throw `Cannot send msg to queue [${streamRefId}]`;
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
        self.uSelf.log.error(err);
        await cleanup();
        if (
          !self.publishChannel.sendToQueue(
            streamReturnRefId,
            Buffer.from(
              JSON.stringify({
                type: "timeout",
                data: err,
              })
            ),
            {
              expiration: self.queueOpts.messageTtl,
              correlationId: streamId,
              appId: self.uSelf.myId,
              timestamp: new Date().getTime(),
            }
          )
        )
          throw `Cannot send msg to queue [${streamRefId}]`;
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
          !self.publishChannel.sendToQueue(
            streamReturnRefId,
            Buffer.from(
              JSON.stringify({ type: "receipt", timeout: thisTimeoutMS })
            ),
            {
              expiration: self.queueOpts.messageTtl,
              correlationId: streamId,
              appId: self.uSelf.myId,
              timestamp: new Date().getTime(),
            }
          )
        )
          throw `Cannot send msg to queue [${streamReturnRefId}] ${streamId}`;
        try {
          stream = new Readable({
            objectMode: true,
            read() {
              if (
                !self.publishChannel.sendToQueue(
                  streamReturnRefId,
                  Buffer.from(JSON.stringify({ type: "read" })),
                  {
                    expiration: self.queueOpts.messageTtl,
                    correlationId: streamId,
                    appId: self.uSelf.myId,
                    timestamp: new Date().getTime(),
                  }
                )
              )
                throw `Cannot send msg to queue [${streamReturnRefId}] ${streamId}`;
            },
          });
          self.uSelf.log.debug(`[R RECEVIED ${streamRefId}] ${streamId}`);
          let eventsToListenTo = ["error", "end"];
          for (let evnt of eventsToListenTo)
            stream.on(evnt, async (e: any, b: any) => {
              if (evnt === "end") await cleanup();
              if (b === "RECEIVED") return;
              if (
                !self.publishChannel.sendToQueue(
                  streamReturnRefId,
                  Buffer.from(
                    JSON.stringify({
                      type: "event",
                      event: evnt,
                      data: e || null,
                    })
                  ),
                  {
                    expiration: self.queueOpts.messageTtl,
                    correlationId: streamId,
                    appId: self.uSelf.myId,
                    timestamp: new Date().getTime(),
                  }
                )
              )
                throw `Cannot send msg to queue [${streamReturnRefId}] ${streamId}`;
            });
          await self.streamChannel.consume(
            streamRefId,
            async (sMsg: amqplib.ConsumeMessage | null): Promise<any> => {
              if (sMsg === null)
                return self.uSelf.log.debug(
                  `[R RECEVIED ${streamRefId}]... as null`
                );
              if (sMsg.properties.correlationId === "event") {
                let data = JSON.parse(sMsg.content.toString());
                stream!.emit(data.event, data.data || null, "RECEIVED");
                self.streamChannel.ack(sMsg);
                return;
              }
              stream!.push(sMsg.content);
              self.streamChannel.ack(sMsg);
            },
            { noAck: false }
          );
          listener(null, stream)
            .then(async () => {
              self.uSelf.log.info("stream OK");
            })
            .catch(async (x: Error) => {
              self.uSelf.log.error("Stream NOT OK", x);
              await cleanup();
              self.uSelf.log.fatal(x);
            });
        } catch (exc) {
          await cleanup();
          self.uSelf.log.fatal(exc);
        }
      };
      await self.receiveChannel.consume(
        streamEventsRefId,
        async (baseMsg: amqplib.ConsumeMessage | null): Promise<any> => {
          console.log(`streamEventsRefId Received`);
          if (receiptTimeoutHandler !== null) {
            clearTimeout(receiptTimeoutHandler);
            receiptTimeoutHandler = null;
          }
          updateLastResponseTimer();
          if (baseMsg === null)
            return self.uSelf.log.debug(
              `[R RECEVIED ${streamEventsRefId}]... as null`
            );
          let data = JSON.parse(baseMsg.content.toString());
          console.log(`streamEventsRefId Received:`, data);
          self.receiveChannel.ack(baseMsg);
          if (data.type === "timeout") {
            await cleanup();
            listener(data.data, null!);
            return;
          }
          if (data.type === "event") {
            stream!.emit(data.event, data.data || null, "RECEIVED");
            return;
          }
          if (data.type === "start") {
            self.uSelf.log.info("Readying to stream");
            await startStream();
            self.uSelf.log.info("Starting to stream");
            return;
          }
        },
        { noAck: false }
      );
      resolve(streamId);
    });
  }

  sendStream(
    callerPluginName: string,
    streamId: string,
    stream: Readable
  ): Promise<void> {
    const self = this;
    const streamReturnRefId = LIB.getLocalKey(this.senderChannelKey, streamId);
    const streamEventsRefId = LIB.getLocalKey(this.eventsChannelKey, streamId);
    const streamRefId = LIB.getLocalKey(this.streamChannelKey, streamId);
    let thisTimeoutMS = self.staticCommsTimeout;
    this.uSelf.log.info(
      `SS: ${callerPluginName} emitting ${streamEventsRefId}`
    );
    return new Promise(async (resolveI, rejectI) => {
      await self.setupChannelsIfNotSetup();
      await self.receiveChannel.assertQueue(streamReturnRefId, self.queueOpts);
      let lastResponseTimeoutHandler: NodeJS.Timeout | null = null;
      let lastResponseTimeoutCount: number = 1;
      let receiptTimeoutHandler: NodeJS.Timeout | null = setTimeout(() => {
        reject(new Error("Send Receipt Timeout"));
      }, thisTimeoutMS);
      const cleanup = async (eType: string, e?: Error) => {
        self.uSelf.log.debug("cleanup:", eType);
        stream.destroy(e);

        if (receiptTimeoutHandler !== null) clearTimeout(receiptTimeoutHandler);
        if (lastResponseTimeoutHandler !== null)
          clearTimeout(lastResponseTimeoutHandler);
        receiptTimeoutHandler = null;
        lastResponseTimeoutHandler = null;
        await self.receiveChannel.deleteQueue(streamReturnRefId);
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
              !self.publishChannel.sendToQueue(
                streamReturnRefId,
                Buffer.from(
                  JSON.stringify({
                    type: "timeout",
                    data: err,
                  })
                ),
                {
                  expiration: self.queueOpts.messageTtl,
                  correlationId: streamId,
                  appId: self.uSelf.myId,
                  timestamp: new Date().getTime(),
                }
              )
            )
              throw `Cannot send msg to queue [${streamRefId}]`;
            rejectI(err);
          };
          createTimeout = () => {
            lastResponseTimeoutHandler = setTimeout(timeoutFunc, thisTimeoutMS);
          };
          createTimeout();
        }
      };
      let eventsToListenTo = ["error", "close", "end"];
      for (let evnt of eventsToListenTo) {
        stream.on(evnt, async (e: any, b: any) => {
          await cleanup(evnt);
          if (b === "RECEIVED") return;
          if (
            !self.publishChannel.sendToQueue(
              streamRefId,
              Buffer.from(
                JSON.stringify({ type: "event", event: evnt, data: e || null })
              ),
              {
                expiration: self.queueOpts.messageTtl,
                correlationId: "event",
                appId: self.uSelf.myId,
                timestamp: new Date().getTime(),
              }
            )
          )
            throw `Cannot send msg to queue [${streamEventsRefId}] ${streamId}`;
          if (evnt === "error") reject(e);
          if (evnt === "close") resolve();
        });
      }
      let pushingData = false;
      let streamStarted = false;
      const pushData = () => {
        if (pushingData) return;
        pushingData = true;
        self.uSelf.log.warn("Switching to push data model.");
        stream.on("data", (data) => {
          if (
            !self.publishChannel.sendToQueue(streamRefId, data, {
              expiration: self.queueOpts.messageTtl,
              correlationId: "stream",
              appId: self.uSelf.myId,
              timestamp: new Date().getTime(),
            })
          ) {
            pushingData = false;
            self.uSelf.log.error(
              `Cannot push msg to queue [${streamRefId}] ${streamId} / switch back to poll model.`
            );
          }
        });
      };
      await self.receiveChannel.consume(
        streamReturnRefId,
        async (baseMsg: amqplib.ConsumeMessage | null): Promise<any> => {
          if (receiptTimeoutHandler !== null) {
            clearTimeout(receiptTimeoutHandler);
            receiptTimeoutHandler = null;
          }
          updateLastResponseTimer();
          if (baseMsg === null)
            return self.uSelf.log.debug(
              `[S RECEVIED ${streamEventsRefId}]... as null`
            );
          let data = JSON.parse(baseMsg.content.toString());
          self.receiveChannel.ack(baseMsg);
          if (data.type === "timeout") {
            await reject(new Error("timeout-receiver"));
            return;
          }
          if (data.type === "receipt") {
            thisTimeoutMS = data.timeout;
            return;
          }
          if (data.type === "event") {
            stream!.emit(data.event, data.data || null, "RECEIVED");
            return;
          }
          if (data.type === "read") {
            if (pushingData) return;
            const readData = stream.read();
            if (!stream.readable || readData === null) {
              self.uSelf.log.info("Stream no longer readable.");
              if (!streamStarted) pushData();
              return;
            }
            streamStarted = true;
            if (
              !self.publishChannel.sendToQueue(streamRefId, readData, {
                expiration: self.queueOpts.messageTtl,
                correlationId: "stream",
                appId: self.uSelf.myId,
                timestamp: new Date().getTime(),
              })
            )
              throw `Cannot send msg to queue [${streamRefId}] ${streamId}`;
            return;
          }
        },
        { noAck: false }
      );
      self.uSelf.log.info(
        `SS: ${callerPluginName} setup, ready ${streamEventsRefId}`
      );
      if (
        !self.publishChannel.sendToQueue(
          streamEventsRefId,
          Buffer.from(JSON.stringify({ type: "start" })),
          {
            expiration: self.queueOpts.messageTtl,
            correlationId: streamId,
            appId: self.uSelf.myId,
            timestamp: new Date().getTime(),
          }
        )
      )
        throw `Cannot send msg to queue [${streamEventsRefId}]`;
      self.uSelf.log.info(
        `SS: ${callerPluginName} emitted ${streamEventsRefId}`
      );
    });
  }
}
