import { Plugin } from "../index";
import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { EventEmitter } from "events";
import { v7 as randomUUID } from "uuid";
import { LIB, SetupChannel } from "./lib";
import {
  BSBError,
  IPluginLogging,
  DTrace,
  SmartFunctionCallAsync,
  IPluginMetrics,
} from "@bettercorp/service-base";

export class emitAndReturn
  extends EventEmitter {
  private plugin: Plugin;
  private log: IPluginLogging;
  private metrics: IPluginMetrics;
  private privateQueuesSetup: Array<string> = [];
  private publishChannel!: SetupChannel;
  private receiveChannel!: SetupChannel;
  private readonly channelKey = "91ar";
  private readonly myChannelKey = "91kr";
  private readonly queueOpts: amqplib.Options.AssertQueue = {
    durable: false,
    autoDelete: false,
    messageTtl: 60 * 1000, // 60 seconds
    expires: 60 * 1000, // 60s
  };
  private readonly myQueueOpts: amqplib.Options.AssertQueue = {
    exclusive: true,
    durable: false,
    autoDelete: false,
    messageTtl: 60 * 1000, // 60 seconds
    expires: 60 * 1000, // 60s
  };

  constructor(plugin: Plugin, log: IPluginLogging, metrics: IPluginMetrics) {
    super();
    this.plugin = plugin;
    this.log = log;
    this.metrics = metrics;
  }

  async init(trace: DTrace) {
    const myEARQueueKey = LIB.getMyQueueKey(
      this.plugin,
      this.myChannelKey,
      this.plugin.myId,
    );
    this.log.debug(trace, `Ready my events name: {myEARQueueKey}`, {
      myEARQueueKey,
    });

    this.publishChannel = await LIB.setupChannel(
      trace,
      this.plugin,
      this.log,
      this.plugin.publishConnection,
      this.myChannelKey,
      null,
    );
    this.receiveChannel = await LIB.setupChannel(
      trace,
      this.plugin,
      this.log,
      this.plugin.receiveConnection,
      this.myChannelKey,
      null,
      undefined,
      undefined,
      2,
    );
    await this.receiveChannel.channel.addSetup(
      async (iChannel: amqplibCore.ConfirmChannel): Promise<void> => {
        await iChannel.assertQueue(myEARQueueKey, this.myQueueOpts);
        this.log.debug(trace, `LISTEN: [{myEARQueueKey}]`, { myEARQueueKey });
        await iChannel.consume(
          myEARQueueKey,
          (msg: amqplibCore.ConsumeMessage | null): any => {
            if (msg === null) {
              this.log.warn(trace, `[RECEIVED {myEARQueueKey}]... as null`, {
                myEARQueueKey,
              });
              return;
            }
            try {
              const body = msg.content.toString();
              this.log.debug(trace, `[RECEIVED {myEARQueueKey}]`, {
                myEARQueueKey,
              });
              this.emit(msg.properties.correlationId, JSON.parse(body)[0]);
              iChannel.ack(msg);
            } catch (exc: any) {
              this.log.error(trace, "AMQP Consumed exception: {eMsg}", {
                eMsg: exc.message || exc.toString(),
              });
              process.exit(7);
            }
          },
          { noAck: false },
        );
        this.log.debug(trace, `LISTEN: [{myEARQueueKey}]`, { myEARQueueKey });
        this.log.debug(trace, `Ready my events name: {myEARQueueKey} OKAY`, {
          myEARQueueKey,
        });
      },
    );
  }

  public dispose() {
    this.publishChannel.channel.close();
    this.receiveChannel.channel.close();
  }

  async onReturnableEvent(
    trace: DTrace,
    pluginName: string,
    event: string,
    listener: { (trace: DTrace, args: Array<any>): Promise<any> },
  ): Promise<void> {
    const queueKey = LIB.getQueueKey(
      this.plugin,
      this.channelKey,
      pluginName,
      event,
    );
    this.log.debug(trace, ` EAR: listen {queueKey}`, {
      queueKey,
    });

    await this.receiveChannel.channel.addSetup(
      async (iChannel: amqplibCore.ConfirmChannel) => {
        await iChannel.assertQueue(queueKey, this.queueOpts);
        await iChannel.consume(
          queueKey,
          async (msg: amqplibCore.ConsumeMessage | null): Promise<any> => {
            //const start = Date.now();
            if (msg === null) {
              return this.log.error(
                trace,
                "Message received on my EAR queue was null...",
              );
            }
            const returnQueue = LIB.getMyQueueKey(
              this.plugin,
              this.myChannelKey,
              msg.properties.appId,
            );
            this.log.debug(trace, `EAR: Received: {queueKey} from {returnQueue}`, {
              queueKey,
              returnQueue,
            });
            const body = msg.content.toString();
            const bodyObj = JSON.parse(body) as Array<any>;
            const iTrace = bodyObj.splice(0, 1)[0] as DTrace;
            const receiveSpan = this.metrics.createSpan(iTrace, "onReturnableEvent:receive", {
              pluginName,
              event,
              functionTraceId: trace.t,
              functionSpanId: trace.s
            });
            try {
              const response = await SmartFunctionCallAsync(
                this.plugin,
                listener,
                iTrace,
                bodyObj,
              );
              iChannel.ack(msg);
              this.log.debug(trace, `EAR: OKAY: {queueKey} -> {returnQueue}`, {
                queueKey,
                returnQueue,
              });
              const sendSpan = this.metrics.createSpan(iTrace, "onReturnableEvent:send", {
                pluginName,
                event,
                status: "ok"
              });
              if (
                !await this.publishChannel.channel.sendToQueue(
                  returnQueue,
                  [response],
                  {
                    expiration: 5000,
                    correlationId: `${ msg.properties.correlationId }-resolve`,
                    contentType: "string",
                    appId: this.plugin.myId,
                    timestamp: Date.now(),
                  },
                )
              ) {
                sendSpan.error(new Error(`Cannot send msg to queue [{returnQueue}]`));
                sendSpan.end();
                throw new BSBError(trace, `Cannot send msg to queue [{returnQueue}]`, { returnQueue });
              }
            } catch (exc) {
                this.log.error(trace, `EAR: ERROR: {queueKey} -> {returnQueue}`, {
                  queueKey,
                  returnQueue,
                });
              const sendSpan = this.metrics.createSpan(iTrace, "onReturnableEvent:send", {
                pluginName,
                event,
                status: "error"
              });
              if (
                !await this.publishChannel.channel.sendToQueue(returnQueue, [exc], {
                  expiration: 5000,
                  correlationId: `${ msg.properties.correlationId }-reject`,
                  contentType: "string",
                  appId: this.plugin.myId,
                  timestamp: Date.now(),
                })
              ) {
                sendSpan.error(new Error(`Cannot send msg to queue [{returnQueue}]`));
                sendSpan.end();
                receiveSpan.error(new Error(`Cannot send msg to queue [{returnQueue}]`));
                receiveSpan.end();
                throw new BSBError(trace, `Cannot send msg to queue [{returnQueue}]`, { returnQueue });
              }
              iChannel.ack(msg);
              sendSpan.end();
              receiveSpan.end();
            }
          },
          { noAck: false },
        );
        this.log.debug(trace, `EAR: listening {queueKey}`, {
          queueKey,
        });
      },
    );
  }

  async emitEventAndReturn(
    trace: DTrace,
    pluginName: string,
    event: string,
    timeoutSeconds: number,
    args: Array<any>,
  ): Promise<any> {
    const start = Date.now();
    const resultKey = `${ randomUUID() }-${ start }${ Math.random() }`;
    const queueKey = LIB.getQueueKey(
      this.plugin,
      this.channelKey,
      pluginName,
      event,
    );
    this.log.debug(trace, `EAR: emitting {queueKey} ({resultKey})`, {
      queueKey,
      resultKey,
    });

    const sendSpan = this.metrics.createSpan(trace, "emitEventAndReturn:send", {
      pluginName,
      event,
      timeoutSeconds
    });

    if (!this.privateQueuesSetup.includes(queueKey)) {
      this.privateQueuesSetup.push(queueKey);
      await this.publishChannel.channel.addSetup(
        async (iChannel: amqplibCore.ConfirmChannel) => {
          await iChannel.assertQueue(queueKey, this.queueOpts);
        },
      );
    }

    // eslint-disable-next-line no-async-promise-executor
    return new Promise(async (resolve: Function, reject: Function) => {
      const timeoutHandler = setTimeout(() => {
        this.removeAllListeners(`${ resultKey }-resolve`);
        this.removeAllListeners(`${ resultKey }-reject`);
        const timeoutError = new BSBError(sendSpan.trace, "Timeout");
        sendSpan.error(timeoutError);
        sendSpan.end();
        reject(timeoutError);
      }, timeoutSeconds * 1000);

      this.once(`${ resultKey }-resolve`, async (rargs: any) => {
        clearTimeout(timeoutHandler);
        sendSpan.end();
        resolve(rargs);
      });

      this.once(`${ resultKey }-reject`, async (rargs: any) => {
        clearTimeout(timeoutHandler);
        sendSpan.error(rargs);
        sendSpan.end();
        reject(rargs);
      });

      if (
        !await this.publishChannel.channel.sendToQueue(queueKey, [trace, ...args], {
          expiration: timeoutSeconds * 1000 + 5000,
          correlationId: resultKey,
          contentType: "string",
          appId: this.plugin.myId,
          timestamp: Date.now(),
        })
      ) {
        sendSpan.error(new Error(`Cannot send msg to queue [${ queueKey }]`));
        sendSpan.end();
        throw new BSBError(trace, `Cannot send msg to queue [{queueKey}]`, { queueKey });
      }
      this.log.debug(trace, `EAR: emitted {queueKey} ({resultKey})`, {
        queueKey,
        resultKey,
      });
    });
  }
}
