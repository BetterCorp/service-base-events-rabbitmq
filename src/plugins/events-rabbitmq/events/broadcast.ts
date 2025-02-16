import { Plugin } from "../index";
import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { LIB, SetupChannel } from "./lib";
import { randomUUID } from "crypto";
import {
  IPluginLogging,
  SmartFunctionCallAsync,
  DTrace,
  IPluginMetrics,
} from "@bettercorp/service-base";

export class broadcast {
  private plugin: Plugin;
  private log: IPluginLogging;
  private metrics: IPluginMetrics;
  private publishQueuesSetup: Array<string> = [];
  private publishChannel!: SetupChannel<string>;
  private receiveChannel!: SetupChannel<string>;
  private readonly channelKey = "91eb";
  private readonly exchange = {
    type: "fanout",
    name: "better.service9.broadcast",
  };
  private readonly exchangeOpts: amqplib.Options.AssertExchange = {
    durable: false,
    //exclusive: true,
    autoDelete: false,
  };
  private readonly queueOpts: amqplib.Options.AssertQueue = {
    durable: false,
    autoDelete: false,
    messageTtl: 60 * 60 * 1000, // 60 min
    expires: 60 * 60 * 1000, // 60 min
  };

  constructor(plugin: Plugin, log: IPluginLogging, metrics: IPluginMetrics) {
    this.plugin = plugin;
    this.log = log;
    this.metrics = metrics;
  }

  async init(trace: DTrace) {
    this.log.debug(trace, `Open broadcast channel ({exchangeName})`, {
      exchangeName: this.exchange.name,
    });
    this.publishChannel = await LIB.setupChannel(
      trace,
      this.plugin,
      this.log,
      this.plugin.publishConnection,
      this.channelKey,
      this.exchange.name,
      this.exchange.type,
      this.exchangeOpts,
    );
    this.receiveChannel = await LIB.setupChannel(
      trace,
      this.plugin,
      this.log,
      this.plugin.receiveConnection,
      this.channelKey,
      this.exchange.name,
      this.exchange.type,
      this.exchangeOpts,
      5,
    );
  }

  public dispose() {
    this.publishChannel.channel.close();
    this.receiveChannel.channel.close();
  }

  async onBroadcast(
    trace: DTrace,
    pluginName: string,
    event: string,
    listener: { (trace: DTrace, args: Array<any>): Promise<void> },
  ): Promise<void> {
    const thisUUID = randomUUID();
    const rawQueueKey = LIB.getQueueKey(
      this.plugin,
      this.channelKey,
      pluginName,
      event,
    );
    const thisQueueKey = LIB.getQueueKey(
      this.plugin,
      this.channelKey,
      pluginName,
      event,
      thisUUID,
    );
    this.log.debug(trace, `LISTEN: [{thisQueueKey}]`, {
      thisQueueKey: rawQueueKey,
    });

    const self = this;
    await this.receiveChannel.channel.addSetup(
      async (iChannel: amqplibCore.ConfirmChannel) => {
        await iChannel.assertQueue(thisQueueKey, this.queueOpts);
        await this.receiveChannel.channel.consume(
          thisQueueKey,
          async (msg: amqplibCore.ConsumeMessage) => {
            //const start = Date.now();
            const body = msg.content.toString();
            const bodyObj = JSON.parse(body) as Array<any>;
            const iTrace = bodyObj.splice(0, 1)[0] as DTrace;
            const receiveSpan = self.metrics.createSpan(iTrace, "onBroadcast:receive", {
              pluginName,
              event,
              functionTraceId: trace.t,
              functionSpanId: trace.s
            });
            try {
              await SmartFunctionCallAsync(this.plugin, listener, iTrace, bodyObj);
              this.receiveChannel.channel.ack(msg);
              //const time = Date.now() - start;
              // this.log.reportStat(
              //     `eventsrec-${this.channelKey}-${pluginName}-${event}-ok`,
              //     time,
              // );
              receiveSpan.end();
            }
            catch (err: any) {
              this.receiveChannel.channel.nack(msg, true);
              //const time = Date.now() - start;
              // this.log.reportStat(
              //     `eventsrec-${this.channelKey}-${pluginName}-${event}-error`,
              //     time,
              // );
              this.log.error<"">(trace, err.toString() as "");
              receiveSpan.error(err);
              receiveSpan.end();
            }
          },
          { noAck: false },
        );
        await iChannel.bindQueue(
          thisQueueKey,
          this.receiveChannel.exchangeName,
          rawQueueKey,
        );
        this.log.debug(trace, `listen rabbit: [{thisQueueKey}]`, {
          thisQueueKey: rawQueueKey,
        });
      },
    );
  }

  async emitBroadcast(
    trace: DTrace,
    pluginName: string,
    event: string,
    args: Array<any>,
  ): Promise<void> {
    const thisQueueKey = LIB.getQueueKey(
      this.plugin,
      this.channelKey,
      pluginName,
      event,
    );
    this.log.debug(trace, `Emit: [{thisQueueKey}]`, {
      thisQueueKey,
    });
    const sendSpan = this.metrics.createSpan(trace, "emitBroadcast:send", {
      pluginName,
      event,
    });

    if (!this.publishQueuesSetup.includes(thisQueueKey)) {
      this.publishQueuesSetup.push(thisQueueKey);
      await this.publishChannel.channel.addSetup(
        async (iChannel: amqplibCore.ConfirmChannel) => {
          await iChannel.assertQueue(thisQueueKey, this.queueOpts);
          this.log.debug(trace, `emit rabbit: [{thisQueueKey}]`, { thisQueueKey });
        },
      );
    }

    if (
      !await this.publishChannel.channel.publish(
        this.exchange.name,
        thisQueueKey,
        [sendSpan.trace, ...args],
        {
          expiration: this.queueOpts.messageTtl,
          contentType: "string",
          appId: this.plugin.myId,
          timestamp: Date.now(),
        },
      )
    ) {
      sendSpan.error(new Error(`Cannot send msg to queue [${ thisQueueKey }]`));
      sendSpan.end();
      throw `Cannot send msg to queue [${ thisQueueKey }]`;
    }
    sendSpan.end({
      expiration: this.queueOpts.messageTtl ?? -1,
    });
    this.log.debug(trace, ` - EMIT: [${ thisQueueKey }] - EMITTED`);
  }
}
