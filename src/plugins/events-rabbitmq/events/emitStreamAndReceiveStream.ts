/**
 * BSB (Better-Service-Base) is an event-bus based microservice framework.  
 * Copyright (C) 2016 - 2025 BetterCorp (PTY) Ltd  
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Alternatively, you may obtain a commercial license for this program. 
 * The commercial license allows you to use the Program in a closed-source manner, 
 * including the right to create derivative works that are not subject to the terms 
 * of the AGPL. 
 *
 * To obtain a commercial license, please contact the copyright holders at 
 * https://www.bettercorp.dev. The terms and conditions of the commercial license 
 * will be provided upon request.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

import { EventEmitter } from "node:events";
import { Readable } from "node:stream";
import { randomUUID } from "node:crypto";
import { BSBError, DTrace, IPluginLogging, IPluginMetrics, Tools } from "@bettercorp/service-base";
import { Plugin } from "../index";
import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { LIB, SetupChannel } from "./lib";

export class emitStreamAndReceiveStream
  extends EventEmitter {
  // If we try receive or send a stream and the other party is not ready for some reason, we will automatically timeout in 5s.
  private readonly staticCommsTimeout = 1000;
  private readonly MAX_CHUNK_SIZE = 128 * 1024; // 128KB chunks
  private readonly CHANNEL_SETUP_TIMEOUT = 5000; // 5s for channel setup

  private log: IPluginLogging;
  private metrics: IPluginMetrics;
  private plugin: Plugin;
  private eventsChannel!: SetupChannel;
  private streamChannel!: SetupChannel;

  private readonly queueOpts: amqplib.Options.AssertQueue = {
    durable: true,
    autoDelete: false,
    messageTtl: 60000, // 60 seconds
    expires: 120000, // 2 minutes
  };

  constructor(plugin: Plugin, log: IPluginLogging, metrics: IPluginMetrics) {
    super();
    this.plugin = plugin;
    this.log = log;
    this.metrics = metrics;
  }

  public dispose() {
    this.removeAllListeners();
    if (this.eventsChannel?.channel) {
      this.eventsChannel.channel.close();
    }
    if (this.streamChannel?.channel) {
      this.streamChannel.channel.close();
    }
  }

  private async setupChannel(trace: DTrace, channelType: 'events' | 'stream'): Promise<SetupChannel> {
    const channelKey = channelType === 'events' ? 'ev' : 'st';
    const queueKey = LIB.getMyQueueKey(this.plugin, channelKey, this.plugin.myId);
    const span = this.metrics.createSpan(trace, `setupChannel:${channelType}`, {
      channelKey,
      queueKey
    });

    return new Promise(async (resolve, reject) => {
      const setupTimeout = setTimeout(() => {
        const error = new BSBError(span.trace, `Channel setup timeout for ${channelType}`);
        span.error(error);
        reject(error);
      }, this.CHANNEL_SETUP_TIMEOUT);

      try {
        const channel = await LIB.setupChannel(
          span.trace,
          this.plugin,
          this.log,
          this.plugin.receiveConnection,
          channelKey,
          null,
          undefined,
          undefined,
          channelType === 'stream' ? 10 : 2 // Higher prefetch for stream channel
        );

        await channel.channel.addSetup(async (ch: amqplibCore.ConfirmChannel) => {
          await ch.assertQueue(queueKey, this.queueOpts);
          
          await ch.consume(queueKey, async (msg) => {
            if (!msg) return;

            try {
              const data = JSON.parse(msg.content.toString());
              const streamId = msg.properties.correlationId;
              const eventKey = `${channelKey}-${streamId}`;

              this.log.debug(span.trace, "Received message on {queue} for stream {id}", { 
                queue: queueKey,
                id: streamId
              });

              this.emit(eventKey, data);
              ch.ack(msg);
            } catch (error) {
              this.log.error(span.trace, "Failed to process message: {error}", {
                error: error instanceof Error ? error.message : String(error)
              });
              ch.nack(msg, false, false);
            }
          }, { noAck: false });
        });

        clearTimeout(setupTimeout);
        span.end();
        resolve(channel);
      } catch (error) {
        clearTimeout(setupTimeout);
        const bsbError = error instanceof BSBError ? error : new BSBError(span.trace, String(error));
        span.error(bsbError);
        reject(bsbError);
      }
    });
  }

  private async ensureChannels(trace: DTrace): Promise<void> {
    const span = this.metrics.createSpan(trace, "ensureChannels", {});
    try {
      if (!this.eventsChannel) {
        this.eventsChannel = await this.setupChannel(span.trace, 'events');
      }
      if (!this.streamChannel) {
        this.streamChannel = await this.setupChannel(span.trace, 'stream');
      }
      span.end();
    } catch (error) {
      const bsbError = error instanceof BSBError ? error : new BSBError(span.trace, String(error));
      span.error(bsbError);
      throw bsbError;
    }
  }

  private async sendToQueue(trace: DTrace, queue: string, data: any, streamId: string): Promise<void> {
    const span = this.metrics.createSpan(trace, "sendToQueue", {
      queue,
      streamId
    });

    try {
      const content = Buffer.from(JSON.stringify(data));
      const sent = await this.streamChannel.channel.sendToQueue(
        queue,
        content,
        {
          correlationId: streamId,
          expiration: this.queueOpts.messageTtl,
          timestamp: Date.now(),
          persistent: true
        }
      );

      if (!sent) {
        throw new BSBError(span.trace, `Failed to send to queue: ${queue}`);
      }

      span.end();
    } catch (error) {
      const bsbError = error instanceof BSBError ? error : new BSBError(span.trace, String(error));
      span.error(bsbError);
      throw bsbError;
    }
  }

  private async streamToQueue(trace: DTrace, stream: Readable, streamId: string): Promise<void> {
    const span = this.metrics.createSpan(trace, "streamToQueue", { streamId });
    const streamQueue = LIB.getMyQueueKey(this.plugin, 'st', this.plugin.myId);
    let totalBytes = 0;

    return new Promise((resolve, reject) => {
      stream.on('data', async (chunk: Buffer) => {
        try {
          // Split large chunks into smaller ones
          for (let i = 0; i < chunk.length; i += this.MAX_CHUNK_SIZE) {
            const slice = chunk.slice(i, Math.min(i + this.MAX_CHUNK_SIZE, chunk.length));
            await this.sendToQueue(span.trace, streamQueue, {
              type: 'data',
              chunk: slice,
              offset: totalBytes
            }, streamId);
            totalBytes += slice.length;
          }
        } catch (error) {
          const bsbError = error instanceof BSBError ? error : new BSBError(span.trace, String(error));
          span.error(bsbError);
          reject(bsbError);
        }
      });

      stream.on('end', async () => {
        try {
          await this.sendToQueue(span.trace, streamQueue, {
            type: 'end',
            totalBytes
          }, streamId);
          span.end();
          resolve();
        } catch (error) {
          const bsbError = error instanceof BSBError ? error : new BSBError(span.trace, String(error));
          span.error(bsbError);
          reject(bsbError);
        }
      });

      stream.on('error', (error) => {
        const bsbError = error instanceof BSBError ? error : new BSBError(span.trace, String(error));
        span.error(bsbError);
        reject(bsbError);
      });
    });
  }

  async receiveStream(
    trace: DTrace,
    event: string,
    listener: { (etrace: DTrace, error: Error | null, stream: Readable): Promise<void> },
    timeoutSeconds: number = 60,
  ): Promise<string> {
    // Create span for receiving stream with setup function trace details
    const receiveSpan = this.metrics.createSpan(trace, "receiveStream:receive", {
      event,
      timeoutSeconds,
      functionTraceId: trace.t,
      functionSpanId: trace.s
    });

    await this.ensureChannels(receiveSpan.trace);

    const streamId = `${randomUUID()}=${timeoutSeconds}`;
    this.log.debug(receiveSpan.trace, "receiveStream: listening to {streamId}", {
      streamId,
    });

    const self = this;
    return new Promise((resolve) => {
      const receiptTimeoutHandler: NodeJS.Timeout = setTimeout(() => {
        const timeoutError = new BSBError(receiveSpan.trace, "Receive Receipt Timeout");
        receiveSpan.error(timeoutError);
        listener(receiveSpan.trace, timeoutError, null!);
        self.emit(`${streamId}-error`, receiveSpan.trace, timeoutError);
        self.removeAllListeners(streamId);
        receiveSpan.end();
      }, self.staticCommsTimeout);

      self.once(streamId, (ttrace: DTrace, stream: Readable): void => {
        clearTimeout(receiptTimeoutHandler);
        self.emit(`${streamId}-emit`);

        stream.on("error", (error: any) => {
          const errorObj = error instanceof Error ? error : new Error(error?.message || String(error));
          receiveSpan.error(errorObj);
          self.emit(`${streamId}-error`, errorObj);
        });

        stream.on("end", () => {
          self.emit(`${streamId}-end`);
          receiveSpan.end();
        });

        listener(receiveSpan.trace, null, stream);
      });

      resolve(streamId);
    });
  }

  async sendStream(
    trace: DTrace,
    event: string,
    streamId: string,
    stream: Readable,
  ): Promise<void> {
    // Create span for sending stream
    const sendSpan = this.metrics.createSpan(trace, "sendStream:send", {
      event,
      streamId
    });

    await this.ensureChannels(sendSpan.trace);

    this.log.debug(sendSpan.trace, "sendStream: emitting _self-{streamId}", { streamId });

    const self = this;
    return new Promise((resolve, rejectI) => {
      // Parse timeout with validation
      const timeoutStr = streamId.split("=")[1];
      const timeoutAS = Tools.isStringNumber(timeoutStr)
      let timeout = 60;
      if (timeoutAS.status && timeoutAS.value !== undefined) {
        timeout = timeoutAS.value;
      }
      
      const clearSessions = (e?: Error) => {
        stream.destroy(e);
        if (receiptTimeoutHandler !== null) {
          clearTimeout(receiptTimeoutHandler);
        }
        receiptTimeoutHandler = null;
        clearTimeout(timeoutHandler);
        self.removeAllListeners(`${streamId}-emit`);
        self.removeAllListeners(`${streamId}-end`);
        self.removeAllListeners(`${streamId}-error`);
        sendSpan.end();
      };

      const reject = (e: Error) => {
        clearSessions(e);
        sendSpan.error(e);
        rejectI(e);
      };

      let receiptTimeoutHandler: NodeJS.Timeout | null = setTimeout(() => {
        const timeoutError = new BSBError(sendSpan.trace, "Send Receipt Timeout");
        reject(timeoutError);
      }, self.staticCommsTimeout);

      const timeoutHandler = setTimeout(() => {
        const timeoutError = new BSBError(sendSpan.trace, "Stream Timeout");
        reject(timeoutError);
      }, timeout * 1000);

      self.once(`${streamId}-emit`, () => {
        if (receiptTimeoutHandler !== null) {
          clearTimeout(receiptTimeoutHandler);
        }
        receiptTimeoutHandler = null;
      });

      self.once(`${streamId}-end`, () => {
        clearSessions();
        resolve();
      });

      self.once(`${streamId}-error`, (error: Error) => reject(error));

      // Start streaming to RabbitMQ
      this.streamToQueue(sendSpan.trace, stream, streamId).catch(reject);

      self.emit(streamId, sendSpan.trace, stream);
    });
  }
}
