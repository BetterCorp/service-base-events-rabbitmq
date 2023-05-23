import { Tools } from "@bettercorp/tools/lib/Tools";
import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { Events } from "../plugin";

export interface SetupChannel<T extends string | null = string | null> {
  exchangeName: T;
  channel: amqplib.ChannelWrapper;
}
export class LIB {
  public static async getQueueKey(
    uSelf: Events,
    channelKey: string,
    callerPluginName: string,
    pluginName: string | null,
    event: string,
    addKey?: string
  ) {
    return `${await uSelf.getPlatformName(channelKey)}-${
      pluginName || callerPluginName
    }-${event}${Tools.isNullOrUndefined(addKey) ? "" : `-${addKey}`}`;
  }
  public static async getMyQueueKey(
    uSelf: Events,
    channelKey: string,
    id: string,
    addKey?: string
  ) {
    return `${await uSelf.getPlatformName(channelKey)}-${id}${
      Tools.isNullOrUndefined(addKey) ? "" : `-${addKey}`
    }`;
  }
  public static async setupChannel<T extends string | null>(
    uSelf: Events,
    connection: amqplib.AmqpConnectionManager,
    queueKey: string,
    exchangeName: T,
    exType?: string,
    exOpts?: amqplib.Options.AssertExchange,
    prefetch?: number,
    json: boolean = true
  ): Promise<SetupChannel<T>> {
    return new Promise(async (resolve) => {
      const exName =
        Tools.isNullOrUndefined(exchangeName) || Tools.isNullOrUndefined(exType)
          ? null
          : await uSelf.getPlatformName(exchangeName);
      let returned = false;
      uSelf.log.debug(`Create channel ({queueKey})`, { queueKey });
      const channel = await connection.createChannel({
        json,
        setup: async (ichannel: amqplibCore.ConfirmChannel) => {
          if (exName !== null)
            await ichannel.assertExchange(exName, exType!, exOpts);
          if (!Tools.isNullOrUndefined(prefetch)) {
            uSelf.log.debug(`prefetch ({queueKey}) {prefetch}`, {
              queueKey,
              prefetch: prefetch!,
            });
            await ichannel.prefetch(prefetch!);
          }
          uSelf.log.debug(`setup exchange ({queueKey}) OK`, {
            queueKey,
          });
          if (!returned) {
            resolve({
              exchangeName: exName as T,
              channel,
            });
            returned = true;
          }
        },
      });
      channel.on("close", () => {
        uSelf.log.warn(`AMQP channel ({queueKey}) close`, { queueKey });
      });
      channel.on("error", (err: any) => {
        uSelf.log.fatal(`AMQP channel ({queueKey}) error: {err}`, {
          queueKey,
          err: err.message || err,
        });
      });
      if (exName !== null)
        uSelf.log.debug(`Assert exchange ({queueKey}) {exName} {exType}`, {
          queueKey,
          exName,
          exType: exType!,
        });
      uSelf.log.debug(`Ready ({queueKey})`, { queueKey });
    });
  }
}
