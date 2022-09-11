import { Tools } from "@bettercorp/tools/lib/Tools";
import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { Events } from "../plugin";

export class LIB {
  public static getQueueKey(
    channelKey: string,
    callerPluginName: string,
    pluginName: string | null,
    event: string,
    addKey?: string
  ) {
    return `${channelKey}-${pluginName || callerPluginName}-${event}${
      Tools.isNullOrUndefined(addKey) ? "" : `-${addKey}`
    }`;
  }
  public static getLocalKey(channelKey: string, event: string) {
    return `${channelKey}-${event}`;
  }
  public static getSpecialQueueKey(
    channelKey: string,
    id: string,
    addKey?: string
  ) {
    return `${channelKey}-${id}${
      Tools.isNullOrUndefined(addKey) ? "" : `-${addKey}`
    }`;
  }
  public static async setupChannel(
    uSelf: Events,
    connection: amqplib.AmqpConnectionManager,
    queueKey: string,
    exName: string,
    exType: string,
    exOpts: amqplib.Options.AssertExchange,
    prefetch?: number,
    json: boolean = true
  ): Promise<amqplib.ChannelWrapper> {
    return new Promise(async (resolve) => {
      let returned = false;
      uSelf.log.debug(`Create channel ({queueKey})`, { queueKey });
      const channel = await connection.createChannel({
        json,
        setup: async (ichannel: amqplibCore.ConfirmChannel) => {
          await ichannel.assertExchange(exName, exType, exOpts);
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
            resolve(channel);
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
      uSelf.log.debug(`Assert exchange ({queueKey}) {exName} {exType}`, {
        queueKey,
        exName,
        exType,
      });
      uSelf.log.debug(`Ready ({queueKey})`, { queueKey });
    });
  }
}
