import { Tools } from "@bettercorp/tools/lib/Tools";
import * as amqplib from "amqp-connection-manager";
import * as amqplibCore from "amqplib";
import { Plugin } from "../index";
import { DTrace, IPluginLogging } from "@bettercorp/service-base";

export interface SetupChannel<T extends string | null = string | null> {
  exchangeName: T;
  channel: amqplib.ChannelWrapper;
}
export class LIB {
  public static getQueueKey(
    plugin: Plugin,
    channelKey: string,
    pluginName: string,
    event: string,
    addKey?: string
  ) {
    return `${ plugin.getPlatformName(channelKey) }-${ pluginName }-${ event }${ Tools.isNullOrUndefined(addKey) ? "" : `-${ addKey }`
      }`;
  }
  public static getMyQueueKey(
    plugin: Plugin,
    channelKey: string,
    id: string,
    addKey?: string
  ) {
    return `${ plugin.getPlatformName(channelKey) }-${ id }${ Tools.isNullOrUndefined(addKey) ? "" : `-${ addKey }`
      }`;
  }
  public static async setupChannel<T extends string | null>(
    trace: DTrace,
    plugin: Plugin,
    log: IPluginLogging,
    connection: amqplib.AmqpConnectionManager,
    queueKey: string,
    exchangeName: T,
    exType?: string,
    exOpts?: amqplib.Options.AssertExchange,
    prefetch?: number,
    json: boolean = true
  ): Promise<SetupChannel<T>> {
    // eslint-disable-next-line no-async-promise-executor
    return new Promise(async (resolve) => {
      const exName =
        Tools.isNullOrUndefined(exchangeName) || Tools.isNullOrUndefined(exType)
          ? null
          : plugin.getPlatformName(exchangeName);
      let returned = false;
      log.debug(trace, `Create channel ({queueKey})`, { queueKey });
      const channel = await connection.createChannel({
        json,
        setup: async (ichannel: amqplibCore.ConfirmChannel) => {
          if (exName !== null)
            await ichannel.assertExchange(exName, exType!, exOpts);
          if (!Tools.isNullOrUndefined(prefetch)) {
            log.debug(trace, `prefetch ({queueKey}) {prefetch}`, {
              queueKey,
              prefetch: prefetch!,
            });
            await ichannel.prefetch(prefetch!);
          }
          log.debug(trace, `setup exchange ({queueKey}) OK`, {
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
        log.warn(trace, `AMQP channel ({queueKey}) close`, { queueKey });
      });
      channel.on("error", (err: any) => {
        log.error(trace, `AMQP channel ({queueKey}) error: {err}`, {
          queueKey,
          err: err.message || err,
        });
        process.exit(6);
      });
      if (exName !== null)
        log.debug(trace, `Assert exchange ({queueKey}) {exName} {exType}`, {
          queueKey,
          exName,
          exType: exType!,
        });
      log.debug(trace, `Ready ({queueKey})`, { queueKey });
    });
  }
}
