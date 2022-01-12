import { Tools } from '@bettercorp/tools/lib/Tools';
import * as amqplib from 'amqplib';
import { Events } from '../plugin';

export class LIB {
  public static getQueueKey(channelKey: string, callerPluginName: string, pluginName: string | null, event: string, addKey?: string) {
    return `${ channelKey }-${ pluginName || callerPluginName }-${ event }${ Tools.isNullOrUndefined(addKey) ? '' : `-${ addKey }` }`;
  }
  public static getLocalKey(channelKey: string, event: string) {
    return `${ channelKey }-${ event }`;
  }
  public static getSpecialQueueKey(channelKey: string, id: string, addKey?: string) {
    return `${ channelKey }-${ id }${ Tools.isNullOrUndefined(addKey) ? '' : `-${ addKey }` }`;
  }
  public static async setupChannel(uSelf: Events, connection: amqplib.Connection, queueKey: string, exName: string, exType: string, exOpts: amqplib.Options.AssertExchange, prefetch?: number) {
    uSelf.log.info(`Create channel (${ queueKey })`);
    const channel = await connection.createChannel();
    channel.on("close", () => {
      uSelf.log.error(`AMQP channel (${ queueKey }) close`);
      uSelf.log.fatal(`AMQP Error: channel (${ queueKey }) close`);
    });
    channel.on("error", (err: any) => {
      uSelf.log.error(`AMQP channel (${ queueKey }) error`, err);
      uSelf.log.fatal(`AMQP Error: channel (${ queueKey }) error`, err);
    });
    uSelf.log.info(`Assert exchange (${ queueKey }) ${ exName } ${ exType }`);
    await channel.assertExchange(exName, exType, exOpts);
    if (!Tools.isNullOrUndefined(prefetch)) {
      uSelf.log.info(`prefetch (${ queueKey }) ${ prefetch }`);
      await channel.prefetch(prefetch!);
    }
    uSelf.log.info(`Ready (${ queueKey })`);

    return channel;
  }
}