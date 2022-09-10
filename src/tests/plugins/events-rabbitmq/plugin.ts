import { IPluginLogger, LogMeta } from '@bettercorp/service-base';
import assert from "assert";
//import { Logger } from "./test-logger";
import { Events as events } from "../../../plugins/events-rabbitmq/plugin";
//import { emit } from "./events/emit";
//import { emitAndReturn } from "./events/emitAndReturn";
import { emitStreamAndReceiveStream } from "./events/emitStreamAndReceiveStream";

//const fakeCLogger = new Logger("test-plugin", process.cwd(), {} as any);
//const debug = console.log;
const debug = (...a: any) => {};
const fakeLogger: IPluginLogger = {
  reportStat: async (key, value): Promise<void> => {},
  info: async (message, meta, hasPIData): Promise<void> => {
    debug(message, meta);
  },
  warn: async (message, meta, hasPIData): Promise<void> => {
    debug(message, meta);
  },
  error: async (
    messageOrError: string | Error,
    meta?: LogMeta<any>,
    hasPIData?: boolean
  ): Promise<void> => {
    debug(messageOrError, meta);
    assert.fail(
      typeof messageOrError === "string"
        ? new Error(messageOrError)
        : messageOrError
    );
  },
  fatal: async (
    messageOrError: string | Error,
    meta?: LogMeta<any>,
    hasPIData?: boolean
  ): Promise<void> => {
    debug(messageOrError, meta);
    assert.fail(
      typeof messageOrError === "string"
        ? new Error(messageOrError)
        : messageOrError
    );
  },
  debug: async (message, meta, hasPIData): Promise<void> => {
    debug(message, meta);
  },
};

const getPluginConfig = async () => {
  return {
    "fatalOnDisconnect": true,
    "prefetch": 10,
    "endpoints": [
      "amqp://127.0.0.1:5672"
    ],
    "credentials": {
      "username": "guest",
      "password": "guest"
    }
  };
}

describe("plugins/events-default", () => {
  /*emit(async () => {
    const refP = new events("test-plugin", process.cwd(), fakeLogger);
    (refP as any).getPluginConfig = getPluginConfig;
    if (refP.init !== undefined) await refP.init();
    return refP;
  }, 50);
  emitAndReturn(async () => {
    const refP = new events("test-plugin", process.cwd(), fakeLogger);
    (refP as any).getPluginConfig = getPluginConfig;
    if (refP.init !== undefined) await refP.init();
    return refP;
  }, 50);*/
  emitStreamAndReceiveStream(async () => {
    const refP = new events("test-plugin", process.cwd(), fakeLogger);
    (refP as any).getPluginConfig = getPluginConfig;
    if (refP.init !== undefined) await refP.init();
    //refP.eas.staticCommsTimeout = 25;
    return refP;
  }, 2000);
});
