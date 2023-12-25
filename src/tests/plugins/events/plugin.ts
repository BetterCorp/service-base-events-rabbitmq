import { Plugin as events } from "../../../plugins/events-rabbitmq/plugin";
import { broadcast } from "./events/broadcast";
import { emit } from "./events/emit";
import { emitAndReturn } from "./events/emitAndReturn";
import { emitStreamAndReceiveStream } from "./events/emitStreamAndReceiveStream";
import {
  SBLogging,
  BSBEventsConstructor,
  SmartFunctionCallSync,
} from "@bettercorp/service-base";
import { readFileSync } from "fs";

const newSBLogging = () => {
  const sbLogging = new SBLogging(
    "test-app",
    "development",
    process.cwd(),
    {} as any
  );
  for (const logger of (sbLogging as any).loggers) {
    SmartFunctionCallSync(logger, logger.dispose);
  }
  (sbLogging as any).loggers = [];
  return sbLogging;
};
/*const generateNullLogging = () => {
  const sbLogging = newSBLogging();
  return new PluginLogger("development", "test-plugin", sbLogging);
};*/
const getPluginConfig = () => {
  return JSON.parse(
    readFileSync("./events-rabbitmq-plugin-config.json").toString()
  );
};
const getEventsConstructorConfig = (): BSBEventsConstructor => {
  return {
    appId: "test-app",
    pluginCwd: process.cwd(),
    cwd: process.cwd(),
    mode: "development",
    pluginName: "test-plugin",
    sbLogging: newSBLogging(),
    config: getPluginConfig(),
  };
};

describe("plugins/events-default", () => {
  broadcast(async () => {
    const refP = new events(getEventsConstructorConfig());
    if (refP.init !== undefined) await refP.init();
    return refP;
  }, 100);
  emit(async () => {
    const refP = new events(getEventsConstructorConfig());
    if (refP.init !== undefined) await refP.init();
    return refP;
  }, 100);
  emitAndReturn(async () => {
    const refP = new events(getEventsConstructorConfig());
    if (refP.init !== undefined) await refP.init();
    return refP;
  }, 100);
  emitStreamAndReceiveStream(async () => {
    const refP = new events(getEventsConstructorConfig());
    if (refP.init !== undefined) await refP.init();
    //refP.eas.staticCommsTimeout = 25;
    return refP;
  }, 500);
});
