import {Plugin} from "../../../plugins/events-rabbitmq/index";
import {BSBEventsRef} from "@bettercorp/service-base";
import {broadcast} from "@bettercorp/service-base/lib/tests/sb/plugins/events/broadcast";
import {getEventsConstructorConfig} from "@bettercorp/service-base/lib/tests/sb/plugins/events/plugin";
import {emit} from "@bettercorp/service-base/lib/tests/sb/plugins/events/emit";
import {emitAndReturn} from "@bettercorp/service-base/lib/tests/sb/plugins/events/emitAndReturn";
//import {emitStreamAndReceiveStream} from "@bettercorp/service-base/lib/tests/sb/plugins/events/emitStreamAndReceiveStream";
import {emitStreamAndReceiveStream} from "./events/emitStreamAndReceiveStream";


export const RunEventsPluginTests = (
    eventsPlugin: typeof BSBEventsRef,
    config: any = undefined,
) => {
  broadcast(async () => {
    const refP = new eventsPlugin(getEventsConstructorConfig(config));
    if (refP.init !== undefined) {
      await refP.init();
    }
    return refP;
  }, 30);
  emit(async () => {
    const refP = new eventsPlugin(getEventsConstructorConfig(config));
    if (refP.init !== undefined) {
      await refP.init();
    }
    return refP;
  }, 30);
  emitAndReturn(async () => {
    const refP = new eventsPlugin(getEventsConstructorConfig(config));
    if (refP.init !== undefined) {
      await refP.init();
    }
    return refP;
  }, 30);
  emitStreamAndReceiveStream(async () => {
    const refP = new eventsPlugin(getEventsConstructorConfig(config));
    if (refP.init !== undefined) {
      await refP.init();
    }
    //refP.eas.staticCommsTimeout = 25;
    return refP;
  },500);
};


describe("plugins/events-rabbitmq", () =>
    RunEventsPluginTests(Plugin, {
      platformKey: null,
      fatalOnDisconnect: false,
      prefetch: 10,
      endpoints: ["amqp://127.0.0.1:5670"],
      credentials: {
        username: "guest",
        password: "guest",
      },
    })
)
