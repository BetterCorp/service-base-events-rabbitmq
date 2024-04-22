import { RunEventsPluginTests } from "../../../../node_modules/@bettercorp/service-base/lib/tests";
import { Plugin } from "../../../plugins/events-rabbitmq/plugin";

describe("plugins/events-rabbitmq", () => {
  RunEventsPluginTests(Plugin, {
    platformKey: null,
    fatalOnDisconnect: false,
    prefetch: 10,
    endpoints: ["amqp://127.0.0.1:5672"],
    credentials: {
      username: "admin",
      password: "admin",
    },
  });
});
