import { Plugin } from "../../../plugins/events-rabbitmq/index";
import {
  RunEventsPluginTests,
} from "../../sb/plugins/events/index";

describe("plugins/events-rabbitmq", () => {
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
});