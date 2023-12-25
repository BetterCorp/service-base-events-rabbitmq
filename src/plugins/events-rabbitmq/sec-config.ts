import { BSBServiceConfig } from "@bettercorp/service-base";
import { z } from "zod";

export enum DataType {
  string = "string",
  number = "number",
  bigint = "bigint",
  boolean = "boolean",
  symbol = "symbol",
  undefined = "undefined",
  object = "object",
  function = "function",
  eventEmiiter = "eventEmiiter",
}

export const secSchema = z
  .object({
    platformKey: z
      .string()
      .nullable()
      .default(null)
      .describe(
        "If you want to run multiple bsb platforms on a single rabbitmq"
      ),
    fatalOnDisconnect: z
      .boolean()
      .default(true)
      .describe(
        "Disconnect on error: Cause the bsb service to exit code 1 if the connection drops"
      ),
    prefetch: z
      .number()
      .default(10)
      .describe("Prefetch: The RabbitMQ Prefetch amount"),
    endpoints: z
      .array(z.string())
      .default(["amqp://localhost"])
      .describe("Endpoints: The list of servers(cluster) to connect too"),
    credentials: z
      .object({
        username: z.string().default("guest").describe("Username"),
        password: z.string().default("guest").describe("Password"),
      })
      .default({}),
    uniqueId: z
      .string()
      .nullable()
      .default(null)
      .describe(
        "Unique Client ID: A static client Id - hostname is used when not set"
      ),
  })
  .default({});

export class Config extends BSBServiceConfig<typeof secSchema> {
  validationSchema = secSchema;

  migrate(
    toVersion: string,
    fromVersion: string | null,
    fromConfig: any | null
  ) {
    return fromConfig;
  }
}
