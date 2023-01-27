import { SecConfig } from "@bettercorp/service-base/lib/interfaces/serviceConfig";
import { Tools } from "@bettercorp/tools/lib/Tools";

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

export interface PluginConfig {
  fatalOnDisconnect: boolean; // Disconnect on error: Cause the bsb service to exit code 1 if the connection drops
  prefetch: number; // Prefetch: The RabbitMQ Prefetch amount
  endpoints: Array<string>; // Endpoints: The list of servers(cluster) to connect too
  credentials: IPluginConfig_Credentials; // Credentials for server authorization
  uniqueId?: string; // Unique Client ID: A static client Id - hostname is used when not set
}
export interface IPluginConfig_Credentials {
  username: string; // Username
  password: string; // Password
}

export class Config extends SecConfig<PluginConfig> {
  migrate(
    mappedPluginName: string,
    existingConfig: PluginConfig
  ): PluginConfig {
    const existAsAny = existingConfig as any;
    if (Tools.isObject(existAsAny) && Tools.isString(existAsAny.endpoint)) {
      existingConfig.endpoints = [(existingConfig as any).endpoint as string];
    }
    let resultConfig: PluginConfig = {
      fatalOnDisconnect: Tools.isBoolean(existingConfig.fatalOnDisconnect)
        ? existingConfig.fatalOnDisconnect
        : true,
      prefetch: Tools.isNumber(existingConfig.prefetch)
        ? existingConfig.prefetch
        : 10,
      endpoints: Tools.isArray(existingConfig.endpoints)
        ? existingConfig.endpoints
        : ["amqp://localhost"],
      credentials: existingConfig.credentials ?? {},
      uniqueId: existingConfig.uniqueId,
    };

    if (
      resultConfig.endpoints.length === 0 ||
      resultConfig.endpoints.filter((x) => !Tools.isString(x)).length > 0
    ) {
      resultConfig.endpoints = ["amqp://localhost"];
    }
    if (resultConfig.endpoints.length === 1) {
      resultConfig.fatalOnDisconnect = true;
    }
    resultConfig.credentials.username =
      (resultConfig.credentials ?? {}).username || "guest";
    resultConfig.credentials.password =
      (resultConfig.credentials ?? {}).password || "guest";
    return resultConfig;
  }
}
