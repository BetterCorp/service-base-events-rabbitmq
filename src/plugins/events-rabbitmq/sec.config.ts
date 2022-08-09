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

export interface IPluginConfig {
  fatalOnDisconnect: boolean; // Disconnect on error: Cause the bsb service to exit code 1 if the connection drops
  prefetch: number; // Prefetch: The RabbitMQ Prefetch amount
  endpoint?: string; // Endpoint: The server endpoint - or use endpoints for a cluster
  endpoints?: Array<string>; // Endpoints: The list of servers(cluster) to connect too - use Endpoint for a single server
  credentials: IPluginConfig_Credentials; // Credentials for server authorization
  uniqueId?: string; // Unique Client ID: A static client Id - hostname is used when not set
}
export interface IPluginConfig_Credentials {
  username: string; // Username
  password: string; // Password
}

export default (): IPluginConfig => {
  return {
    fatalOnDisconnect: true,
    prefetch: 10,
    //endpoint: "amqp://localhost",
    endpoints: ["amqp://localhost"],
    credentials: {
      username: "guest",
      password: "guest",
    },
    uniqueId: undefined,
  };
};
