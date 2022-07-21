export enum DataType {
  string = 'string',
  number = 'number',
  bigint = 'bigint',
  boolean = 'boolean',
  symbol = 'symbol',
  undefined = 'undefined',
  object = 'object',
  function = 'function',
  eventEmiiter = 'eventEmiiter'
}

export interface IPluginConfig {
  fatalOnDisconnect: boolean;
  prefetch: number;
  endpoint?: string;
  endpoints?: Array<string>;
  credentials: IPluginConfig_Credentials;
  uniqueId: string | null;
}
export interface IPluginConfig_Credentials {
  username: string;
  password: string;
}

export default () => {
  return {
    fatalOnDisconnect: true,
    prefetch: 10,
    //endpoint: "amqp://localhost",
    endpoints: ["amqp://localhost"],
    credentials: {
      username: "guest",
      password: "guest"
    },
    uniqueId: null
  };
}