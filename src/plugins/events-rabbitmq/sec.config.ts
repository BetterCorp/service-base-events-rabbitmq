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
  prefetch: number;
  endpoint: string;
  credentials: IPluginConfig_Credentials;
  uniqueId: string | null;
}
export interface IPluginConfig_Credentials {
  username: string;
  password: string;
}

export default () => {
  return {
    prefetch: 10,
    endpoint: "amqp://localhost",
    credentials: {
      username: "guest",
      password: "guest"
    },
    uniqueId: null
  };
}