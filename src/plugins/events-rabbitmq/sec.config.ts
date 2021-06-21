export interface IPluginConfig {
  prefetch: number;
  prefetchEAR: number;
  endpoint: string;
  credentials: IPluginConfig_Credentials;
  noRandomDebugName: boolean;
  uniqueId: string | null;
}
export interface IPluginConfig_Credentials {
  username: string;
  password: string;
}

export default () => {
  return {
    prefetch: 10,
    prefetchEAR: 10,
    endpoint: "amqp://localhost",
    credentials: {
      username: "guest",
      password: "guest"
    },
    noRandomDebugName: false,
    uniqueId: null
  };
}