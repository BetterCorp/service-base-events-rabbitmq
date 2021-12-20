export interface IPluginConfig {
  prefetch: number;
  endpoint: string;
  credentials: IPluginConfig_Credentials;
}
export interface IPluginConfig_Credentials {
  username: string;
  password: string;
}

export default () => {
  return {
    prefetch: 2,
    endpoint: "amqp://localhost",
    credentials: {
      username: "guest",
      password: "guest"
    },
  };
}