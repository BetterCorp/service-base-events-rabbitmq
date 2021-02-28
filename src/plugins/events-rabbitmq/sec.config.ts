export default () => {
  return {
    endpoint: "amqp://localhost",
    credentials: {
      username: "guest",
      password: "guest"
    },
    noRandomDebugName: false,
    uniqueId: null
  };
}