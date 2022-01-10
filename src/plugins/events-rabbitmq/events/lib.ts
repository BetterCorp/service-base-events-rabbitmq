export class LIB {
  public static getQueueKey(channelKey: string, callerPluginName: string, pluginName: string | null, event: string) {
    return `${ channelKey }-${ pluginName || callerPluginName }-${ event }`;
  }
  public static getSpecialQueueKey(channelKey: string, id: string) {
    return `${ channelKey }-${ id }`;
  }
}