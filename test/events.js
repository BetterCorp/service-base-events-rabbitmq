const events = require('../lib/plugins/events-rabbitmq/plugin').Events;

const logger_Def = require("../node_modules/@bettercorp/service-base/lib/logger/logger").Logger;
const testogger = require('../node_modules/@bettercorp/service-base/test/virt-clientLogger').testogger;
const emit = require('../node_modules/@bettercorp/service-base/test/events/emit').default;
const emitAndReturn = require('../node_modules/@bettercorp/service-base/test/events/emitAndReturn').default;
//const emitStreamAndReceiveStream = require('../node_modules/@bettercorp/service-base/test/events/emitStreamAndReceiveStream').default;
const devConfig = JSON.parse(require('fs').readFileSync(require('path').join(process.cwd(),'./sec.config.json').toString()));
const testPluginName = 'events-rabbitmq';
const appConfig = {
  runningInDebug: false,
  getPluginConfig: (pluginName) => {
    return devConfig.plugins[pluginName];
  }
};

const fakeLogger = new testogger('test-logger', process.cwd(),
  new logger_Def('def-logger', process.cwd(), null, appConfig), null, {
    error: (e) => assert.fail(new Error(e)),
    fatal: (e) => assert.fail(new Error(e))
  });
//const fakeLogger = new logger_Def('def-logger', process.cwd(), null, appConfig);

describe('Events', () => {
  emit(async () => {
    const refP = new events(testPluginName, process.cwd(), fakeLogger, appConfig);
    if (refP.init !== undefined)
      await refP.init();
    return refP;
  }, 20);
  emitAndReturn(async () => {
    const refP = new events(testPluginName, process.cwd(), fakeLogger, appConfig);
    if (refP.init !== undefined)
      await refP.init();
    return refP;
  }, 100);
  /*emitStreamAndReceiveStream(async () => {
    const refP = new events(testPluginName, process.cwd(), fakeLogger, appConfig);
    if (refP.init !== undefined)
      await refP.init();
    refP.eas.staticCommsTimeout = 25;
    return refP;
  }, 50);*/
});