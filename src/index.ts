import { IEvents } from '@bettercorp/service-base/lib/ILib';
import { Events } from './plugins/events-rabbitmq/plugin';

let plugin: IEvents = new Events();
console.log('INIT!');
plugin.init({
  pluginName: 'core',
  cwd: 'TEST-CLIENT',
  getPluginConfig: () => {
    return {
      "endpoint": "amqp://172.16.3.109",
      "credentials": {
        "username": "guest",
        "password": "guest"
      }
    };
  },
  config: {
    debug: true
  },
  log: {
    debug: console.log,
    info: console.log,
    error: console.error,
    warn: console.warn
  }
} as any).then(async () => {
  plugin.onEvent('core', 'hello', 'world', (x) => { console.log(`RECEIVED: ${x}`); });
  plugin.onReturnableEvent('core', 'hello', 'world2', (resolve, reject, x) => {
    console.log('received event');
    console.log(x);
    //plugin.emitEvent('core', x.resultNames.plugin, x.resultNames.success, 'wdfgwdfgwdfgwfg');
    setTimeout(() => {
      resolve('THIS IS A ....REPLY: ' + new Date().getTime());
    }, 1000);
  });
  //as
  //plugin.emitEventAndReturn('core', 'hello', 'world2', 'THIS IS A ....TEST: ' + new Date().getTime()).then(console.warn);
  plugin.emitEvent('core', 'hello', 'world', { key: 'THIS IS A ....TEST: ' + new Date().getTime() });
  return;
  //while(true) await new Promise((resolve)=>{plugin.emitEvent('core', 'hello', 'world', 'THIS IS A ....TEST: ' + new Date().getTime());setTimeout(resolve, 10);});
  setTimeout(async () => {
    console.log('-------------CLEAR');
    //plugin.emitEventAndReturn('core', 'hello', 'world2', 'THIS IS A ....TEST: ' + new Date().getTime()).then(console.warn);
    while (true) await new Promise((resolve: any) => {
      /*plugin.emitEventAndReturn('core', 'hello', 'world2', 'THIS IS A ....TEST: ' +
        new Date().getTime()).then(console.warn); */
      plugin.emitEvent('core', 'hello', 'world', 'THIS IS A ....TEST: ' + new Date().getTime());
      //resolve();
      setTimeout(resolve, 1);
    });
  }, 1000);
}).catch(console.error);

