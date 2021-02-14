import { IEvents } from '@bettercorp/service-base/lib/ILib';
import { Events } from './plugins/events-rabbitmq/plugin';

let plugin: IEvents = new Events();
console.log('INIT!');
plugin.init({
  getPluginConfig: () => {
    return {
      endpoint: 'amqp://localhost',
      exchange: 'amq.direct',
      credentials: {
        username: 'admin',
        password: 'admin'
      }
    };
  },
  log: {
    debug: console.log,
    info: console.log,
    error: console.error,
    warn: console.warn
  }
} as any).then(async () => {
  plugin.onEvent('core', 'hello', 'world', (console.log));
  plugin.onEvent('core', 'hello', 'world2', x => {
    console.log('received event');
    console.log(x);
    //plugin.emitEvent('core', x.resultNames.plugin, x.resultNames.success, 'wdfgwdfgwdfgwfg');
    setTimeout(() => {
      plugin.emitEvent('core', x.resultNames.success, x.resultKey, 'THIS IS A ....REPLY: ' + new Date().getTime());
    }, 1000);
  });
  //as

  //while(true) await new Promise((resolve)=>{plugin.emitEvent('core', 'hello', 'world', 'THIS IS A ....TEST: ' + new Date().getTime());setTimeout(resolve, 10);});
  setTimeout(async () => {
    console.log('-------------CLEAR');
    plugin.emitEventAndReturn('core', 'hello', 'world2', 'THIS IS A ....TEST: ' + new Date().getTime()).then(console.warn);
    while (true) await new Promise((resolve) => { plugin.emitEventAndReturn('core', 'hello', 'world2', 'THIS IS A ....TEST: ' + 
      new Date().getTime()).then(console.warn); setTimeout(resolve, 500); });
  }, 1000);
}).catch(console.error);

