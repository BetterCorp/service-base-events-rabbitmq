import { CPlugin } from "@bettercorp/service-base/lib/interfaces/plugins";
import { MyPluginConfig } from './sec.config';

export class Plugin extends CPlugin<MyPluginConfig> {
  loaded(): Promise<void> {
    const self = this;
    return new Promise((resolve) => {
      setTimeout(async () => {
        await self.onReturnableEvent('test', 'test', (resolve, reject, data) => {
          setTimeout(() => {
            console.log('Received onEvent');
          }, 1);
          resolve(1);
        });
        console.log('!!Received onEvent');
        const resp = await self.emitEventAndReturn('test', 'test', {}, 10);
        console.log(resp);
      }, 2000);
      resolve();
    });
  }
}