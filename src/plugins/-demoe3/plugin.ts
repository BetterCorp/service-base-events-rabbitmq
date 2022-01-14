import { CPlugin } from "@bettercorp/service-base/lib/interfaces/plugins";
import { MyPluginConfig } from './sec.config';

import * as crypto from 'crypto';
import * as path from 'path';
import * as fs from 'fs';
import { exec } from 'child_process';
import { pipeline } from 'stream';
import { Tools } from '@bettercorp/tools/lib/Tools';
const getFileHash = (filename: string) => new Promise((resolve, reject) => {
  var fd = fs.createReadStream(filename);
  var hash = crypto.createHash('sha1');
  hash.setEncoding('hex');

  fd.on('error', reject);
  fd.on('end', () => {
    hash.end();
    resolve(hash.read());
  });

  // read all file and pipe it (write it) to the hash object
  fd.pipe(hash);
});

const runCMD = (cmd: string) => new Promise((resolve, reject) => {
  exec(cmd, (err: any, stdout: any, stderr: any) => {
    if (err) {
      // node couldn't execute the command
      return reject(err);
    }

    // the *entire* stdout and stderr (buffered)
    resolve({
      stdout,
      stderr
    });
  });
});

const convertBytes = (bytes: number, sizes = ["Bytes", "KB", "MB", "GB", "TB"]) => {
  if (bytes == 0) {
    return "n/a";
  }

  const i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)).toFixed(0));

  if (i == 0) {
    return bytes + " " + sizes[i];
  }

  return (bytes / Math.pow(1024, i)).toFixed(1) + " " + sizes[i];
};
export class Plugin extends CPlugin<MyPluginConfig> {
  init(): Promise<void> {
    const self = this;
    return new Promise((resolve) => {
      self.onReturnableEvent<any, void>(null, 'fakeFileDownload', async (data): Promise<void> => new Promise(async rr => {
        if (Tools.isNullOrUndefined(data)) self.log.fatal('INVALID DATA');
        console.log(data);
        let fileName = path.join(self.cwd, `./test-file-${ data.seshID }.in`);
        console.log(fs.existsSync(fileName));
        await self.sendStream(data.streamId, fs.createReadStream(fileName));
        rr();
      }));
      resolve();
    });
  }
  loaded(): Promise<void> {
    const self = this;
    return new Promise(async (resolve) => {
      const seshID = crypto.randomUUID();
      let fileName = path.join(self.cwd, `./test-file-${ seshID }.in`);
      let fileNameOut = path.join(self.cwd, `./test-file-${ seshID }.out`);
      if (fs.existsSync(fileNameOut))
        fs.unlinkSync(fileNameOut);
      if (fs.existsSync(fileNameOut))
        fs.unlinkSync(fileNameOut);
      await runCMD(`dd if=/dev/urandom of=${ fileName } bs=128MB count=4`);
      setTimeout(async () => {
        const now = new Date().getTime();
        let uuid = await self.receiveStream((err, stream) => new Promise((resolv: any): void => {
          //clearTimeout(emitTimeout);
          if (err) throw err;
          pipeline(stream, fs.createWriteStream(fileNameOut), async (err) => {
            if (err)
              console.error(err);
            resolv();
          });
        }));
        await self.emitEventAndReturn<any, void>(null, 'fakeFileDownload', {
          streamId: uuid,
          seshID
        }, 30);
        console.log('DONE');
        const srcFileHash = await getFileHash(fileName);
        const dstFileHash = await getFileHash(fileNameOut);
        console.error('HASH MATCH:', srcFileHash, dstFileHash, 'Validate data equals? ', (srcFileHash == dstFileHash));
        const done = new Date().getTime();
        const totalTimeMS = (done - now);
        const fileBytes = fs.statSync(fileName).size;
        const fullBytes = convertBytes(fileBytes);
        const bytesPerSecond = (fileBytes / (totalTimeMS / 1000));
        setTimeout(() => {
          if (fs.existsSync(fileName))
            fs.unlinkSync(fileName);
          if (fs.existsSync(fileNameOut))
            fs.unlinkSync(fileNameOut);
        }, 1000);
        console.log(` PLUGIN [${ uuid }] act size: ${ fullBytes } as ${ convertBytes(bytesPerSecond, ["Bps", "KBps", "MBps", "GBps", "TBps"]) } in ${ totalTimeMS }ms`);
      }, 2000);
      resolve();
    });
  }
}