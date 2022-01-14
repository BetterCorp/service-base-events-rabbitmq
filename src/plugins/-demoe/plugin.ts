import { CPlugin } from "@bettercorp/service-base/lib/interfaces/plugins";
import { MyPluginConfig } from './sec.config';

import * as crypto from 'crypto';
import * as path from 'path';
import * as fs from 'fs';
import { exec } from 'child_process';
import { pipeline } from 'stream';
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
  loaded(): Promise<void> {
    const self = this;
    return new Promise((resolve) => {
      setTimeout(async () => {
        const seshID = crypto.randomUUID();
        const runTest = async (size: string, count = 1) => {
          const now = new Date().getTime();
          //let fileName = '/mnt/sda1/themeforest-fpKIRGsM-metronic-responsive-admin-dashboard-template.zip';//path.join(self.cwd, `./test-file-${ size }`);
          let fileName = path.join(self.cwd, `./test-file-${ seshID }-${ size }`);
          if (fs.existsSync(fileName))
            fs.unlinkSync(fileName);
          //let fileNameOut = path.join(self.cwd, './outfile.zip');//fileName + '-out';
          let fileNameOut = fileName + '-out';
          if (fs.existsSync(fileNameOut))
            fs.unlinkSync(fileNameOut);
          try {
            await runCMD(`dd if=/dev/urandom of=${ fileName } bs=${ size } count=${ count }`);
            //fs.copyFileSync('/home/mitchellr/_repos/BetterCorp/better-service-base-events-rabbitmq/test/events2.js', fileName);
            /*fs.writeFileSync(fileName, 'XX');
            for (let x = 0; x < itr1; x++) {
              let fileBatch = crypto.randomUUID();
              for (let x = 0; x < itr2; x++)
                fileBatch += crypto.randomUUID();
              //console.log('batch:' + x)
              await new Promise((r, re) => fs.appendFile(fileName, fileBatch, (err) => ((err ? re : r)())));
            }*/
            const fileBytes = fs.statSync(fileName).size;
            const fullBytes = convertBytes(fileBytes);
            console.log(` ${ size } act size: ${ fullBytes }`);
            let srcFileHash =
              await getFileHash(fileName);
            /*const emitTimeout = setTimeout(() => {
              console.error('Event not received');
            }, 5000);*/
            //let uuid = '60-aaaaaa-aaaaa';
            let uuid = await self.receiveStream((err, stream) => new Promise((resolv: any): void => {
              //clearTimeout(emitTimeout);
              if (err) throw err;
              pipeline(stream, fs.createWriteStream(fileNameOut), (err) => {
                if (err)
                  console.error(err);
                resolv();
              });
            }), 60);
            await self.sendStream(uuid, fs.createReadStream(fileName));
            console.error('HASH MATCH:', await getFileHash(fileNameOut), srcFileHash, 'Validate data equals? ', (await getFileHash(fileNameOut) == srcFileHash));
            setTimeout(() => {
              if (fs.existsSync(fileName))
                fs.unlinkSync(fileName);
              if (fs.existsSync(fileNameOut))
                fs.unlinkSync(fileNameOut);
            }, 10000);
            const done = new Date().getTime();
            const totalTimeMS = (done - now);
            const bytesPerSecond = (fileBytes / (totalTimeMS / 1000));
            console.log(` ${ size } [${uuid}] act size: ${ fullBytes } as ${ convertBytes(bytesPerSecond, ["Bps", "KBps", "MBps", "GBps", "TBps"]) } in ${ totalTimeMS }ms`);
            return [size, (await getFileHash(fileNameOut) == srcFileHash), totalTimeMS];
          } catch (xx) {
            if (fs.existsSync(fileName))
              fs.unlinkSync(fileName);
            if (fs.existsSync(fileNameOut))
              fs.unlinkSync(fileNameOut);
            self.log.fatal(xx);
            return [size, 'ERR', 0];
          }
        };
        let results = [];
        results.push(await runTest('1KB'));
        //results.push(await runTest('1KB'));
        results.push(await runTest('16KB'));
        results.push(await runTest('128KB'));
        results.push(await runTest('512KB'));
        results.push(await runTest('1MB'));
        results.push(await runTest('16MB'));
        results.push(await runTest('128MB', 4));
        //results.push(await runTest('512MB', 16));
        //results.push(await runTest('1GB', 32));
        //await runTest('5GB', 160);
        //await runTest('12GB', 384);
        console.log(results);
        fs.writeFileSync(`./log-${ seshID }.txt`, JSON.stringify(results));
        /*setTimeout(() => {
          process.exit(0);
        }, 12000);*/
      }, 2000);
      resolve();
    });
  }
}