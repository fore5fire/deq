import Koa from './koa';
import { spawn } from 'child_process';

const config = {
  port: process.env.PORT || 3000,
};

const app = Koa();

const server = app.listen(config.port);
console.log(`Listening on port ${config.port}`);

if (process.argv[2]) {
  console.log('Running comand: ' + process.argv.slice(2).join(' '));
  const subProcess = spawn(process.argv[2], process.argv.slice(3), {
    stdio: 'inherit',
  });

  subProcess.on('exit', () => {
    server.close();
  });
}
