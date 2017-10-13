import { Server } from './server';
import { Health } from './health';
import { spawn } from 'child_process';

const config = {
  port: process.env.PORT || 3000,
  healthPort: process.env.HEALTH_PORT || 3333
};

const health = new Health({
  alive: () => true,
  ready: () => true,
}).listen(config.healthPort);


const app = new Server();

const server = app.listen(config.port);
console.log(`Listening on port ${config.port}`);

if (process.argv[2]) {
  console.log('Running comand: ' + process.argv.slice(2).join(' '));
  const subProcess = spawn(process.argv[2], process.argv.slice(3), {
    stdio: 'inherit',
  });

  subProcess.on('exit', () => {
    health.close();
    server.close();
  });
}
