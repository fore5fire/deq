import { Server } from './server';
import { Health } from './health';
import { spawn } from 'child_process';
import mongoose from 'mongoose';

const { PORT = 3000, HEALTH_PORT = 3333 } = process.env;

mongoose.Promise = global.Promise;
mongoose.connect('mongodb://localhost/task-service', { useMongoClient: true });

const health = new Health({
  alive: () => true,
  ready: () => mongoose.connection.readyState === 1,
}).listen(HEALTH_PORT);


const app = new Server();

const server = app.listen(PORT);
console.log(`Listening on port ${PORT}`);

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
