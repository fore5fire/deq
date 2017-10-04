import fetch from 'node-fetch';
import { spawn } from 'child_process';

global.request = (query) => fetch(global.config.endpoint, { method: 'POST', body: query });
global.config = {
  endpoint: 'http://localhost:3000/graphql',
};

before(function() {
  this.serverProcess = spawn('npm', ['start']);
  console.log(this.serverProcess);
});

after(function() {
  this.serverProcess.kill();
});
