import { Koa } from './koa';

const config = {
  port: process.env.PORT || 3000,
};

const app = Koa.create();

app.listen(config.port);
console.log(`Listening on port ${config.port}`);
