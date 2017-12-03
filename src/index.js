import { Health } from 'alive-and-well';
import mongoose from 'mongoose';
import Koa from 'koa';
import bodyparser from 'koa-bodyparser';
import jwt from 'jsonwebtoken';
import { ready } from './schema';
import { graphqlKoa } from 'apollo-server-koa';
import pino from 'pino';

const {
  PORT = 3000,
  HEALTH_PORT,
  MONGODB_ENDPOINT = 'mongodb://localhost/users',
  LOG_LEVEL = 'info',
  JWT_PRIVATE_KEY_PATH,
} = process.env;

global.log = pino({ level: LOG_LEVEL, name: 'auth-service' });


mongoose.Promise = global.Promise;
mongoose.connect(MONGODB_ENDPOINT, { useMongoClient: true });

let isReady = false;

let health;
if (HEALTH_PORT) {
  health = new Health({
    ready: () => isReady
  }).listen(HEALTH_PORT);
  log.info(`Health listening on port ${HEALTH_PORT}`);
}

let server;
ready().then(schema => {
  server = new Koa()
    .use((ctx, next) => {
      log.debug("recieved request", { headers: ctx.headers });
      if (ctx.headers.authorization) {
        const authHeader = ctx.headers.authorization.split(/\s+/).filter(x => x);
        if (authHeader.length === 2 && authHeader[0].toLowerCase() === 'bearer') {
          ctx.user = jwt.decode(authHeader[1]);
        }
      }
      return next();
    })
    .use(bodyparser())
    .use(graphqlKoa(ctx => ({
      schema,
      context: { secretKeyPath: JWT_PRIVATE_KEY_PATH, user: ctx.user },
      logFunction: arg => log.debug(arg)
    })));

  server.listen(PORT);
  log.info(`Listening on port ${PORT}`);

  isReady = true;

}).catch(error => {
  log.fatal(error);
  server?.stop();
  health?.stop();
  process.exit(1);
});



// try {
//   context.token = jwt.verify(token, 'secretKey', {
//     algorithms: ["HS256"],
//   });
//   return context.token;
// }
