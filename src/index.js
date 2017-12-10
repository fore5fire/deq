import { Health } from 'alive-and-well';
import mongoose from 'mongoose';
import Koa from 'koa';
import bodyparser from 'koa-bodyparser';
import jwt from 'jsonwebtoken';
import { ready } from './schema';
import { graphqlKoa } from 'apollo-server-koa';
import pino from 'pino';
import { formatError } from 'graphql-error-codes';
import { bootstrapAdmin } from './account';
import { auth } from './auth';

const {
  PORT = '8001',
  HEALTH_PORT,
  MONGODB_ENDPOINT = 'mongodb://localhost/users',
  LOG_LEVEL = 'info',
  JWT_PRIVATE_KEY_PATH = '',
  BOOTSTRAP_ADMIN_EMAIL,
  BOOTSTRAP_ADMIN_PASSWORD
} = process.env;

global.log = pino({ level: LOG_LEVEL, name: 'auth-service' });

log.debug("Starting up", { env: process.env });

mongoose.Promise = global.Promise;
mongoose.connect(MONGODB_ENDPOINT, { useMongoClient: true });
mongoose.connection.on('error', error => fail(error));

let health, healthServer, server;
if (HEALTH_PORT) {
  health = new Health();
  healthServer = health.listen(HEALTH_PORT);
  log.info(`Health listening on port ${HEALTH_PORT}`);
}

ready().then(async schema => {

  log.debug({ msg: "Schema is ready", schema });

  if (BOOTSTRAP_ADMIN_EMAIL && BOOTSTRAP_ADMIN_PASSWORD) {
    await bootstrapAdmin(BOOTSTRAP_ADMIN_EMAIL, BOOTSTRAP_ADMIN_PASSWORD);
  }

  server = new Koa()
    .use((ctx, next) => {
      log.debug("recieved request", { headers: ctx.headers });
      if (ctx.headers.authorization) {
        const authHeader = ctx.headers.authorization.split(/\s+/).filter(x => x);
        if (authHeader.length === 2 && authHeader[0].toLowerCase() === 'bearer') {
          ctx.userToken = authHeader[1];
          ctx.user = jwt.decode(ctx.userToken);
        }
      }
      return next();
    })
    .use((ctx, next) => {
      ctx.aid = ctx.user?.aid;
      ctx.user = auth(ctx.user);
      return next();
    })
    .use(bodyparser())
    .use(graphqlKoa(ctx => ({
      schema,
      context: { secretKeyPath: JWT_PRIVATE_KEY_PATH, user: ctx.user, aid: ctx.aid },
      logFunction: arg => log.debug(arg),
      formatError: e => {
        return formatError({ logger: e => log.error(e), });
      },
      debug: LOG_LEVEL === 'debug',
    })));

  server.listen(PORT);
  log.info(`Listening on port ${PORT}`);

  health.ready = () => mongoose.connection.readyState == 1;

}).catch(error => {
  fail(error);
});


function fail(error) {
  log.fatal(error);
  server?.close();
  healthServer?.close();
  process.exit(1);
}


// try {
//   context.token = jwt.verify(token, 'secretKey', {
//     algorithms: ["HS256"],
//   });
//   return context.token;
// }
