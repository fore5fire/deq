
import Koa from 'koa';
import Router from 'koa-router';
import bodyparser from 'koa-bodyparser';
import { graphqlKoa } from 'apollo-server-koa';
import schema from './schema/index';
const router = new Router();

router.get('/graphql', graphqlKoa({ schema }));
router.post('/graphql', bodyparser(), graphqlKoa({ schema }));

const app = new Koa()
  .use(router.routes())
  .use(router.allowedMethods());

const port = process.env.PORT || 3000;
app.listen(port);
console.log(`Listening on port ${port}`);
