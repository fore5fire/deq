import Koa from 'koa';
import bodyparser from 'koa-bodyparser';
import { graphqlKoa } from 'apollo-server-koa';
import schema from './schema/index';

export default function () {
  return new Koa()
    .use(bodyparser())
    .use(graphqlKoa({ schema }));
}
