import Koa from 'koa';
import bodyparser from 'koa-bodyparser';
import { graphqlKoa } from 'apollo-server-koa';
import schema from './schema/index';

export class Server extends Koa {
  constructor(...args) {
    super(...args);
    this
      .use(bodyparser())
      .use(graphqlKoa({ schema }));
  }
}
