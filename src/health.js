
import Koa from 'koa';

export class Health extends Koa {

  constructor({ alive, ready }, options) {
    super(options);

    if (typeof alive !== 'function' || typeof ready !== 'function') {
      throw new Error("Both alive and ready handlers must be defined");
    }

    this.use(async (ctx, next) => {
      if (ctx.path === '/liveness') {
        ctx.status = alive() ? 200 : 503;
      }
      else if (ctx.path === '/readiness') {
        ctx.status = ready() ? 200 : 503;
      }
      else {
        ctx.status = 404;
        return;
      }

      await next();
    });
  }
};
