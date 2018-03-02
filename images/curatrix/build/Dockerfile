FROM amd64/node:9.5.0-alpine AS build

ENV NODE_ENV=build
WORKDIR /build

COPY package.json yarn.lock /build/
RUN yarn install

COPY . /build/
RUN $(yarn bin)/webpack --config build/webpack.config.js --env.production && \
    yarn install --production && \
    mkdir /app && \
    mv node_modules dist /app

FROM amd64/node:9.5.0-alpine

ENV NODE_ENV=production
WORKDIR /app

COPY --from=build /app /app

CMD ["node", "dist/bundle.js"]
