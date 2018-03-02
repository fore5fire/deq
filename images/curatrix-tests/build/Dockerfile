FROM amd64/node:9.5.0-alpine AS build

ENV NODE_ENV=build
WORKDIR /build

COPY package.json yarn.lock /build/
RUN yarn install

COPY . /build/
RUN yarn install

CMD ["node_modules/.bin/mocha", "--recursive", "src/", "--require", "babel-register", "--require", "source-map-support"]
