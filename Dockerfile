FROM node:alpine AS build

ENV NODE_ENV=build

RUN apk update && apk add python make g++

COPY package.json yarn.lock /build/
RUN cd /build && yarn install

COPY . /build/
RUN cd /build && yarn build

FROM node:alpine

ENV NODE_ENV=production

RUN apk update && apk add python make g++

COPY package.json yarn.lock /app/
WORKDIR /app
RUN yarn install --production && npm rebuild bcrypt --build-from-source

COPY tests /app/tests
COPY --from=build /build/dist /app/dist

CMD ["npm","start"]
