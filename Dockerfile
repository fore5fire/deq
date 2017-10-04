FROM node:alpine AS build

COPY package.json /build/package.json
RUN cd /build && npm install -q

COPY . /build/
RUN cd /build && npm run build-production -q && npm run test

FROM node:alpine AS package

COPY --from=build /build/dist/* /app/dist/
COPY package.json ./package.json

ENV port=8080
EXPOSE 8080

CMD ["npm","start"]
