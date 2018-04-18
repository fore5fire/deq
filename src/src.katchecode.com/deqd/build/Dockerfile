FROM golang:alpine AS build

WORKDIR /go/src/src.katchecode.com/deqd/
RUN apk add --no-cache git ca-certificates

# COPY Gopkg.lock Gopkg.toml ./
# RUN dep ensure -vendor-only && mv -r vendor /go/src
COPY vendor /go/src/
RUN cd /go/src && go install -v ./...
COPY ./ ./
RUN rm -R vendor && CGO_ENABLED=0 go build -o app


FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /go/src/src.katchecode.com/deqd/app /bin/app
ENTRYPOINT ["/bin/app"]
