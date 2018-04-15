FROM golang:alpine AS build

WORKDIR /go/src/src.katchecode.com/event-store/
RUN apk add --no-cache git ca-certificates && \
    wget -qO /usr/local/bin/dep https://github.com/golang/dep/releases/download/v0.4.1/dep-linux-amd64 && \
    chmod +x /usr/local/bin/dep

COPY Gopkg.lock Gopkg.toml ./
RUN dep ensure -vendor-only
COPY ./ ./
RUN CGO_ENABLED=0 go build -o app


FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /go/src/src.katchecode.com/event-store/app /bin/app
ENTRYPOINT ["/bin/app"]
