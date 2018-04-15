FROM golang:alpine AS build

WORKDIR /go/src/src.katchecode.com/event-store-tests/
RUN apk add --no-cache git && \
    wget -qO /usr/local/bin/dep https://github.com/golang/dep/releases/download/v0.4.1/dep-linux-amd64 && \
    chmod +x /usr/local/bin/dep
    
COPY Gopkg.lock Gopkg.toml ./
RUN dep ensure -vendor-only
COPY ./ ./
RUN CGO_ENABLED=0 go test -c -o tests


FROM scratch

COPY --from=build /go/src/src.katchecode.com/event-store-tests/tests /bin/tests
ENTRYPOINT ["/bin/tests"]
