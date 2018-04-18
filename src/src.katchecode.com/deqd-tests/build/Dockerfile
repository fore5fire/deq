FROM golang:alpine AS build

WORKDIR /go/src/src.katchecode.com/deqd-tests/
RUN apk add --no-cache git ca-certificates

COPY vendor /go/src/
RUN cd /go/src && go install -v ./...
COPY ./ ./
RUN rm -R vendor && CGO_ENABLED=0 go test -c -o tests


FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /go/src/src.katchecode.com/deqd-tests/tests /bin/tests
ENTRYPOINT ["/bin/tests"]
