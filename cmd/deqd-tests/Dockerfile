FROM golang:alpine AS build

WORKDIR /go/src/gitlab.com/katchecode/deqd
RUN apk add --no-cache git ca-certificates

COPY vendor /go/src/
RUN cd /go/src && go install -v ./...

COPY pkg ./pkg
COPY api ./api
COPY cmd/deqd-tests ./cmd/deqd-tests
RUN CGO_ENABLED=0 go test ./cmd/deqd-tests -c -o tests


FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /go/src/gitlab.com/katchecode/deqd/tests /bin/tests
ENTRYPOINT ["/bin/tests"]
