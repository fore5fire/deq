#!/bin/bash

function cleanup {
  ! kill $LOGS_PID &>/dev/null
  ! docker kill deqd &>/dev/null
}
trap cleanup EXIT

set -e

echo building server
GOOS=linux CGO_ENABLED=0 go build -o build/deqd gitlab.com/katcheCode/deq/cmd/deqd
docker build --tag=deqd:local build

echo starting server
docker run -itd --rm --name=deqd -p 8080:8080 -e PORT=8080 deqd:local

docker logs deqd -f | sed -e 's/^/SERVER: /;' &
LOGS_PID=$!
disown

echo running tests
TEST_TARGET_URL=localhost:8080 DEQ_HOST=localhost:80 go test -count 1 gitlab.com/katcheCode/deq/cmd/deqd_tests | sed -e 's/^/TEST: /;'
! docker rm -f deqd &> /dev/null
