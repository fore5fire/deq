#!/bin/bash

set -e
export CGO_ENABLED=0

echo building server
GOOS=linux go build -o build/deqd gitlab.com/katcheCode/deqd/cmd/deqd
docker build --tag=deqd:local build

! docker rm -f deqd &> /dev/null
echo starting server
docker run -itd --name=deqd -p 8080:8080 \
    -e PORT=8080 \
    deqd:local
sleep 3
echo running tests
TEST_TARGET_URL=localhost:8080 DEQ_HOST=localhost:80 GOCACHE=off go test gitlab.com/katcheCode/deqd/cmd/deqd_tests || docker logs deqd
! docker rm -f deqd &> /dev/null
