//go:generate protoc --go_out=plugins=grpc:./ --go_out=plugins=grpc:../../../../event-store-tests/eventstore/ ./eventstore.proto

package eventstore
