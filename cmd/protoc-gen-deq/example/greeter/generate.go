//go:generate protoc --gogofaster_out=.  ./greeter.proto ./greeter2.proto
//go:generate protoc --plugin=. --deq_out=.  ./greeter.proto ./greeter2.proto

package greeter
