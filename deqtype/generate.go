//go:generate protoc --plugin=./protoc-gen-deq --deq_out=Many.proto=github.com/gogo/protobuf/types:. any.proto
//go:generate protoc --plugin=./protoc-gen-deq --deq_out=Mapi.proto=github.com/gogo/protobuf/types:. api.proto
//go:generate protoc --plugin=./protoc-gen-deq --deq_out=Mduration.proto=github.com/gogo/protobuf/types:. duration.proto
//go:generate protoc --plugin=./protoc-gen-deq --deq_out=Mdescriptor.proto=github.com/gogo/protobuf/protoc-gen-gogo/descriptor:. descriptor.proto
//go:generate protoc --plugin=./protoc-gen-deq --deq_out=Mempty.proto=github.com/gogo/protobuf/types:. empty.proto
//go:generate protoc --plugin=./protoc-gen-deq --deq_out=Mfield_mask.proto=github.com/gogo/protobuf/types:. field_mask.proto
//go:generate protoc --plugin=./protoc-gen-deq --deq_out=Mtimestamp.proto=github.com/gogo/protobuf/types:. timestamp.proto
//go:generate protoc --plugin=./protoc-gen-deq --deq_out=Mtype.proto=github.com/gogo/protobuf/types:. type.proto
//go:generate protoc --plugin=./protoc-gen-deq --deq_out=Mwrappers.proto=github.com/gogo/protobuf/types:. wrappers.proto
//go:generate protoc --plugin=./protoc-gen-deq --plugin=./protoc-gen-gogofaster --gogofaster_out=. --deq_out=. deq.proto

package deqtype
