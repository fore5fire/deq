//go:generate protoc --deq_out=Many.proto=github.com/gogo/protobuf/types:. any.proto
//go:generate protoc --deq_out=Mapi.proto=google.golang.org/genproto/protobuf/api:. api.proto
//go:generate protoc --deq_out=Mduration.proto=github.com/gogo/protobuf/types:. duration.proto
//go:generate protoc --deq_out=Mdescriptor.proto=github.com/gogo/protobuf/protoc-gen-gogo/descriptor:. descriptor.proto
//go:generate protoc --deq_out=Mempty.proto=github.com/gogo/protobuf/types:. empty.proto
//go:generate protoc --deq_out=Mfield_mask.proto=github.com/gogo/protobuf/types:. field_mask.proto
//go:generate protoc --deq_out=Mtimestamp.proto=github.com/gogo/protobuf/types:. timestamp.proto
//go:generate protoc --deq_out=Mtype.proto=github.com/gogo/protobuf/types:. type.proto
//go:generate protoc --deq_out=Mwrappers.proto=github.com/gogo/protobuf/types:. wrappers.proto

package deqtype
