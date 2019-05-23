//go:generate protoc --deq_out=Many/any.proto=github.com/gogo/protobuf/types:. any/any.proto
//go:generate protoc --deq_out=Mapi/api.proto=github.com/gogo/protobuf/types:. api/api.proto
//go:generate protoc --deq_out=Mduration/duration.proto=github.com/gogo/protobuf/types:. duration/duration.proto
//go:generate protoc --deq_out=Mdescriptor/descriptor.proto=github.com/gogo/protobuf/types:. descriptor/descriptor.proto
//go:generate protoc --deq_out=Mempty/empty.proto=github.com/gogo/protobuf/types:. empty/empty.proto
//go:generate protoc --deq_out=Mfield_mask/field_mask.proto=github.com/gogo/protobuf/types:. field_mask/field_mask.proto
//go:generate protoc --deq_out=Mtimestamp/timestamp.proto=github.com/gogo/protobuf/types:. timestamp/timestamp.proto
//go:generate protoc --deq_out=Mptype/type.proto=github.com/gogo/protobuf/types:. ptype/type.proto
//go:generate protoc --deq_out=Mwrappers/wrappers.proto=github.com/gogo/protobuf/types:. wrappers/wrappers.proto

package dtypes
