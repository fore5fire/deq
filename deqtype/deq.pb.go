// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: deq.proto

package deqtype

import (
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion2 // please upgrade the proto package

// Topic events are published automatically the first time an event is published to a topic.
type Topic struct {
	// The name of the new topic.
	Topic string `protobuf:"bytes,1,opt,name=topic,proto3" json:"topic,omitempty"`
}

func (m *Topic) Reset()         { *m = Topic{} }
func (m *Topic) String() string { return proto.CompactTextString(m) }
func (*Topic) ProtoMessage()    {}
func (*Topic) Descriptor() ([]byte, []int) {
	return fileDescriptor_cc02b310faf1c402, []int{0}
}
func (m *Topic) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Topic) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Topic.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Topic) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Topic.Merge(m, src)
}
func (m *Topic) XXX_Size() int {
	return m.Size()
}
func (m *Topic) XXX_DiscardUnknown() {
	xxx_messageInfo_Topic.DiscardUnknown(m)
}

var xxx_messageInfo_Topic proto.InternalMessageInfo

func (m *Topic) GetTopic() string {
	if m != nil {
		return m.Topic
	}
	return ""
}

func init() {
	proto.RegisterType((*Topic)(nil), "deq.events.Topic")
}

func init() { proto.RegisterFile("deq.proto", fileDescriptor_cc02b310faf1c402) }

var fileDescriptor_cc02b310faf1c402 = []byte{
	// 111 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x4c, 0x49, 0x2d, 0xd4,
	0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0x02, 0x31, 0x53, 0xcb, 0x52, 0xf3, 0x4a, 0x8a, 0x95,
	0x64, 0xb9, 0x58, 0x43, 0xf2, 0x0b, 0x32, 0x93, 0x85, 0x44, 0xb8, 0x58, 0x4b, 0x40, 0x0c, 0x09,
	0x46, 0x05, 0x46, 0x0d, 0xce, 0x20, 0x08, 0xc7, 0x49, 0xf1, 0xc4, 0x23, 0x39, 0xc6, 0x0b, 0x8f,
	0xe4, 0x18, 0x1f, 0x3c, 0x92, 0x63, 0x9c, 0xf0, 0x58, 0x8e, 0xe1, 0xc2, 0x63, 0x39, 0x86, 0x1b,
	0x8f, 0xe5, 0x18, 0xa2, 0xd8, 0x53, 0x52, 0x0b, 0x4b, 0x2a, 0x0b, 0x52, 0x93, 0xd8, 0xc0, 0x86,
	0x1a, 0x03, 0x02, 0x00, 0x00, 0xff, 0xff, 0xf6, 0xc8, 0x34, 0x2c, 0x61, 0x00, 0x00, 0x00,
}

func (m *Topic) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Topic) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Topic) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintDeq(dAtA, i, uint64(len(m.Topic)))
		i += copy(dAtA[i:], m.Topic)
	}
	return i, nil
}

func encodeVarintDeq(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *Topic) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Topic)
	if l > 0 {
		n += 1 + l + sovDeq(uint64(l))
	}
	return n
}

func sovDeq(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozDeq(x uint64) (n int) {
	return sovDeq(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Topic) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowDeq
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Topic: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Topic: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Topic", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDeq
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthDeq
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthDeq
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Topic = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipDeq(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthDeq
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthDeq
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipDeq(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowDeq
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowDeq
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowDeq
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthDeq
			}
			iNdEx += length
			if iNdEx < 0 {
				return 0, ErrInvalidLengthDeq
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowDeq
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipDeq(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
				if iNdEx < 0 {
					return 0, ErrInvalidLengthDeq
				}
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthDeq = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowDeq   = fmt.Errorf("proto: integer overflow")
)
