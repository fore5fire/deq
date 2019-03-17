//go:generate protoc -I=. -I=../../../ -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogofaster_out=plugins=grpc,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mapi/v1/deq/deq.proto=gitlab.com/katcheCode/deq/api/v1/deq,Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types:. data.proto

// Package data provides keys and payloads for data stored in DEQ's internal key-value store.
package data

import (
	"bytes"
	"errors"
	"io"

	"github.com/gogo/protobuf/proto"
)

// Tags are used to group keys by type.
const (
	ChannelTag   = 'h'
	EventTag     = 'e'
	EventV0Tag   = 'E'
	EventTimeTag = 't'
	IndexTag     = 'I'

	Sep byte = 0

	IndexTagV1_0_0 = 'i'

	FirstTopic = "\x01"
	LastTopic  = "\xff\xff\xff\xff"

	FirstEventID = "\x01"
	LastEventID  = "\xff\xff\xff\xff"
)

var (
	// EventPrefix is the prefix for all event keys.
	EventPrefix = []byte{EventTag, Sep}
)

// Key is an object which can be marshalled and unmarshalled by this package
type Key interface {
	Marshal([]byte) ([]byte, error)
	isKey()
}

// UnmarshalTo unmarshals a key marshaled by Marshal(key Key) into a provided object
func UnmarshalTo(src []byte, dest Key) error {
	switch dest := dest.(type) {
	case *EventKey:
		return UnmarshalEventKey(src, dest)
	case *EventTimeKey:
		return UnmarshalEventTimeKey(src, dest)
	case *ChannelKey:
		return UnmarshalChannelKey(src, dest)
	case *IndexKey:
		return UnmarshalIndexKey(src, dest)
	case *IndexKeyV1_0_0:
		return UnmarshalIndexKeyV1_0_0(src, dest)
	case EventKey, ChannelKey, EventTimeKey, IndexKey, IndexKeyV1_0_0:
		return errors.New("dest must be pointer to a key")
	default:
		return errors.New("unrecognized type")
	}
}

// Unmarshal unmarshals a key marshaled by Marshal(key Key), by infering the type
func Unmarshal(src []byte) (Key, error) {
	i := bytes.IndexByte(src, Sep)
	if i == -1 {
		return nil, io.ErrUnexpectedEOF
	}

	switch src[0] {
	case EventTag:
		var key EventKey
		err := UnmarshalEventKey(src, &key)
		return key, err
	case EventTimeTag:
		var key EventTimeKey
		err := UnmarshalEventTimeKey(src, &key)
		return key, err
	case ChannelTag:
		var key ChannelKey
		err := UnmarshalChannelKey(src, &key)
		return key, err
	case IndexTag:
		var key IndexKey
		err := UnmarshalIndexKey(src, &key)
		return key, err
	case IndexTagV1_0_0:
		var key IndexKeyV1_0_0
		err := UnmarshalIndexKeyV1_0_0(src, &key)
		return key, err
	default:
		return nil, errors.New("unrecognized type")
	}
}

// UnmarshalPayload unmarshals a payload based on the type of it's key.
func UnmarshalPayload(data []byte, key Key) (proto.Message, error) {

	var payload proto.Message
	switch key.(type) {
	case EventKey, *EventKey:
		payload = new(EventPayload)
	case EventTimeKey, *EventTimeKey:
		payload = new(EventTimePayload)
	case ChannelKey, *ChannelKey:
		payload = new(ChannelPayload)
	default:
		return nil, errors.New("unrecognized type")
	}

	err := proto.Unmarshal(data, payload)
	if err != nil {
		return nil, err
	}

	return payload, nil
}
