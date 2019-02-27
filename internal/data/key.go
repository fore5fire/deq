//go:generate protoc -I=. -I=../../../ -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogofaster_out=plugins=grpc,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mapi/v1/deq/deq.proto=gitlab.com/katcheCode/deq/api/v1/deq,Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types:. data.proto

package data

import (
	"bytes"
	"errors"
	"io"

	"github.com/gogo/protobuf/proto"
)

// Tags are used to group keys by type.
const (
	ChannelTag        = 'h'
	EventTag          = 'e'
	EventV0Tag        = 'E'
	EventTimeTag      = 't'
	Sep          byte = 0

	FirstTopic = "\x01"
	LastTopic  = "\xff\xff\xff\xff"

	FirstEventID = "\x01"
	LastEventID  = "\xff\xff\xff\xff"
)

var (
	// EventPrefix is the prefix for all event keys.
	EventPrefix = []byte{EventTag, Sep}
)

// Key is an object which can be marshalled and unmarshalled by this library
type Key interface {
	Marshal() ([]byte, error)
	isKey()
}

// UnmarshalTo unmarshals a key marshaled by Marshal(key Key) into a provided object
func UnmarshalTo(data []byte, dest Key) error {
	switch dest := dest.(type) {
	case *EventKey:
		return UnmarshalEventKey(data, dest)
	case *EventTimeKey:
		return UnmarshalEventTimeKey(data, dest)
	case *ChannelKey:
		return UnmarshalChannelKey(data, dest)
	case EventKey, ChannelKey, EventTimeKey:
		return errors.New("dest must be pointer to a key")
	default:
		return errors.New("unrecognized type")
	}
}

// Unmarshal unmarshals a key marshaled by Marshal(key Key), by infering the type
func Unmarshal(data []byte) (Key, error) {
	i := bytes.IndexByte(data, Sep)
	if i == -1 {
		return nil, io.ErrUnexpectedEOF
	}

	switch data[0] {
	case EventTag:
		var key EventKey
		err := UnmarshalEventKey(data, &key)
		return key, err
	case EventTimeTag:
		var key EventTimeKey
		err := UnmarshalEventTimeKey(data, &key)
		return key, err
	case ChannelTag:
		var key ChannelKey
		err := UnmarshalChannelKey(data, &key)
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
