//go:generate protoc -I=. -I=../../../ -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogofaster_out=plugins=grpc,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mapi/v1/deq/deq.proto=gitlab.com/katcheCode/deq/api/v1/deq,Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types:. data.proto

// Package data provides keys and payloads for data stored in DEQ's internal key-value store.
package data

import (
	"bytes"
	"errors"
	fmt "fmt"
	"io"

	"github.com/dgraph-io/badger"
	"gitlab.com/katcheCode/deq"

	"github.com/gogo/protobuf/proto"
)

// Tags are used to group keys by type.
const (
	ChannelTag   = 'h'
	EventTag     = 'e'
	EventV0Tag   = 'E'
	EventTimeTag = 't'
	IndexTag     = 'I'
	SendCountTag = 'c'

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
	NewValue() proto.Message
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
	case *SendCountKey:
		return UnmarshalSendCountKey(src, dest)
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
		key := new(EventKey)
		return key, UnmarshalEventKey(src, key)
	case EventTimeTag:
		key := new(EventTimeKey)
		return key, UnmarshalEventTimeKey(src, key)
	case ChannelTag:
		key := new(ChannelKey)
		return key, UnmarshalChannelKey(src, key)
	case IndexTag:
		key := new(IndexKey)
		return key, UnmarshalIndexKey(src, key)
	case IndexTagV1_0_0:
		key := new(IndexKeyV1_0_0)
		return key, UnmarshalIndexKeyV1_0_0(src, key)
	case SendCountTag:
		key := new(SendCountKey)
		return key, UnmarshalSendCountKey(src, key)
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

func Get(txn Txn, key Key, dst proto.Message) error {
	rawkey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal key: %v", err)
	}

	item, err := txn.Get(rawkey)
	if err == badger.ErrKeyNotFound {
		return deq.ErrNotFound
	}
	if err != nil {
		return err
	}

	return item.Value(func(val []byte) error {
		err = proto.Unmarshal(val, dst)
		if err != nil {
			return fmt.Errorf("unmarshal value: %v", err)
		}
		return nil
	})
}
