package data

import (
	"bytes"
	"errors"
	fmt "fmt"
	"strings"

	"github.com/dgraph-io/badger"
	proto "github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq"
)

// ChannelKey is a key for ChannelPayloads. It can be marshalled and used
// in a key-value store.
type ChannelKey struct {
	// Channel must not contain the null character
	Channel string
	// Topic must not contain the null character
	Topic string
	ID    string
}

func (key ChannelKey) isKey() {}

// Size returns the length of this key's marshalled data. The result is only
// valid until the key is modified.
func (key ChannelKey) Size() int {
	return len(key.Channel) + len(key.Topic) + len(key.ID) + 4
}

// Marshal marshals a key into a byte slice, prefixed according to the key's type.
//
// If buf is nil or has insufficient capacity, a new buffer is allocated. Marshal returns the
// slice that index was marshalled to.
func (key ChannelKey) Marshal(buf []byte) ([]byte, error) {

	if strings.ContainsRune(key.Topic, 0) {
		return nil, errors.New("Topic cannot contain null character")
	}
	if strings.ContainsRune(key.Channel, 0) {
		return nil, errors.New("Channel cannot contain null character")
	}

	size := key.Size()
	if cap(buf) < size {
		buf = make([]byte, 0, size)
	} else {
		buf = buf[:0]
	}

	buf = append(buf, ChannelTag, Sep)
	buf = append(buf, key.Channel...)
	buf = append(buf, Sep)
	buf = append(buf, key.Topic...)
	buf = append(buf, Sep)
	buf = append(buf, key.ID...)

	return buf, nil
}

func (key ChannelKey) NewValue() proto.Message {
	return new(ChannelPayload)
}

// UnmarshalChannelKey updates the this key's values by decoding the provided buf
func UnmarshalChannelKey(buf []byte, key *ChannelKey) error {
	buf = buf[2:]
	i := bytes.IndexByte(buf, Sep)
	if i == -1 {
		return errors.New("parse Channel: null terminator not found")
	}
	key.Channel = string(buf[:i])
	buf = buf[i+1:]
	j := bytes.IndexByte(buf, Sep)
	if j == -1 {
		return errors.New("parse Topic: null terminator not found")
	}
	key.Topic = string(buf[:j])
	buf = buf[j+1:]
	key.ID = string(buf)
	return nil
}

// ChannelPrefix creates a prefix for ChannelKeys of a given topic
func ChannelPrefix(channel string) ([]byte, error) {

	if strings.ContainsRune(channel, 0) {
		return nil, errors.New("Channel cannot contain null character")
	}

	ret := make([]byte, len(channel)+2)
	buf := ret

	buf[0], buf[1] = ChannelTag, Sep
	buf = buf[2:]
	copy(buf, channel)
	buf = buf[len(channel):]
	// buf[0] = Sep
	return ret, nil
}

// EventStateToProto converts a deq.State to an EventState usable with this package.
func EventStateToProto(e deq.State) EventState {
	switch e {
	case deq.StateUnspecified:
		return EventState_UNSPECIFIED_STATE
	case deq.StateQueued:
		return EventState_QUEUED
	case deq.StateQueuedLinear:
		return EventState_QUEUED_LINEAR
	case deq.StateQueuedConstant:
		return EventState_QUEUED_CONSTANT
	case deq.StateOK:
		return EventState_OK
	case deq.StateInternal:
		return EventState_INTERNAL
	case deq.StateInvalid:
		return EventState_INVALID
	case deq.StateDequeuedError:
		return EventState_DEQUEUED_ERROR
	case deq.StateSendLimitReached:
		return EventState_SEND_LIMIT_EXCEEDED
	default:
		panic("unrecognized EventState")
	}
}

// EventStateFromProto converts an EventState from this package to a deq.State.
func EventStateFromProto(e EventState) deq.State {
	switch e {
	case EventState_UNSPECIFIED_STATE:
		return deq.StateUnspecified
	case EventState_QUEUED:
		return deq.StateQueued
	case EventState_QUEUED_LINEAR:
		return deq.StateQueuedLinear
	case EventState_QUEUED_CONSTANT:
		return deq.StateQueuedConstant
	case EventState_OK:
		return deq.StateOK
	case EventState_INVALID:
		return deq.StateInvalid
	case EventState_INTERNAL:
		return deq.StateInternal
	case EventState_DEQUEUED_ERROR:
		return deq.StateDequeuedError
	case EventState_SEND_LIMIT_EXCEEDED:
		return deq.StateSendLimitReached
	default:
		panic("unrecognized EventState")
	}
}

// GetChannelEvent gets a ChannelPayload from the database by its ChannelKey.
//
// If no ChannelPayload exists in the database for the specified key, deq.ErrNotFound is returned.
// When deq.ErrNotFound is returned, dst is guaranteed to be unmodified, so to set a default value
// for dst, just initialize it to the desired default and ignore the error deq.ErrNotFound. For
// example:
//
//   channel := data.ChannelPayload{
//     EventState: data.EventState_QUEUED,
//   }
//   err := data.GetChannelEvent(txn, &key, &channel)
//   if err != nil && err != deq.ErrNotFound {
//	   // handle error
//   }
func GetChannelEvent(txn Txn, key *ChannelKey, dst *ChannelPayload) error {

	rawKey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal event key: %v", err)
	}

	item, err := txn.Get(rawKey)
	if err == badger.ErrKeyNotFound {
		return deq.ErrNotFound
	}
	if err != nil {
		return err
	}

	return item.Value(func(val []byte) error {
		err := proto.Unmarshal(val, dst)
		if err != nil {
			return fmt.Errorf("unmarshal channel payload: %v", err)
		}
		return nil
	})
}

// SetChannelEvent writes a ChannelPayload to the database at the provided key.
func SetChannelEvent(txn Txn, key *ChannelKey, payload *ChannelPayload) error {

	rawkey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal key: %v", err)
	}
	buf, err := proto.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %v", err)
	}

	err = txn.Set(rawkey, buf)
	if err != nil {
		return err
	}

	return nil
}
