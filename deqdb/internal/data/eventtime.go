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

// EventTimeKey is a key for EventTimePayloads. It can be marshalled and used
// in a key-value store.
//
// The marshalled format of an EventTimeKey is:
// EventTimeTag + Sep + Topic as string data + Sep + ID
type EventTimeKey struct {
	// Topic must not contain the null character.
	Topic string
	ID    string
}

func (key EventTimeKey) isKey() {}

// Size returns the length of this key's marshalled data. The result is only
// valid until the key is modified.
func (key EventTimeKey) Size() int {
	return len(key.Topic) + len(key.ID) + 3
}

// Marshal marshals a key into a byte slice, prefixed according to the key's type.
//
// If buf is nil or has insufficient capacity, a new buffer is allocated. Marshal returns the
// slice that index was marshalled to.
func (key EventTimeKey) Marshal(buf []byte) ([]byte, error) {

	if strings.ContainsRune(key.Topic, 0) {
		return nil, errors.New("Topic cannot contain null character")
	}

	size := key.Size()
	if cap(buf) < size {
		buf = make([]byte, 0, size)
	} else {
		buf = buf[:0]
	}

	buf = append(buf, EventTimeTag, Sep)
	buf = append(buf, key.Topic...)
	buf = append(buf, Sep)
	buf = append(buf, key.ID...)

	return buf, nil
}

func (key EventTimeKey) NewValue() proto.Message {
	return new(EventTimePayload)
}

// UnmarshalEventTimeKey updates the this key's values by decoding the provided buf
func UnmarshalEventTimeKey(buf []byte, key *EventTimeKey) error {
	buf = buf[2:]
	i := bytes.IndexByte(buf, Sep)
	if i == -1 {
		return errors.New("parse Topic: null terminator not found")
	}
	key.Topic = string(buf[:i])
	buf = buf[i+1:]
	key.ID = string(buf)
	return nil
}

// EventTimePrefixTopic creates a prefix for EventKeys of a given topic.
//
//
func EventTimePrefixTopic(topic string) ([]byte, error) {
	if strings.ContainsRune(topic, 0) {
		return nil, errors.New("Topic cannot contain null character")
	}
	ret := make([]byte, 0, len(topic)+3)
	ret = append(ret, EventTimeTag, Sep)
	ret = append(ret, topic...)
	ret = append(ret, Sep)

	return ret, nil
}

// EventTimeCursorBeforeTopic returns an EventTimeKey cursor before the given topic. Unlike
// EventTimePrefixTopic, EventTopicCursor does not include a trailing Sep.
//
// Pass topic as the empty string for a cursor before the first topic.
func EventTimeCursorBeforeTopic(topic string) ([]byte, error) {
	if strings.ContainsRune(topic, 0) {
		return nil, errors.New("Topic cannot contain null character")
	}
	ret := make([]byte, 0, len(topic)+2)
	ret = append(ret, EventTimeTag, Sep)
	ret = append(ret, topic...)

	return ret, nil
}

// EventTimeCursorAfterTopic returns an EventTimeKey cursor just after all events of the given topic.
//
// Pass LastTopic for topic parameter for a cursor after events in the last topic.
func EventTimeCursorAfterTopic(topic string) ([]byte, error) {
	if strings.ContainsRune(topic, 0) {
		return nil, errors.New("Topic cannot contain null character")
	}
	ret := make([]byte, 0, len(topic)+7)
	ret = append(ret, EventTimeTag, Sep)
	ret = append(ret, topic...)
	ret = append(ret, Sep, 0xff, 0xff, 0xff, 0xff)

	return ret, nil
}

// GetEventTimePayload gets an EventTimePayload from the database by its EventTimeKey.
//
// If no EventTimePayload exists in the database for the specified key, deq.ErrNotFound is returnd.
func GetEventTimePayload(txn Txn, key *EventTimeKey, dst *EventTimePayload) error {
	rawKey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal event time key: %v", err)
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
			return fmt.Errorf("unmarshal event time payload: %v", err)
		}
		return nil
	})
}
