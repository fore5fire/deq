package data

import (
	"bytes"
	"errors"
	"strings"
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

// Marshal allocates a byte slice and marshals the key into it.
func (key EventTimeKey) Marshal() ([]byte, error) {
	buf := make([]byte, key.Size())
	err := key.MarshalTo(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// MarshalTo marshals a key into a byte slice, prefixed according
// to the key's type. buf must have length of at least key.Size().
func (key EventTimeKey) MarshalTo(buf []byte) error {

	if strings.ContainsRune(key.Topic, 0) {
		return errors.New("Topic cannot contain null character")
	}

	buf[0], buf[1] = EventTimeTag, Sep
	buf = buf[2:]
	copy(buf, key.Topic)
	buf = buf[len(key.Topic):]
	buf[0] = Sep
	buf = buf[1:]
	copy(buf[:], key.ID)
	return nil
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
