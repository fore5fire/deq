package data

import (
	"bytes"
	"errors"
	"strings"
)

// EventTimeKey is a key for EventTimePayloads. It can be marshalled and used
// in a key-value store.
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

// Unmarshal updates the this key's values by decoding the provided buf
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
