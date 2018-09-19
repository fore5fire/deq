package data

import (
	"bytes"
	"errors"
	"strings"
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

// Marshal allocates a byte slice and marshals the key into it.
func (key ChannelKey) Marshal() ([]byte, error) {
	buf := make([]byte, key.Size())
	err := key.MarshalTo(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// MarshalTo marshals a key into a byte slice, prefixed according
// to the key's type. buf must have length of at least key.Size().
func (key ChannelKey) MarshalTo(buf []byte) error {

	if strings.ContainsRune(key.Topic, 0) {
		return errors.New("Topic cannot contain null character")
	}
	if strings.ContainsRune(key.Channel, 0) {
		return errors.New("Channel cannot contain null character")
	}

	buf[0], buf[1] = ChannelTag, Sep
	buf = buf[2:]
	copy(buf, key.Channel)
	buf = buf[len(key.Channel):]
	buf[0] = Sep
	buf = buf[1:]
	copy(buf, key.Topic)
	buf = buf[len(key.Topic):]
	buf[0] = Sep
	buf = buf[1:]
	copy(buf, key.ID)
	return nil
}

// Unmarshal updates the this key's values by decoding the provided buf
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
