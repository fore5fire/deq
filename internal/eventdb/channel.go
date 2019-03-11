package eventdb

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
