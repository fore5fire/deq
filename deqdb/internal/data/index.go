package data

import (
	"bytes"
	"errors"
	"strings"

	proto "github.com/gogo/protobuf/proto"
)

// IndexKey is a key for custom indexes of events. It can be marshalled and used in a key-value
// store.
//
// The marshalled format of an IndexKey is:
// IndexTag + Sep + Topic + Sep + Type + Sep + Value
type IndexKey struct {
	// Topic must not contain the null character
	Topic string
	// Value must not contain the null character.
	Value string
}

func (key IndexKey) isKey() {}

// Size returns the length of this key's marshalled data. The result is only
// valid until the key is modified.
func (key IndexKey) Size() int {
	return len(key.Topic) + len(key.Value) + 3
}

// Marshal marshals a key into a byte slice, prefixed according to the key's type.
//
// If buf is nil or has insufficient capacity, a new buffer is allocated. Marshal returns the
// slice that index was marshalled to.
func (key IndexKey) Marshal(buf []byte) ([]byte, error) {

	if key.Topic == "" {
		return nil, errors.New("Topic is required")
	}
	if strings.ContainsRune(key.Topic, 0) {
		return nil, errors.New("Topic cannot contain null character")
	}

	size := key.Size()
	if cap(buf) < size {
		buf = make([]byte, 0, size)
	} else {
		buf = buf[:0]
	}

	buf = append(buf, IndexTag, Sep)
	buf = append(buf, key.Topic...)
	buf = append(buf, Sep)
	buf = append(buf, key.Value...)
	return buf, nil
}

func (key IndexKey) NewValue() proto.Message {
	return new(IndexPayload)
}

// UnmarshalIndexKey unmarshals a key marshaled by key.Marshal()
func UnmarshalIndexKey(buf []byte, key *IndexKey) error {
	i := bytes.IndexByte(buf, Sep)
	if i == -1 {
		return errors.New("parse tag: null terminator not found")
	}
	var comparisonTag = [...]byte{IndexTag}
	if !bytes.Equal(buf[:i], comparisonTag[:]) {
		return errors.New("buf does not contain an IndexKey")
	}
	j := bytes.IndexByte(buf[i+1:], Sep) + i + 1
	if j == -1 {
		return errors.New("parse Topic: null terminator not found")
	}

	key.Topic = string(buf[i+1 : j])
	key.Value = string(buf[j+1:])

	return nil
}

// IndexPrefixTopic creates a prefix for IndexKeys of a given topic.
func IndexPrefixTopic(topic string) ([]byte, error) {
	if strings.ContainsRune(topic, 0) {
		return nil, errors.New("Topic cannot contain null character")
	}
	ret := make([]byte, 0, len(topic)+3)
	ret = append(ret, IndexTag, Sep)
	ret = append(ret, topic...)
	ret = append(ret, Sep)

	return ret, nil
}

// IndexCursorAfterTopic returns an IndexKey cursor just after all index entries of the given topic.
//
// Pass topic as "\xff\xff\xff\xff" for a cursor after indexes in the last topic.
func IndexCursorAfterTopic(topic string) ([]byte, error) {
	if strings.ContainsRune(topic, 0) {
		return nil, errors.New("Topic cannot contain null character")
	}
	ret := make([]byte, 0, len(topic)+7)
	ret = append(ret, IndexTag, Sep)
	ret = append(ret, topic...)
	ret = append(ret, Sep, 0xff, 0xff, 0xff, 0xff)

	return ret, nil
}
