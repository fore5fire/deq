package data

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"time"
)

// EventKey is a key for EventPayloads. It can be marshalled and used
// in a key-value store.
//
// The marshalled format of an EventKey is:
// EventTag + Sep + Topic as string data + Sep + CreateTime as 8 byte unix nano integer + Sep + ID
//
// TODO: Decide if we even need to keep an index over CreateTime, and if so then migrate to use
// IndexKey and store event body in what is not EventTimeIndex
type EventKey struct {
	// Topic must not contain the null character
	Topic string
	// Must be after unix epoch
	CreateTime time.Time
	ID         string
}

func (key EventKey) isKey() {}

// Size returns the length of this key's marshalled data. The result is only
// valid until the key is modified.
func (key EventKey) Size() int {
	return len(key.Topic) + len(key.ID) + 11
}

// Marshal marshals a key into a byte slice, prefixed according to the key's type.
//
// If buf is nil or has insufficient capacity, a new buffer is allocated. Marshal returns the
// slice that index was marshalled to.
func (key EventKey) Marshal(buf []byte) ([]byte, error) {

	if key.CreateTime.Before(time.Unix(0, 1)) {
		return nil, errors.New("CreateTime must be after the unix epoch")
	}
	if strings.ContainsRune(key.Topic, 0) {
		return nil, errors.New("Topic cannot contain null character")
	}
	if key.Topic == "" {
		return nil, errors.New("Topic is required")
	}
	if key.ID == "" {
		return nil, errors.New("ID is required")
	}

	size := key.Size()
	if cap(buf) < size {
		buf = make([]byte, 0, size)
	} else {
		buf = buf[:0]
	}

	buf = append(buf, EventTag, Sep)
	buf = append(buf, key.Topic...)
	buf = append(buf, Sep)
	buf = buf[:len(buf)+8]
	binary.BigEndian.PutUint64(buf[len(buf)-8:], uint64(key.CreateTime.UnixNano()))
	buf = append(buf, key.ID...)

	return buf, nil
}

// UnmarshalEventKey unmarshals a key marshaled by key.Marshal()
func UnmarshalEventKey(buf []byte, key *EventKey) error {
	buf = buf[2:]
	i := bytes.IndexByte(buf, Sep)
	if i == -1 {
		return errors.New("parse Topic: null terminator not found")
	}
	if i+9 > len(buf) {
		return errors.New("parse CreateTime: unexpected end of input")
	}
	key.Topic = string(buf[:i])
	buf = buf[i+1:]
	key.CreateTime = time.Unix(0, int64(binary.BigEndian.Uint64(buf[:8])))
	buf = buf[8:]
	key.ID = string(buf)
	return nil
}

// EventPrefixTopic creates a prefix for EventKeys of a given topic.
func EventPrefixTopic(topic string) ([]byte, error) {
	if strings.ContainsRune(topic, 0) {
		return nil, errors.New("Topic cannot contain null character")
	}
	ret := make([]byte, 0, len(topic)+3)
	ret = append(ret, EventTag, Sep)
	ret = append(ret, topic...)
	ret = append(ret, Sep)

	return ret, nil
}

// // EventCursorBeforeTopic returns an EventKey cursor before the given topic. Unlike
// // EventPrefixTopic, EventTopicCursor does not include a trailing Sep.
// //
// // Pass topic as the empty string for a cursor before the first topic.
// func EventCursorBeforeTopic(topic string) ([]byte, error) {
// 	if strings.ContainsRune(topic, 0) {
// 		return nil, errors.New("Topic cannot contain null character")
// 	}
// 	ret := make([]byte, 0, len(topic)+2)
// 	ret = append(ret, EventTag, Sep)
// 	ret = append(ret, topic...)

// 	return ret, nil
// }

// EventCursorAfterTopic returns an EventKey cursor just after all events of the given topic.
//
// Pass topic as "\xff\xff\xff\xff" for a cursor after events in the last topic.
func EventCursorAfterTopic(topic string) ([]byte, error) {
	if strings.ContainsRune(topic, 0) {
		return nil, errors.New("Topic cannot contain null character")
	}
	ret := make([]byte, 0, len(topic)+7)
	ret = append(ret, EventTag, Sep)
	ret = append(ret, topic...)
	ret = append(ret, Sep, 0xff, 0xff, 0xff, 0xff)

	return ret, nil
}
