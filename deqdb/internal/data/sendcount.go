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

// SendCountKey is a key for SendCountPayloads. It can be marshalled and used
// in a key-value store.
type SendCountKey struct {
	// Channel must not contain the null character
	Channel string
	// Topic must not contain the null character
	Topic string
	ID    string
}

func (key *SendCountKey) isKey() {}

// Size returns the length of this key's marshalled data. The result is only
// valid until the key is modified.
func (key *SendCountKey) Size() int {
	return len(key.Channel) + len(key.Topic) + len(key.ID) + 4
}

// Marshal marshals a key into a byte slice, prefixed according to the key's type.
//
// If buf is nil or has insufficient capacity, a new buffer is allocated. Marshal returns the
// slice that index was marshalled to.
func (key *SendCountKey) Marshal(buf []byte) ([]byte, error) {

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

	buf = append(buf, SendCountTag, Sep)
	buf = append(buf, key.Channel...)
	buf = append(buf, Sep)
	buf = append(buf, key.Topic...)
	buf = append(buf, Sep)
	buf = append(buf, key.ID...)

	return buf, nil
}

func (key *SendCountKey) NewValue() proto.Message {
	return new(SendCount)
}

// UnmarshalSendCountKey updates the the key's values by decoding the provided buffer
func UnmarshalSendCountKey(buf []byte, key *SendCountKey) error {
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

// GetSendCount gets a ChannelPayload from the database by its SendCountKey.
//
// If no ChannelPayload exists in the database for the specified key, deq.ErrNotFound is returned.
// When deq.ErrNotFound is returned, dst is guaranteed to be unmodified, so to set a default value
// for dst, just initialize it to the desired default and ignore the error deq.ErrNotFound. For
// example:
//
//   count := data.SendCount{
//     SendCount: 1,
//   }
//   err := data.GetSendCount(txn, &key, &count)
//   if err != nil && err != deq.ErrNotFound {
//	   // handle error
//   }
func GetSendCount(txn Txn, key *SendCountKey, dst *SendCount) error {

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

// SetSendCount writes a SendCount to the database at the provided key.
func SetSendCount(txn Txn, key *SendCountKey, count *SendCount) error {

	rawkey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal key: %v", err)
	}
	buf, err := proto.Marshal(count)
	if err != nil {
		return fmt.Errorf("marshal payload: %v", err)
	}

	err = txn.Set(rawkey, buf)
	if err != nil {
		return err
	}

	return nil
}
