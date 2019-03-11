package eventdb

import (
	fmt "fmt"
	"log"

	"github.com/dgraph-io/badger"
	proto "github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq/internal/eventdb/kvstore"
)

type ChannelIter struct {
	iter    kvstore.Iter
	reverse bool
}

func (txn *Txn) NewChannelIter(opts badger.IteratorOptions) *ChannelIter {
	return &ChannelIter{
		iter:    txn.txn.NewIterator(opts),
		reverse: opts.Reverse,
	}
}

func (it *ChannelIter) Seek(key ChannelKey) error {
	keybuf, err := key.Marshal(nil)
	if err != nil {
		return err
	}
	it.iter.Seek(keybuf)
	return nil
}

func (it *ChannelIter) SeekChannel(channel string) error {
	if it.reverse {
		channel += "\x01"
	}
	buf, err := ChannelPrefix(channel)
	if err != nil {
		return err
	}
	it.iter.Seek(buf)
	return nil
}

func (it *ChannelIter) Rewind() {
	key, _ := ChannelPrefix("")
	it.iter.Seek(key)
}

func (it *ChannelIter) Next() {
	it.iter.Next()
}

func (it *ChannelIter) Valid() bool {
	prefix := [...]byte{ChannelTag, Sep}
	return it.iter.ValidForPrefix(prefix[:])
}

func (it *ChannelIter) ValidForChannel(channel string) bool {
	prefix, err := ChannelPrefix(channel)
	if err != nil {
		log.Printf("[WARN] use of invalid topic")
		return false
	}
	return it.iter.ValidForPrefix(prefix)
}

func (it *ChannelIter) ChannelPayload(dst *ChannelPayload) error {
	buf, err := it.iter.Item().Value()
	if err != nil {
		return err
	}
	err = proto.Unmarshal(buf, dst)
	if err != nil {
		return fmt.Errorf("unmarshal Channel time payload: %v", err)
	}
	return nil
}

func (it *ChannelIter) Key(dst *ChannelKey) error {
	err := UnmarshalChannelKey(it.iter.Item().Key(), dst)
	if err != nil {
		return err
	}
	return nil
}

// Close closes iter. Close should always be called when an iter is done being used.
func (it *ChannelIter) Close() {
	it.iter.Close()
}
