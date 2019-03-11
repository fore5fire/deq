package eventdb

import (
	fmt "fmt"
	"log"

	"github.com/dgraph-io/badger"
	proto "github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq/internal/eventdb/kvstore"
)

type EventIter struct {
	iter kvstore.Iter
}

func (txn *Txn) NewEventIter(opts badger.IteratorOptions) *EventIter {

	return &EventIter{txn.txn.NewIterator(opts)}
}

func (it *EventIter) Seek(key EventKey) error {
	keybuf, err := key.Marshal(nil)
	if err != nil {
		return err
	}
	it.iter.Seek(keybuf)
	return nil
}

func (it *EventIter) Rewind() {
	key, _ := EventPrefixTopic("")
	it.iter.Seek(key)
}

func (it *EventIter) Next() {
	it.iter.Next()
}

func (it *EventIter) Valid() bool {
	prefix := [...]byte{EventTag, Sep}
	return it.iter.ValidForPrefix(prefix[:])
}

func (it *EventIter) ValidForTopic(topic string) bool {
	prefix, err := EventPrefixTopic(topic)
	if err != nil {
		log.Printf("[WARN] use of invalid topic")
		return false
	}
	return it.iter.ValidForPrefix(prefix)
}

func (it *EventIter) EventPayload(dst *EventPayload) error {
	buf, err := it.iter.Item().Value()
	if err != nil {
		return err
	}
	err = proto.Unmarshal(buf, dst)
	if err != nil {
		return fmt.Errorf("unmarshal event time payload: %v", err)
	}
	return nil
}

func (it *EventIter) Key(dst *EventKey) error {
	err := UnmarshalEventKey(it.iter.Item().Key(), dst)
	if err != nil {
		return err
	}
	return nil
}

// Close closes iter. Close should always be called when an iter is done being used.
func (it *EventIter) Close() {
	it.iter.Close()
}
