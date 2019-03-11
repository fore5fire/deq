package eventdb

import (
	fmt "fmt"
	"log"

	"github.com/dgraph-io/badger"
	proto "github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq/internal/eventdb/kvstore"
)

type EventTimeIter struct {
	iter kvstore.Iter
}

func (txn *Txn) NewEventTimeIter(opts badger.IteratorOptions) *EventTimeIter {

	return &EventTimeIter{txn.txn.NewIterator(opts)}
}

func (it *EventTimeIter) Seek(key EventTimeKey) error {
	keybuf, err := key.Marshal(nil)
	if err != nil {
		return err
	}
	it.iter.Seek(keybuf)
	return nil
}

func (it *EventTimeIter) Rewind() {
	key, _ := EventTimePrefixTopic("")
	it.iter.Seek(key)
}

func (it *EventTimeIter) Next() {
	it.iter.Next()
}

func (it *EventTimeIter) Valid() bool {
	prefix := [...]byte{EventTimeTag, Sep}
	return it.iter.ValidForPrefix(prefix[:])
}

func (it *EventTimeIter) ValidForTopic(topic string) bool {
	prefix, err := EventTimePrefixTopic(topic)
	if err != nil {
		log.Printf("[WARN] use of invalid topic")
		return false
	}
	return it.iter.ValidForPrefix(prefix)
}

func (it *EventTimeIter) EventTimePayload(dst *EventTimePayload) error {
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

func (it *EventTimeIter) Key(dst *EventTimeKey) error {
	err := UnmarshalEventTimeKey(it.iter.Item().Key(), dst)
	if err != nil {
		return err
	}
	return nil
}

// Close closes iter. Close should always be called when an iter is done being used.
func (it *EventTimeIter) Close() {
	it.iter.Close()
}
