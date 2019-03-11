package eventdb

import (
	"log"

	"github.com/dgraph-io/badger"
	"gitlab.com/katcheCode/deq/internal/eventdb/kvstore"
)

type IndexIter struct {
	iter kvstore.Iter
}

func (txn *Txn) NewIndexIter(opts badger.IteratorOptions) *IndexIter {
	return &IndexIter{txn.txn.NewIterator(opts)}
}

func (it *IndexIter) Seek(key IndexKey) error {
	keybuf, err := key.Marshal(nil)
	if err != nil {
		return err
	}
	it.iter.Seek(keybuf)
	return nil
}

func (it *IndexIter) Rewind() {
	key, _ := IndexPrefixTopic("")
	it.iter.Seek(key)
}

func (it *IndexIter) Next() {
	it.iter.Next()
}

func (it *IndexIter) Valid() bool {
	prefix := [...]byte{IndexTag, Sep}
	return it.iter.ValidForPrefix(prefix[:])
}

func (it *IndexIter) ValidForTopic(topic string) bool {
	prefix, err := IndexPrefixTopic(topic)
	if err != nil {
		log.Printf("[WARN] use of invalid topic")
		return false
	}
	return it.iter.ValidForPrefix(prefix)
}

func (it *IndexIter) Key(dst *IndexKey) error {
	err := UnmarshalIndexKey(it.iter.Item().Key(), dst)
	if err != nil {
		return err
	}
	return nil
}

// Close closes iter. Close should always be called when an iter is done being used.
func (it *IndexIter) Close() {
	it.iter.Close()
}
