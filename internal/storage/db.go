package kvstore

import (
	"github.com/dgraph-io/badger"
)

type DB interface {
	NewTransaction(update bool) Txn
	RunValueLogGC(float64) error
	Close() error
}

type Txn interface {
	Commit() error
	Discard()
	Delete(key []byte) error
	Get(key []byte) (Item, error)
	Set(key []byte, val []byte) error
	NewIterator(badger.IteratorOptions) Iter
}

type Item interface {
	Key() []byte
	KeyCopy(dst []byte) []byte
	Value() ([]byte, error)
	ValueCopy(dst []byte) ([]byte, error)
}

type Iter interface {
	Item() Item
	Close()
	Rewind()
	Seek(key []byte)
	Valid() bool
	ValidForPrefix(prefix []byte) bool
	Next()
}
