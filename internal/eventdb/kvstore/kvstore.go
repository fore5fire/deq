package kvstore

import (
	"github.com/dgraph-io/badger"
)

type badgerDB struct {
	*badger.DB
}

func WrapBadger(db *badger.DB) DB {
	return &badgerDB{db}
}

func (db *badgerDB) NewTransaction(update bool) Txn {
	return &badgerTxn{db.DB.NewTransaction(update)}
}

type badgerTxn struct {
	*badger.Txn
}

func (txn *badgerTxn) Commit() error {
	return txn.Txn.Commit(nil)
}

func (txn *badgerTxn) Get(key []byte) (Item, error) {
	return txn.Txn.Get(key)
}

func (txn *badgerTxn) NewIterator(opts badger.IteratorOptions) Iter {
	return &badgerIter{txn.Txn.NewIterator(opts)}
}

type badgerIter struct {
	*badger.Iterator
}

func (iter *badgerIter) Item() Item {
	return iter.Iterator.Item()
}
