package data

import (
	"github.com/dgraph-io/badger"
)

// DBFromBadger creates a DB using a badger DB as its data store.
func DBFromBadger(db *badger.DB) DB {
	return badgerDB{db}
}

type badgerDB struct {
	*badger.DB
}

type badgerTxn struct {
	*badger.Txn
}

type badgerIter struct {
	*badger.Iterator
}

func (db badgerDB) NewTransaction(update bool) Txn {
	return badgerTxn{db.DB.NewTransaction(update)}
}

func (txn badgerTxn) NewIterator(opts badger.IteratorOptions) Iter {
	return badgerIter{txn.Txn.NewIterator(opts)}
}

func (txn badgerTxn) Get(key []byte) (Item, error) {
	return txn.Txn.Get(key)
}

func (txn badgerTxn) Commit() error {
	return txn.Txn.Commit()
}

func (it badgerIter) Item() Item {
	return it.Iterator.Item()
}
