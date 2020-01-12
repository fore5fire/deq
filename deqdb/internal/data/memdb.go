package data

import (
	"bytes"
	"errors"
	io "io"
	"sort"
	"sync"

	"github.com/dgraph-io/badger"
)

type memDB struct {
	data []memItem
	mut  sync.RWMutex
}

// NewInMemoryDB creates a new DB that stores its data in-memory. In-memory storage is intended for
// unit tests only, and should not be used in production or integration tests.
func NewInMemoryDB() DB {
	return &memDB{}
}

func (db *memDB) Close() error {
	return nil
}

func (db *memDB) RunValueLogGC(float64) error {
	return nil
}

func (db *memDB) Backup(w io.Writer, since uint64) (uint64, error) {
	return 0, errors.New("unimplemented")
}

func (db *memDB) Load(io.Reader, int) error {
	return errors.New("unimplemented")
}

type memTxn struct {
	update  bool
	data    []memItem
	writes  []memItem
	deletes []memItem
	reads   []memItem
	db      *memDB
}

func (db *memDB) NewTransaction(update bool) Txn {
	db.mut.RLock()
	defer db.mut.RUnlock()

	return &memTxn{
		update: update,
		data:   append([]memItem(nil), db.data...),
		db:     db,
	}
}

// find returns key's index in data and whether key already exists in data. If the key exists in
// data, its index is returned regardless of the value of reversed. If there is no exact match,
// the index of the next highest key is returned if reversed is false, or the next lowest index if
// reversed is true. If there is no next element in the requested order, len(memItem) is returned.
func find(data []memItem, key []byte, reversed bool) (int, bool) {
	i := sort.Search(len(data), func(i int) bool {
		return bytes.Compare(data[i].key, key) >= 0
	})
	found := i < len(data) && bytes.Equal(data[i].key, key)
	if found {
		return i, true
	}
	if reversed {
		if i == 0 {
			return len(data), false
		}
		i--
	}
	return i, false
}

// findExclusive finds the index of the item before or after key in data.
func findExclusive(data []memItem, key []byte, reversed bool) int {
	if reversed {
		i := sort.Search(len(data), func(i int) bool {
			return bytes.Compare(data[i].key, key) >= 0
		})
		if i == 0 {
			return len(data)
		}
		return i - 1
	}

	return sort.Search(len(data), func(i int) bool {
		return bytes.Compare(data[i].key, key) > 0
	})
}

func set(data []memItem, item memItem) []memItem {
	i, exists := find(data, item.key, false)
	if exists {
		// key already exists - update it.
		data[i] = item
	} else {
		// key is new - insert it.
		data = append(data, memItem{})
		copy(data[i+1:], data[i:])
		data[i] = item
	}

	return data
}

func del(data []memItem, key []byte) []memItem {
	i, exists := find(data, key, false)
	if !exists {
		return data
	}

	copy(data[i:], data[i+1:])
	return data[:len(data)-1]
}

func (txn *memTxn) Set(key, val []byte) error {
	if !txn.update {
		return badger.ErrReadOnlyTxn
	}
	if len(key) == 0 {
		return badger.ErrEmptyKey
	}

	txn.writes = set(txn.writes, memItem{
		key: append([]byte(nil), key...),
		val: append([]byte(nil), val...),
	})
	txn.deletes = del(txn.deletes, key)
	return nil
}

func (txn *memTxn) Get(key []byte) (Item, error) {
	if txn.update {
		i, exists := find(txn.writes, key, false)
		if exists {
			return txn.writes[i], nil
		}
		i, exists = find(txn.deletes, key, false)
		if exists {
			return nil, badger.ErrKeyNotFound
		}
		// Record the read if the transaction is updatable and the value hasn't been modified.
		txn.reads = set(txn.reads, memItem{
			key: append([]byte(nil), key...),
		})
	}
	i, exists := find(txn.data, key, false)
	if exists {
		return txn.data[i], nil
	}
	return nil, badger.ErrKeyNotFound
}

func (txn *memTxn) Delete(key []byte) error {
	if !txn.update {
		return badger.ErrReadOnlyTxn
	}
	if len(key) == 0 {
		return badger.ErrEmptyKey
	}

	txn.writes = del(txn.writes, key)
	txn.deletes = set(txn.deletes, memItem{
		key: append([]byte(nil), key...),
	})
	return nil
}

func (txn *memTxn) Discard() {}

func (txn *memTxn) Commit() error {
	if !txn.update {
		return badger.ErrReadOnlyTxn
	}

	txn.db.mut.Lock()
	defer txn.db.mut.Unlock()

	// Check for conflicts.
	for _, item := range txn.reads {
		i, exists := find(txn.db.data, item.key, false)
		j, existed := find(txn.data, item.key, false)
		if exists != existed {
			return badger.ErrConflict
		}
		if exists && bytes.Compare(txn.db.data[i].val, txn.data[j].val) != 0 {
			return badger.ErrConflict
		}
	}

	// Persist writes.
	for _, item := range txn.writes {
		txn.db.data = set(txn.db.data, item)
	}

	// Persist deletes.
	for _, item := range txn.deletes {
		txn.db.data = del(txn.db.data, item.key)
	}

	return nil
}

type memIter struct {
	current   memItem
	txn       *memTxn
	reversed  bool
	trackRead bool
}

func (txn *memTxn) NewIterator(opts badger.IteratorOptions) Iter {
	return &memIter{
		txn:      txn,
		reversed: opts.Reverse,
	}
}

func (it *memIter) Rewind() {

	var current memItem
	if it.reversed {
		if len(it.txn.data) > 0 {
			current = it.txn.data[len(it.txn.data)-1]
			it.trackRead = it.txn.update
		}
		idx := len(it.txn.writes) - 1
		if it.txn.update && len(it.txn.writes) > 0 && (len(current.key) == 0 || bytes.Compare(current.key, it.txn.writes[idx].key) <= 0) {
			current = it.txn.writes[idx]
			it.trackRead = false
		}
	} else {
		if len(it.txn.data) > 0 {
			current = it.txn.data[0]
			it.trackRead = it.txn.update
		}
		if it.txn.update && len(it.txn.writes) > 0 && (len(current.key) == 0 || bytes.Compare(current.key, it.txn.writes[0].key) >= 0) {
			current = it.txn.writes[0]
			it.trackRead = false
		}
	}

	it.current = current
}

func (it *memIter) Seek(key []byte) {
	if len(key) == 0 {
		if it.reversed {
			it.current = memItem{}
		} else {
			it.Rewind()
		}
		return
	}

	i, _ := find(it.txn.data, key, it.reversed)
	var current memItem
	if i < len(it.txn.data) {
		current = it.txn.data[i]
		it.trackRead = it.txn.update
	}

	if it.txn.update {
		j, _ := find(it.txn.writes, key, it.reversed)
		// Use the earliest key in the iteration order, or the key from Writes if they're the same.
		target := 1
		if it.reversed {
			target = -1
		}
		if j < len(it.txn.writes) && (len(current.key) == 0 || bytes.Compare(it.current.key, it.txn.writes[j].key) != target) {
			current = it.txn.writes[j]
			it.trackRead = false
		}
	}

	it.current = current
}

func (it *memIter) Next() {
	if !it.Valid() {
		panic("called Next on invalid iterator")
	}

	var current memItem

	i := findExclusive(it.txn.data, it.current.key, it.reversed)
	if i < len(it.txn.data) {
		current = it.txn.data[i]
		it.trackRead = it.txn.update
	}

	if it.txn.update {
		j := findExclusive(it.txn.writes, it.current.key, it.reversed)
		// Use the earliest key, or the key from Writes if they're the same.
		if j < len(it.txn.writes) && (len(current.key) == 0 || bytes.Compare(it.current.key, it.txn.writes[j].key) <= 0) {
			current = it.txn.writes[j]
			it.trackRead = false
		}
	}

	it.current = current
}

func (it *memIter) Valid() bool {
	return len(it.current.key) != 0
}

func (it *memIter) ValidForPrefix(prefix []byte) bool {
	return it.Valid() && bytes.HasPrefix(it.current.key, prefix)
}

func (it *memIter) Item() Item {
	if !it.Valid() {
		panic("cannot call Item on invalid Iter")
	}

	if it.trackRead {
		it.txn.reads = set(it.txn.reads, memItem{
			key: it.current.key,
		})
	}

	return &it.current
}

func (it *memIter) Close() {
}

type memItem struct {
	key, val []byte
}

func (item memItem) Key() []byte {
	return item.key
}

func (item memItem) KeyCopy(dst []byte) []byte {
	return append(dst[:0], item.key...)
}

func (item memItem) Value(callback func([]byte) error) error {
	return callback(item.val)
}

func (item memItem) ValueCopy(dst []byte) ([]byte, error) {
	return append(dst[:0], item.val...), nil
}
