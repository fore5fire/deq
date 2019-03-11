package kvstore

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/dgraph-io/badger"
)

func NewInMemoryDB(defaults map[string]string) DB {

	size := 20
	if len(defaults) > size {
		size = len(defaults)
	}

	db := &inMemDB{
		data: make(inMemItemList, 0, size),
	}

	for k := range defaults {
		db.data, _ = db.data.set(&inMemItem{
			key:   []byte(k),
			value: []byte(defaults[k]),
		})
	}
	fmt.Println(db.data.String())
	return db
}

type inMemDB struct {
	sync.Mutex
	data inMemItemList
}

type inMemItemList []*inMemItem

func (db *inMemDB) NewTransaction(update bool) Txn {
	db.Lock()
	defer db.Unlock()

	txn := &inMemTxn{
		view: make(inMemItemList, 0, len(db.data)),
		db:   db,
	}

	for _, item := range db.data {
		txn.view = append(txn.view, &inMemItem{
			key:   []byte(item.key),
			value: []byte(item.value),
		})
	}

	if update {
		txn.updates = make(map[string]string)
		txn.deletes = make(map[string]struct{})
		txn.data = make(map[string]string, len(db.data))

		for _, item := range db.data {
			txn.data[string(item.key)] = string(item.value)
		}
	}

	return txn
}

func (db *inMemDB) RunValueLogGC(float64) error {
	return nil
}

func (db *inMemDB) Close() error {
	db.Lock()
	defer db.Unlock()

	db.data = nil

	return nil
}

type inMemTxn struct {
	data    map[string]string
	updates map[string]string
	deletes map[string]struct{}
	view    inMemItemList
	db      *inMemDB
}

func (txn *inMemTxn) Commit() error {
	txn.db.Lock()
	defer txn.db.Unlock()

	// Verify modified keys haven't been updated since the transaction was created
	for k := range txn.updates {
		item := txn.db.data.get([]byte(k))
		if txn.data[k] != string(item.value) {
			return badger.ErrConflict
		}
	}

	// Verify deleted keys haven't been udpated since the transaction was created
	for k := range txn.deletes {
		item := txn.db.data.get([]byte(k))
		if txn.data[k] != string(item.key) {
			return badger.ErrConflict
		}
	}

	for k, v := range txn.updates {
		txn.db.data, _ = txn.db.data.set(&inMemItem{
			key:   []byte(k),
			value: []byte(v),
		})
	}

	for k := range txn.deletes {
		var ok bool
		txn.db.data, ok = txn.db.data.del([]byte(k))
		if !ok {
			panic("no conflict detected but deleted failed")
		}
	}

	txn.Discard()

	return nil
}

func (txn *inMemTxn) Discard() {
	txn.data = nil
	txn.view = nil
	txn.updates = nil
	txn.deletes = nil
}

func (txn *inMemTxn) Delete(key []byte) error {
	k := string(key)
	txn.deletes[k] = struct{}{}
	delete(txn.updates, k)

	return nil
}

func (data inMemItemList) seekIndex(key []byte, reversed bool) int {
	if reversed {
		return sort.Search(len(data), func(i int) bool {
			return bytes.Compare(data[i].Key(), key) <= 0
		})
	}
	return sort.Search(len(data), func(i int) bool {
		return bytes.Compare(data[i].Key(), key) >= 0
	})
}

func (data inMemItemList) get(key []byte) *inMemItem {
	i := data.getIndex(key)
	if i == -1 {
		return nil
	}
	return data[i]
}

func (data inMemItemList) getIndex(key []byte) int {
	i := data.seekIndex(key, false)
	if i == len(data) || !bytes.Equal(data[i].key, key) {
		return -1
	}
	return i
}

func (data inMemItemList) set(item *inMemItem) (inMemItemList, bool) {
	i := data.seekIndex(item.Key(), false)

	// New key, make space for it
	insert := i == len(data) || !bytes.Equal(item.Key(), data[i].Key())
	if insert {
		data = append(data, nil)
		copy(data[i+1:], data[i:])
	}

	data[i] = item
	return data, insert
}

func (data inMemItemList) del(key []byte) (inMemItemList, bool) {
	i := data.getIndex(key)
	if i == -1 {
		return data, false
	}
	copy(data[i+1:len(data)], data[i:len(data)-1])
	return data[:len(data)-1], true
}

func (txn *inMemTxn) Get(key []byte) (Item, error) {
	item := txn.view.get(key)
	if item == nil {
		return nil, badger.ErrKeyNotFound
	}
	return item, nil
}

func (data inMemItemList) String() string {
	builder := strings.Builder{}
	for _, item := range data {
		if item == nil {
			builder.WriteString("<nil>\n")
			continue
		}

		builder.Write(item.key)
		builder.WriteString(": ")
		builder.Write(item.value)
		builder.WriteString("\n")
	}
	return builder.String()
}

func (txn *inMemTxn) Set(key []byte, val []byte) error {
	k, v := string(key), string(val)
	txn.updates[k] = v
	delete(txn.deletes, k)
	txn.view.set(&inMemItem{
		// Make sure to copy the key and value so the caller can't modify them
		key:   []byte(k),
		value: []byte(v),
	})
	return nil
}

func (txn *inMemTxn) NewIterator(opts badger.IteratorOptions) Iter {
	return &inMemIter{
		txn:     txn,
		reverse: opts.Reverse,
	}
}

func (txn *inMemTxn) printKeys() {
	for i, item := range txn.view {
		fmt.Printf("%d: %s\n", i, item.Key())
	}
}

type inMemItem struct {
	key, value []byte
}

func (it *inMemItem) Key() []byte {
	return it.key
}

func (it *inMemItem) KeyCopy(dst []byte) []byte {
	if len(dst) < len(it.key) {
		dst = make([]byte, len(it.key))
	}
	copy(dst, it.key)
	return dst
}

func (it *inMemItem) Value() ([]byte, error) {
	return it.value, nil
}

func (it *inMemItem) ValueCopy(dst []byte) ([]byte, error) {
	if len(dst) < len(it.value) {
		dst = make([]byte, len(it.value))
	}
	copy(dst, it.value)
	return dst, nil
}

type inMemIter struct {
	txn     *inMemTxn
	reverse bool
	key     []byte
	current Item
}

func (iter *inMemIter) Item() Item {
	return iter.current
}

func (iter *inMemIter) Close() {
}

func (iter *inMemIter) Rewind() {
	iter.Seek(nil)
}

func (iter *inMemIter) Seek(key []byte) {
	i := iter.txn.view.seekIndex(key, iter.reverse)
	if i == len(iter.txn.view) {
		iter.current = nil
		return
	}

	iter.current = iter.txn.view[i]
	iter.key = iter.current.Key()
}

func (iter *inMemIter) Valid() bool {
	return iter.current != nil
}

func (iter *inMemIter) ValidForPrefix(prefix []byte) bool {
	return iter.Valid() && bytes.HasPrefix(iter.current.Key(), prefix)
}

func (iter *inMemIter) Next() {
	i := iter.txn.view.seekIndex(iter.key, iter.reverse)
	if i == len(iter.txn.view) {
		iter.current = nil
		return
	}

	if bytes.Equal(iter.txn.view[i].Key(), iter.key) {
		i++
		if i == len(iter.txn.view) {
			iter.current = nil
			return
		}
	}

	iter.current = iter.txn.view[i]
	iter.key = iter.current.Key()
}
