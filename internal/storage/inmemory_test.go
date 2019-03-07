package kvstore

import (
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/google/go-cmp/cmp"
)

func TestIter(t *testing.T) {
	expected := map[string]string{
		"abc":  "def",
		"aaaa": "b",
	}
	db := NewInMemoryDB(expected)
	defer db.Close()

	txn := db.NewTransaction(false)
	defer txn.Discard()

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		it.Next()
	}
}

func TestSeek(t *testing.T) {

	expected := struct{ Keys, Vals []string }{
		Keys: []string{
			"1",
			"2",
			"3",
			"4",
			"5",
			"6",
			"7",
			"8",
			"9",
		},
		Vals: []string{
			"a",
			"bb",
			"ccc",
			"dddd",
			"eeeee",
			"ffff",
			"ggg",
			"hh",
			"",
		},
	}

	initData := map[string]string{
		"1": "a", "11": "a",
		"2": "bb", "22": "bb",
		"3": "ccc", "33": "ccc",
		"4": "dddd", "44": "dddd",
		"5": "eeeee", "55": "eeeee",
		"6": "ffff", "66": "ffff",
		"7": "ggg", "77": "ggg",
		"8": "hh", "88": "hh",
		"9": "", "99": "",
	}

	db := NewInMemoryDB(initData)
	defer db.Close()

	txn := db.NewTransaction(false)
	defer txn.Discard()

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	var actual struct{ Keys, Vals []string }
	var cur []byte
	for it.Seek(cur); it.Valid(); it.Seek(cur) {
		item := it.Item()
		cur = item.KeyCopy(cur)
		cur[0]++

		val, err := item.Value()
		if err != nil {
			t.Fatalf("get value: %v", err)
		}
		actual.Keys = append(actual.Keys, string(item.Key()))
		actual.Vals = append(actual.Vals, string(val))
	}

	if !cmp.Equal(expected.Keys, actual.Keys) {
		t.Errorf("check keys:\n%v", cmp.Diff(expected.Keys, actual.Keys))
	}
	if !cmp.Equal(expected.Vals, actual.Vals) {
		t.Errorf("check values:\n%v", cmp.Diff(expected.Vals, actual.Vals))
	}
}

func TestGet(t *testing.T) {
	key, expected := []byte("abc"), []byte("def")
	db := NewInMemoryDB(map[string]string{
		string(key): string(expected),
	})
	defer db.Close()

	txn := db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get([]byte("abc"))
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	actual, err := item.Value()
	if err != nil {
		t.Fatalf("get value: %v", err)
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
	if !cmp.Equal(item.Key(), key) {
		t.Errorf("\n%s", cmp.Diff(key, item.Key()))
	}
}

// func TestSet(t *testing.T) {
// 	db := NewInMemoryDB(nil)
// 	defer db.Close()

// 	txn := db.NewTransaction(true)
// 	defer txn.Discard()

// 	expected := struct{ Keys, Values []string }{
// 		Keys:   []string{"abc", "aaaa", "d", ""},
// 		Values: []string{"123", "1234", "1", "12"},
// 	}

// 	for i, k := range expected.Keys {
// 		err := txn.Set([]byte(k), []byte(expected.Values[i]))
// 		if err != nil {
// 			t.Fatalf("set: %v", err)
// 		}
// 	}

// 	for i, item := iterator.It
// }
