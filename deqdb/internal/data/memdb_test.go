package data

import (
	"testing"

	"github.com/dgraph-io/badger/v2"

	"github.com/google/go-cmp/cmp"
)

func TestMemDBInsert(t *testing.T) {
	t.Parallel()

	expect := map[string]string{
		"abc": "123",
		"aaa": "111",
		"def": "ghi",
	}

	db := NewInMemoryDB()
	defer db.Close()

	// Insert values
	{
		txn := db.NewTransaction(true)
		defer txn.Discard()

		for keyString, valString := range expect {
			key, val := []byte(keyString), []byte(valString)

			// Write the value.
			err := txn.Set(key, val)
			if err != nil {
				t.Fatalf("set %s=%s: %v", key, val, err)
			}

			// Read the value back.
			actual, err := txn.Get(key)
			if err != nil {
				t.Fatalf("get %s before commit: %v", key, err)
			}
			if !cmp.Equal(key, actual.Key()) {
				t.Errorf("get %s before commit: verify key: \n%s", key, cmp.Diff(key, actual.Key()))
			}
			err = actual.Value(func(actualVal []byte) error {
				if !cmp.Equal(val, actualVal) {
					t.Errorf("get %s before commit: verify value: \n%s", key, cmp.Diff(val, actualVal))
				}
				return nil
			})
			if err != nil {
				t.Fatalf("get %s before commit: unwrap value: %v", key, err)
			}
		}

		// Verify no changes have been made to other transactions yet.
		{
			txn := db.NewTransaction(false)
			defer txn.Discard()

			for keyString := range expect {
				key := []byte(keyString)

				item, err := txn.Get(key)
				if err == nil {
					t.Errorf("get %s from different txn before commit: got item %s", key, item.Key())
				} else if err != badger.ErrKeyNotFound {
					t.Fatalf("get %s from different txn before commit: %v", key, err)
				}
			}
		}

		err := txn.Commit()
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// Read back written values in a different transaction.
	{
		txn := db.NewTransaction(false)
		defer txn.Discard()

		for keyString, valString := range expect {
			key, val := []byte(keyString), []byte(valString)

			actual, err := txn.Get(key)
			if err != nil {
				t.Fatalf("get %s: %v", key, err)
			}
			if !cmp.Equal(key, actual.Key()) {
				t.Errorf("get %s: verify key: \n%s", key, cmp.Diff(key, actual.Key()))
			}
			actualVal, err := actual.ValueCopy(nil)
			if err != nil {
				t.Fatalf("get %s: unwrap value: %v", key, err)
			}
			if !cmp.Equal(val, actualVal) {
				t.Errorf("get %s: verify value: \n%s", key, cmp.Diff(val, actualVal))
			}
		}
	}
}

func TestMemDBModify(t *testing.T) {
	t.Parallel()

	initial := map[string]string{
		"abc": "123",
		"aaa": "111",
		"def": "ghi",
	}

	middle := map[string]string{
		"abc": "aaa",
		"aaa": "aaa",
		"def": "bbb",
	}

	final := map[string]string{
		"abc": "123",
		"aaa": "111",
		"def": "000",
	}

	db := NewInMemoryDB()
	defer db.Close()

	// Insert values
	{
		txn := db.NewTransaction(true)
		defer txn.Discard()

		// Write the initial values.
		for keyString, valString := range initial {
			key, val := []byte(keyString), []byte(valString)

			err := txn.Set(key, val)
			if err != nil {
				t.Fatalf("set %s=%s: %v", key, val, err)
			}
		}

		// Update the values before committing them.
		for keyString, valString := range middle {
			key, val := []byte(keyString), []byte(valString)

			// Write the value.
			err := txn.Set(key, val)
			if err != nil {
				t.Fatalf("set %s=%s: %v", key, val, err)
			}

			// Read the value back.
			actual, err := txn.Get(key)
			if err != nil {
				t.Fatalf("get %s before commit: %v", key, err)
			}
			if !cmp.Equal(key, actual.Key()) {
				t.Errorf("get %s before commit: verify key: \n%s", key, cmp.Diff(key, actual.Key()))
			}
			actualVal, err := actual.ValueCopy(nil)
			if err != nil {
				t.Fatalf("get %s before commit: unwrap value: %v", key, err)
			}
			if !cmp.Equal(val, actualVal) {
				t.Errorf("get %s before commit: verify value: \n%s", key, cmp.Diff(val, actualVal))
			}
		}

		// Verify no changes have been made to other transactions yet.
		{
			txn := db.NewTransaction(false)
			defer txn.Discard()

			for keyString := range middle {
				key := []byte(keyString)

				item, err := txn.Get(key)
				if err == nil {
					t.Errorf("get %s from different txn before commit: got item %s", key, item.Key())
				} else if err != badger.ErrKeyNotFound {
					t.Fatalf("get %s from different txn before commit: %v", key, err)
				}
			}
		}

		err := txn.Commit()
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// Modify the values from a different transaction.
	{
		txn := db.NewTransaction(true)
		defer txn.Discard()

		// First verify the existing values.
		for keyString, valString := range middle {
			key, val := []byte(keyString), []byte(valString)

			actual, err := txn.Get(key)
			if err != nil {
				t.Fatalf("get %s before commit: %v", key, err)
			}
			if !cmp.Equal(key, actual.Key()) {
				t.Errorf("get %s before commit: verify key: \n%s", key, cmp.Diff(key, actual.Key()))
			}
			actualVal, err := actual.ValueCopy(nil)
			if err != nil {
				t.Fatalf("get %s before commit: unwrap value: %v", key, err)
			}
			if !cmp.Equal(val, actualVal) {
				t.Errorf("get %s before commit: verify value: \n%s", key, cmp.Diff(val, actualVal))
			}
		}

		// Now write and verify the final values.
		for keyString, valString := range final {
			key, val := []byte(keyString), []byte(valString)

			// Write the value.
			err := txn.Set(key, val)
			if err != nil {
				t.Fatalf("set %s=%s: %v", key, val, err)
			}

			// Read the value back.
			actual, err := txn.Get(key)
			if err != nil {
				t.Fatalf("get %s: %v", key, err)
			}
			if !cmp.Equal(key, actual.Key()) {
				t.Errorf("get %s: verify key: \n%s", key, cmp.Diff(key, actual.Key()))
			}
			actualVal, err := actual.ValueCopy(nil)
			if err != nil {
				t.Fatalf("get %s: unwrap value: %v", key, err)
			}
			if !cmp.Equal(val, actualVal) {
				t.Errorf("get %s: verify value: \n%s", key, cmp.Diff(val, actualVal))
			}
		}

		err := txn.Commit()
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// Read the final values from a different transaction.
	{
		txn := db.NewTransaction(false)
		defer txn.Discard()

		for keyString, valString := range final {
			key, val := []byte(keyString), []byte(valString)

			actual, err := txn.Get(key)
			if err != nil {
				t.Fatalf("get %s: %v", key, err)
			}
			if !cmp.Equal(key, actual.Key()) {
				t.Errorf("get %s: verify key: \n%s", key, cmp.Diff(key, actual.Key()))
			}
			actualVal, err := actual.ValueCopy(nil)
			if err != nil {
				t.Fatalf("get %s: unwrap value: %v", key, err)
			}
			if !cmp.Equal(val, actualVal) {
				t.Errorf("get %s: verify value: \n%s", key, cmp.Diff(val, actualVal))
			}
		}
	}
}

func TestMemDBIter(t *testing.T) {
	t.Parallel()

	type Item struct {
		Key, Val string
	}

	expect := []Item{
		{
			Key: "aaa", Val: "111",
		},
		{
			Key: "abc", Val: "123",
		},
		{
			Key: "def", Val: "ghi",
		},
	}

	db := NewInMemoryDB()
	defer db.Close()

	// Write and iterate values
	{
		txn := db.NewTransaction(true)
		defer txn.Discard()

		for _, item := range expect {
			key, val := []byte(item.Key), []byte(item.Val)

			// Write the value.
			err := txn.Set(key, val)
			if err != nil {
				t.Fatalf("set %s=%s: %v", key, val, err)
			}
		}

		it := txn.NewIterator(badger.IteratorOptions{})
		var actual []Item

		for it.Rewind(); it.Valid(); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				t.Fatalf("iterate values: get value for %s: %v", it.Item().Key(), err)
			}
			actual = append(actual, Item{
				Key: string(it.Item().KeyCopy(nil)),
				Val: string(val),
			})
		}

		if !cmp.Equal(expect, actual) {
			t.Errorf("iterate values:\n%s", cmp.Diff(expect, actual))
		}

		err := txn.Commit()
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// Iterate written values from another transaction
	{
		txn := db.NewTransaction(false)
		defer txn.Discard()

		it := txn.NewIterator(badger.IteratorOptions{})

		var actual []Item

		for it.Rewind(); it.Valid(); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				t.Fatalf("iterate written values: get value for %s: %v", it.Item().Key(), err)
			}
			actual = append(actual, Item{
				Key: string(it.Item().KeyCopy(nil)),
				Val: string(val),
			})
		}

		if !cmp.Equal(expect, actual) {
			t.Errorf("iterate written values:\n%s", cmp.Diff(expect, actual))
		}
	}
}

func TestMemDBIterReversed(t *testing.T) {
	t.Parallel()

	type Item struct {
		Key, Val string
	}

	expect := []Item{
		{
			Key: "def", Val: "ghi",
		},
		{
			Key: "abc", Val: "123",
		},
		{
			Key: "aaa", Val: "111",
		},
	}

	db := NewInMemoryDB()
	defer db.Close()

	// Write and iterate values
	{
		txn := db.NewTransaction(true)
		defer txn.Discard()

		for _, item := range expect {
			key, val := []byte(item.Key), []byte(item.Val)

			// Write the value.
			err := txn.Set(key, val)
			if err != nil {
				t.Fatalf("set %s=%s: %v", key, val, err)
			}
		}

		it := txn.NewIterator(badger.IteratorOptions{
			Reverse: true,
		})
		var actual []Item

		for it.Rewind(); it.Valid(); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				t.Fatalf("iterate values: get value for %s: %v", it.Item().Key(), err)
			}
			actual = append(actual, Item{
				Key: string(it.Item().KeyCopy(nil)),
				Val: string(val),
			})
		}

		if !cmp.Equal(expect, actual) {
			t.Errorf("iterate values:\n%s", cmp.Diff(expect, actual))
		}

		err := txn.Commit()
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// Iterate written values from another transaction
	{
		txn := db.NewTransaction(false)
		defer txn.Discard()

		it := txn.NewIterator(badger.IteratorOptions{
			Reverse: true,
		})

		var actual []Item

		for it.Rewind(); it.Valid(); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				t.Fatalf("iterate written values: get value for %s: %v", it.Item().Key(), err)
			}
			actual = append(actual, Item{
				Key: string(it.Item().KeyCopy(nil)),
				Val: string(val),
			})
		}

		if !cmp.Equal(expect, actual) {
			t.Errorf("iterate written values:\n%s", cmp.Diff(expect, actual))
		}
	}
}

func TestMemDBDelete(t *testing.T) {
	t.Parallel()

	expect := map[string]string{
		"abc": "123",
		"aaa": "111",
		"def": "ghi",
	}

	db := NewInMemoryDB()
	defer db.Close()

	// Insert values
	{
		txn := db.NewTransaction(true)
		defer txn.Discard()

		// Write the values.
		for keyString, valString := range expect {
			key, val := []byte(keyString), []byte(valString)

			err := txn.Set(key, val)
			if err != nil {
				t.Fatalf("set %s=%s: %v", key, val, err)
			}
		}

		// Delete the values before committing them.
		for keyString := range expect {
			key := []byte(keyString)

			// Delete the value.
			err := txn.Delete(key)
			if err != nil {
				t.Fatalf("delete %s: %v", key, err)
			}

			// Verify it was deleted.
			_, err = txn.Get(key)
			if err != nil && err != badger.ErrKeyNotFound {
				t.Fatalf("read deleted %q: %v", key, err)
			}
			if err == nil {
				t.Errorf("read deleted %q: returned deleted value", key)
			}
		}

		// Rewrite the values.
		for keyString, valString := range expect {
			key, val := []byte(keyString), []byte(valString)

			err := txn.Set(key, val)
			if err != nil {
				t.Fatalf("set %s=%s: %v", key, val, err)
			}
		}

		err := txn.Commit()
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// Delete the values in a different transaction.
	{
		txn := db.NewTransaction(true)
		defer txn.Discard()

		// First verify the commited values.
		for keyString, valString := range expect {
			key, val := []byte(keyString), []byte(valString)

			actual, err := txn.Get(key)
			if err != nil {
				t.Fatalf("get %s before commit: %v", key, err)
			}
			if !cmp.Equal(key, actual.Key()) {
				t.Errorf("get %s before commit: verify key: \n%s", key, cmp.Diff(key, actual.Key()))
			}
			actualVal, err := actual.ValueCopy(nil)
			if err != nil {
				t.Fatalf("get %s before commit: unwrap value: %v", key, err)
			}
			if !cmp.Equal(val, actualVal) {
				t.Errorf("get %s before commit: verify value: \n%s", key, cmp.Diff(val, actualVal))
			}
		}

		// Delete the committed values.
		for keyString := range expect {
			key := []byte(keyString)

			// Delete the value.
			err := txn.Delete(key)
			if err != nil {
				t.Fatalf("delete %s: %v", key, err)
			}

			// Verify it was deleted.
			_, err = txn.Get(key)
			if err != nil && err != badger.ErrKeyNotFound {
				t.Fatalf("read deleted %q: %v", key, err)
			}
			if err == nil {
				t.Errorf("read deleted %q: returned deleted value", key)
			}
		}

		err := txn.Commit()
		if err != nil {
			t.Fatalf("commit: %v", err)
		}

	}

	// Verify the values are deleted from another transaction.
	{
		txn := db.NewTransaction(true)
		defer txn.Discard()

		for keyString := range expect {
			key := []byte(keyString)
			_, err := txn.Get(key)
			if err != nil && err != badger.ErrKeyNotFound {
				t.Fatalf("read deleted %q: %v", key, err)
			}
			if err == nil {
				t.Errorf("read deleted %q: returned deleted value", key)
			}
		}
	}
}

func TestMemDBConflict(t *testing.T) {

}
