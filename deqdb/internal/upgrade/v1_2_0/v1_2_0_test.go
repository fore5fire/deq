package v1_2_0

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/deqdb/internal/data"
)

func TestDeletedOrphaned(t *testing.T) {

	now := time.Now()

	events := []*deq.Event{
		{
			ID:           "2",
			Topic:        "topic1",
			CreateTime:   now,
			DefaultState: deq.StateQueuedLinear,
			State:        deq.StateQueuedLinear,
			Indexes:      []string{"abc", "def", "123"},
		},
		{
			ID:           "1",
			Topic:        "topic2",
			CreateTime:   now,
			DefaultState: deq.StateOK,
			State:        deq.StateOK,
			Indexes:      []string{"abc"},
		},
	}

	orphaned := map[*data.IndexKey]*data.IndexPayload{
		{Topic: "topic1", Value: "abc"}: {EventId: "5", CreateTime: now.Add(time.Second).UnixNano()},
		{Topic: "topic1", Value: "def"}: {EventId: "5", CreateTime: now.Add(time.Second).UnixNano()},
	}

	nonorphaned := map[*data.IndexKey]*data.IndexPayload{
		{Topic: "topic1", Value: "123"}: {EventId: "2", CreateTime: now.UnixNano()},
		{Topic: "topic2", Value: "abc"}: {EventId: "1", CreateTime: now.UnixNano()},
	}

	ctx := context.Background()

	db := data.NewInMemoryDB()
	defer db.Close()

	// Setup DB
	{
		txn := db.NewTransaction(true)
		defer txn.Discard()
		// Write orphaned indexes
		for key, val := range orphaned {
			keybuf, err := key.Marshal(nil)
			if err != nil {
				t.Fatalf("marshal index key: %v", err)
			}
			valbuf, err := val.Marshal()
			if err != nil {
				t.Fatalf("marshal index payload: %v", err)
			}
			err = txn.Set(keybuf, valbuf)
			if err != nil {
				t.Fatalf("set index: %v", err)
			}
		}
		// Write events (including non-orphaned indexes)
		for _, e := range events {
			err := data.WriteEvent(txn, e)
			if err != nil {
				t.Fatalf("write event: %v", err)
			}
		}

		err := txn.Commit()
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// Run upgrade.
	err := deleteOrphanedIndexes(ctx, db)
	if err != nil {
		t.Fatalf("deleteOrphanedIndexes: %v", err)
	}

	{
		txn := db.NewTransaction(false)
		defer txn.Discard()

		// Verify non-orphaned indexes are unchanged.
		for key, val := range nonorphaned {
			keybuf, _ := key.Marshal(nil)
			buf, err := txn.Get(keybuf)
			if err != nil {
				t.Fatalf("get non-orhpaned index %q %q: %v", key.Topic, key.Value, err)
			}
			actual := new(data.IndexPayload)
			err = buf.Value(func(data []byte) error {
				return proto.Unmarshal(data, actual)
			})
			if err != nil {
				t.Fatalf("get non-orhpaned index %q %q: %v", key.Topic, key.Value, err)
			}
			if !cmp.Equal(val, actual) {
				t.Errorf("get non-orhpaned index %q %q:\n%s", key.Topic, key.Value, cmp.Diff(val, actual))
			}
		}

		// Verify orphaned indexes were deleted.
		for key := range orphaned {
			keybuf, _ := key.Marshal(nil)
			_, err := txn.Get(keybuf)
			if err != nil && err != badger.ErrKeyNotFound {
				t.Fatalf("get deleted index %q %q: %v", key.Topic, key.Value, err)
			}
			if err == nil {
				t.Errorf("get deleted index %q %q: found", key.Topic, key.Value)
			}
		}
	}
}
