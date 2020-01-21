package v1_1_0

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/gogo/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/deqdb/internal/data"
)

func TestPubTopicEvents(t *testing.T) {

	events := []*deq.Event{
		{
			ID:           "1",
			Topic:        "topic1",
			CreateTime:   time.Now(),
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
		{
			ID:           "2",
			Topic:        "topic1",
			CreateTime:   time.Now(),
			DefaultState: deq.StateQueuedLinear,
			State:        deq.StateQueuedLinear,
			Indexes:      []string{"abc", "def"},
		},
		{
			ID:           "1",
			Topic:        "topic2",
			CreateTime:   time.Now(),
			DefaultState: deq.StateOK,
			State:        deq.StateOK,
			Indexes:      []string{"abc", "def"},
		},
		{
			ID:           "3",
			Topic:        "topic3",
			CreateTime:   time.Now(),
			DefaultState: deq.StateInvalid,
			State:        deq.StateInvalid,
		},
	}

	ctx := context.Background()

	db := data.NewInMemoryDB()
	defer db.Close()

	// Setup DB
	{
		txn := db.NewTransaction(true)
		defer txn.Discard()
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

	// Run upgrade
	err := pubTopicEvents(ctx, db)
	if err != nil {
		t.Fatalf("pubTopicEvents: %v", err)
	}

	// Verify events are unchanged
	{
		txn := db.NewTransaction(false)
		defer txn.Discard()
		for _, e := range events {
			actual, err := data.GetEvent(txn, e.Topic, e.ID, "")
			if err != nil {
				t.Fatalf("verify events: get %q %q: %v", e.Topic, e.ID, err)
			}
			if !cmp.Equal(e, actual) {
				t.Fatalf("verify events:\n%s", cmp.Diff(e, actual))
			}
		}
	}

	// Verify topics were published
	{
		txn := db.NewTransaction(false)
		defer txn.Discard()

		for _, e := range events {
			actual, err := data.GetEvent(txn, e.Topic, e.ID, "")
			if err != nil {
				t.Fatalf("verify events: get %q %q: %v", e.Topic, e.ID, err)
			}
			if !cmp.Equal(e, actual) {
				t.Fatalf("verify events:\n%s", cmp.Diff(e, actual))
			}
		}
	}
}

func TestSplitChannelKeys(t *testing.T) {

	type ChannelItem struct {
		Key *data.ChannelKey
		Val *data.ChannelPayload
	}

	type Item struct {
		Key data.Key
		Val proto.Message
	}

	table := struct {
		Before []ChannelItem
		Expect []Item
	}{
		Before: []ChannelItem{
			{
				Key: &data.ChannelKey{
					Channel: "channel1",
					ID:      "1",
					Topic:   "topic1",
				},
				Val: &data.ChannelPayload{
					EventState:          data.EventState_QUEUED,
					DeprecatedSendCount: 0,
				},
			},
			{
				Key: &data.ChannelKey{
					Channel: "channel2",
					ID:      "1",
					Topic:   "topic1",
				},
				Val: &data.ChannelPayload{
					EventState:          data.EventState_QUEUED,
					DeprecatedSendCount: 1,
				},
			},
			{
				Key: &data.ChannelKey{
					Channel: "channel1",
					ID:      "2",
					Topic:   "topic1",
				},
				Val: &data.ChannelPayload{
					EventState:          data.EventState_QUEUED,
					DeprecatedSendCount: 100,
				},
			},
		},
		Expect: []Item{
			{
				Key: &data.SendCountKey{
					Channel: "channel1",
					ID:      "2",
					Topic:   "topic1",
				},
				Val: &data.SendCount{
					SendCount: 100,
				},
			},
			{
				Key: &data.SendCountKey{
					Channel: "channel2",
					ID:      "1",
					Topic:   "topic1",
				},
				Val: &data.SendCount{
					SendCount: 1,
				},
			},
			{
				Key: &data.ChannelKey{
					Channel: "channel1",
					ID:      "1",
					Topic:   "topic1",
				},
				Val: &data.ChannelPayload{
					EventState: data.EventState_QUEUED,
				},
			},
			{
				Key: &data.ChannelKey{
					Channel: "channel1",
					ID:      "2",
					Topic:   "topic1",
				},
				Val: &data.ChannelPayload{
					EventState: data.EventState_QUEUED,
				},
			},
			{
				Key: &data.ChannelKey{
					Channel: "channel2",
					ID:      "1",
					Topic:   "topic1",
				},
				Val: &data.ChannelPayload{
					EventState: data.EventState_QUEUED,
				},
			},
		},
	}

	// expectPayloads := map[data.ChannelKey]*data.ChannelPayload{
	// 	{
	// 		Channel: "channel1",
	// 		ID:      "1",
	// 		Topic:   "topic1",
	// 	}: {
	// 		EventState:          data.EventState_QUEUED,
	// 	},
	// }

	// expectSendCounts := map[data.SendCountKey]*data.SendCount{
	// 	{
	// 		Channel: "channel1",
	// 		ID:      "1",
	// 		Topic:   "topic1",
	// 	}, {
	// 		SendCount: 0,
	// 	},
	// }

	ctx := context.Background()

	db := data.NewInMemoryDB()
	defer db.Close()

	// Setup DB
	{
		txn := db.NewTransaction(true)
		defer txn.Discard()
		for _, item := range table.Before {
			err := data.SetChannelEvent(txn, item.Key, item.Val)
			if err != nil {
				t.Fatalf("write channel data %v: %v", item.Key, err)
			}
		}

		err := txn.Commit()
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// Run upgrade
	err := splitChannelKeys(ctx, db)
	if err != nil {
		t.Fatalf("splitChannelKeys: %v", err)
	}

	// Check results.
	{
		txn := db.NewTransaction(false)
		defer txn.Discard()

		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		it.Rewind()
		var actual []Item

		// Aggregate all items in the database.
		for it.Rewind(); it.Valid(); it.Next() {
			key, err := data.Unmarshal(it.Item().Key())
			if err != nil {
				t.Fatalf("unmarshal channel key: %v", err)
			}

			val := key.NewValue()
			err = data.Get(txn, key, val)
			if err != nil {
				t.Fatalf("verify: get value for key %+v: %v", key, err)
			}
			t.Log(Item{Key: key, Val: val})
			actual = append(actual, Item{
				Key: key,
				Val: val,
			})
		}
		// Verify aggregated results match expected results.
		if !cmp.Equal(table.Expect, actual) {
			t.Errorf("\n%s", cmp.Diff(table.Expect, actual))
		}
	}
}
