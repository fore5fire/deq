package deqdb

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/deqerr"
)

func TestSyncTo(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB(t)
	defer discard()
	db2, discard2 := newTestDB(t)
	defer discard2()

	// Round(0) gets rid of leap-second info, which will be lost in serialization
	createTime := time.Now().Round(0)

	// Publish some events
	events := struct {
		Before, After, Expected []deq.Event
	}{
		Before: []deq.Event{
			{
				ID:         "before-event1",
				Topic:      "TopicA",
				CreateTime: createTime,
			},
			{
				ID:         "before-event2",
				Topic:      "TopicA",
				CreateTime: createTime,
			},
			{
				ID:         "before-event1",
				Topic:      "TopicB",
				CreateTime: createTime,
			},
		},
		After: []deq.Event{
			{
				ID:         "after-event1",
				Topic:      "TopicA",
				CreateTime: createTime,
			},
			{
				ID:         "after-event2",
				Topic:      "TopicA",
				CreateTime: createTime,
			},
			{
				ID:         "after-event1",
				Topic:      "TopicB",
				CreateTime: createTime,
			},
		},
		Expected: []deq.Event{
			{
				ID:           "after-event1",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
				SendCount:    1,
				Selector:     "after-event1",
			},
			{
				ID:           "after-event2",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
				SendCount:    1,
				Selector:     "after-event2",
			},
			{
				ID:           "before-event1",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
				SendCount:    1,
				Selector:     "before-event1",
			},
			{
				ID:           "before-event2",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
				SendCount:    1,
				Selector:     "before-event2",
			},
		},
	}

	done := make(chan bool)
	done2 := make(chan bool)
	received := make(chan deq.Event)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Publish before events
	for _, e := range events.Before {
		_, err := db.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	// Sync events
	channel := db.Channel("test-channel", "TopicA")
	defer channel.Close()
	go func() {
		defer close(done)

		err := deq.SyncTo(ctx, AsClient(db2), channel)
		if deqerr.GetCode(err) != deqerr.Canceled && err != ErrChannelClosed {
			t.Errorf("sync: %v", err)
		}
	}()

	// Subscribe to synced events
	subChannel := db2.Channel("test-channel-remote", "TopicA")
	defer subChannel.Close()
	go func() {
		defer close(done2)

		err := subChannel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {

			received <- e

			return nil, nil
		})
		if deqerr.GetCode(err) != deqerr.Canceled && err != ErrChannelClosed {
			t.Errorf("sub: %v", err)
		}
	}()

	// Publish some more events now that we're syncing
	for _, e := range events.After {
		_, err := db.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	// Verify that events were received by synced database
	var actual []deq.Event
	for e := range received {
		actual = append(actual, e)
		if len(actual) >= len(events.Expected) {
			break
		}
	}

	sort.Slice(actual, func(i, j int) bool {
		return actual[i].ID < actual[j].ID
	})

	if !cmp.Equal(events.Expected, actual) {
		t.Errorf("received events:\n%s", cmp.Diff(events.Expected, actual))
	}

	// Verify that subscriptions close correctly.
	cancel()
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("close subs: timed out")
		}
	}
}
