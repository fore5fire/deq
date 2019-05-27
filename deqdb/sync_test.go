package deqdb

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/ack"
)

func TestSyncTo(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()
	db2, discard2 := newTestDB()
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
			},
			{
				ID:           "after-event2",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
			},
			{
				ID:           "before-event1",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
			},
			{
				ID:           "before-event2",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
			},
		},
	}

	errc := make(chan error)
	errc2 := make(chan error)
	recieved := make(chan deq.Event)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		// check err channels
		err := <-errc
		if err != ctx.Err() {
			t.Errorf("sync: %v", err)
		}
		err = <-errc2
		if err != ctx.Err() {
			t.Errorf("sub: %v", err)
		}
	}()

	// Publish before events
	for _, e := range events.Before {
		_, err := db.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	// Sync events
	go func() {
		defer close(errc)

		channel := db.Channel("test-channel", "TopicA")
		defer channel.Close()

		errc <- deq.SyncTo(ctx, AsClient(db2), channel)
	}()

	// Subscribe to synced events
	go func() {
		defer close(errc2)

		channel := db2.Channel("test-channel-remote", "TopicA")
		defer channel.Close()

		errc2 <- channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, ack.Code) {

			recieved <- e

			return nil, ack.DequeueOK
		})
	}()

	// Publish some more events now that we're syncing
	for _, e := range events.After {
		_, err := db.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	// Verify that events were recieved by synced database
	var actual []deq.Event
	for e := range recieved {
		actual = append(actual, e)
		if len(actual) >= len(events.Expected) {
			break
		}
	}

	sort.Slice(actual, func(i, j int) bool {
		return actual[i].ID < actual[j].ID
	})

	if !cmp.Equal(events.Expected, actual) {
		t.Errorf("recieved events:\n%s", cmp.Diff(events.Expected, actual))
	}
}
