package deqdb

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/deqdb/internal/data"
)

var long bool

func init() {
	flag.BoolVar(&long, "long", false, "run long tests")
	flag.Parse()
}

type TestLogger struct {
	tb     testing.TB
	prefix string
}

func (l *TestLogger) Printf(format string, a ...interface{}) {
	l.tb.Helper()
	a = append([]interface{}{time.Now().Format("15:04:05.000000"), l.prefix}, a...)
	l.tb.Logf("%s %s: "+format, a...)
}

func newTestDB(tb testing.TB) (*Store, func()) {
	tb.Helper()
	memdb := data.NewInMemoryDB()

	info := &TestLogger{tb, "INFO"}
	debug := &TestLogger{tb, "DEBUG"}

	db, err := open(memdb, 40, false, info, debug)
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}

	return db, func() {
		tb.Helper()

		err := db.Close()
		if err != nil {
			tb.Fatal(err)
		}
	}
}

func TestDel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, discard := newTestDB(t)
	defer discard()

	expected := deq.Event{
		ID:           "event1",
		Topic:        "topic",
		CreateTime:   time.Now(),
		DefaultState: deq.StateQueued,
		State:        deq.StateInternal,
		SendCount:    1,
		Indexes:      []string{"abc", "123", "qwerty"},
	}

	events := []deq.Event{
		expected,
		{
			ID:           "event2",
			Topic:        "topic",
			CreateTime:   expected.CreateTime.Add(time.Millisecond),
			DefaultState: deq.StateQueued,
			State:        deq.StateInternal,
			SendCount:    1,
			Indexes:      []string{"abc", "def"},
		},
		{
			ID:           "event1",
			Topic:        "topic2",
			CreateTime:   time.Now(),
			DefaultState: deq.StateQueued,
			State:        deq.StateInternal,
			SendCount:    1,
			Indexes:      []string{"123"},
		},
	}

	channels := []string{
		"channel",
		"channel2",
		"channel4",
	}

	for _, e := range events {
		_, err := db.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
		// Make sure the event is sent on each channel so it has send count data saved in the database.
		for _, channel := range channels {
			c := db.Channel(channel, e.Topic)
			c.SetInitialResendDelay(time.Millisecond * 10)
			e, err := c.Next(ctx)
			if err != nil {
				t.Fatalf("create event %q %q: ensure send count incremented: sub: %v", e.Topic, e.ID, err)
			}
			err = c.SetEventState(ctx, e.ID, deq.StateInternal)
			if err != nil {
				t.Fatalf("create event %q %q: set state: %v", e.Topic, e.ID, err)
			}
			c.Close()
		}
	}

	err := db.Del(ctx, expected.Topic, expected.ID)
	if err != nil {
		t.Fatalf("del: %v", err)
	}

	channel := db.Channel("channel", expected.Topic)
	defer channel.Close()

	_, err = channel.Get(ctx, expected.ID)
	if err != nil && err != deq.ErrNotFound {
		t.Fatalf("get deleted: %v", err)
	}
	if err == nil {
		t.Errorf("returned deleted event")
	}

	txn := db.db.NewTransaction(false)

	// Ensure channel data was deleted.
	for i, channel := range channels {
		// Verify channel events were deleted
		key, err := data.ChannelKey{
			Channel: channel,
			Topic:   "topic",
			ID:      "event1",
		}.Marshal(nil)
		if err != nil {
			t.Fatalf("marshal channel key %d: %v", i, err)
		}
		_, err = txn.Get(key)
		if err != nil && err != badger.ErrKeyNotFound {
			t.Fatalf("get deleted channel event %d: %v", i, err)
		}
		if err == nil {
			t.Errorf("get deleted channel event %d: not deleted", i)
		}

		// Verify send counts were deleted
		sendCountKey := data.SendCountKey{
			Channel: "channel",
			Topic:   "topic",
			ID:      "event1",
		}
		key, err = sendCountKey.Marshal(nil)
		if err != nil {
			t.Fatalf("marshal send count key %d: %v", i, err)
		}
		_, err = txn.Get(key)
		if err != nil && err != badger.ErrKeyNotFound {
			t.Fatalf("get deleted send count %d: %v", i, err)
		}
		if err == nil {
			t.Errorf("get deleted send count %d: not deleted", i)
		}
	}

	// Ensure other events channel data wasn't modified.
	for _, e := range events {
		if cmp.Equal(e, expected) {
			continue
		}
		for _, channel := range channels {
			c := db.Channel(channel, e.Topic)
			actual, err := c.Get(ctx, e.ID)
			if err != nil {
				t.Fatalf("verify other event wasn't modified: get: %v", err)
			}
			if !cmp.Equal(e, actual) {
				t.Errorf("verify other event wasn't modified:\n%s", cmp.Diff(e, actual))
			}
			c.Close()
		}
	}

	// Verify uncovered indexes were deleted.
	for _, index := range []string{"123", "qwerty"} {
		key, err := data.IndexKey{
			Topic: expected.Topic,
			Value: index,
		}.Marshal(nil)
		if err != nil {
			t.Fatalf("marshal index key: %v", err)
		}

		_, err = txn.Get(key)
		if err != nil && err != badger.ErrKeyNotFound {
			t.Fatalf("get index: %v", err)
		}
		if err == nil {
			t.Errorf("got index for deleted event")
		}
	}

	expectedIndexPayload := &data.IndexPayload{
		EventId:    events[1].ID,
		CreateTime: events[1].CreateTime.UnixNano(),
	}

	// Verify covered indexes were not deleted.
	actualIndexPayload := new(data.IndexPayload)
	err = data.GetIndexPayload(txn, &data.IndexKey{
		Topic: expected.Topic,
		Value: "abc",
	}, actualIndexPayload)
	if err != nil {
		t.Fatalf("get covered index: %v", err)
	}
	if !cmp.Equal(expectedIndexPayload, actualIndexPayload) {
		t.Errorf("verify covered index: %v", cmp.Diff(expectedIndexPayload, actualIndexPayload))
	}
}

func TestPub(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, discard := newTestDB(t)
	defer discard()

	expected := deq.Event{
		ID:           "event1",
		Topic:        "topic",
		CreateTime:   time.Now(),
		DefaultState: deq.StateQueued,
		State:        deq.StateQueued,
		SendCount:    1,
	}

	channel := db.Channel("channel", expected.Topic)
	defer channel.Close()

	_, err := db.Pub(ctx, expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	event, err := channel.Next(context.Background())
	if err != nil {
		t.Fatalf("get next: %v", err)
	}
	if !cmp.Equal(event, expected) {
		t.Errorf("get next:\n%s", cmp.Diff(expected, event))
	}

	expected.State = deq.StateQueued

	event, err = channel.Get(ctx, expected.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// SendCount is only eventually consistent, so it might not have updated yet
	if event.SendCount < 0 || event.SendCount > 1 {
		t.Errorf("get: send count: want 0 or 1, got %d", event.SendCount)
	}
	expected.SendCount = event.SendCount
	if !cmp.Equal(event, expected) {
		t.Errorf("get:\n%s", cmp.Diff(expected, event))
	}
	if !cmp.Equal(event, expected) {
		t.Errorf("get:\n%s", cmp.Diff(expected, event))
	}
}

func TestMassPub(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, discard := newTestDB(t)
	defer discard()

	topic := "topic"
	// Round(0) to discard's leap-second info that's lost in serialization
	createTime := time.Now().Round(0)

	expected := make([]deq.Event, 500)
	for i := 0; i < 500; i++ {
		expected[i] = deq.Event{
			ID:           fmt.Sprintf("event%03d", i),
			Topic:        topic,
			CreateTime:   createTime,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		}
	}

	channel := db.Channel("channel", topic)
	defer channel.Close()

	for i, e := range expected {
		_, err := db.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub %d: %v", i, err)
		}
	}

	var actual []deq.Event
	iter := channel.NewEventIter(nil)
	for iter.Next(ctx) {
		actual = append(actual, iter.Event())
	}
	if iter.Err() != nil {
		t.Fatalf("iterate: %v", iter.Err())
	}
	if !cmp.Equal(expected, actual) {
		t.Errorf(":\n%s", cmp.Diff(expected, actual))
	}
}

func TestPubDuplicate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, discard := newTestDB(t)
	defer discard()

	want := deq.Event{
		ID:           "event1",
		Topic:        "topic",
		CreateTime:   time.Now(),
		DefaultState: deq.StateQueued,
		State:        deq.StateQueued,
		SendCount:    1,
	}

	channel := db.Channel("channel", want.Topic)
	defer channel.Close()

	// Publish the event
	_, err := db.Pub(ctx, want)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	// Publish and verify event with same id and payload
	_, err = db.Pub(ctx, want)
	if err != nil {
		t.Fatalf("identical duplicate pub: %v", err)
	}

	// Publish and verify event with same id and different payload
	want.Payload = []byte{1}
	_, err = db.Pub(ctx, want)
	if err != deq.ErrAlreadyExists {
		t.Fatalf("modified duplicate pub: %v", err)
	}
	want.Payload = nil

	got, err := channel.Next(context.Background())
	if err != nil {
		t.Fatalf("get next: %v", err)
	}
	if !cmp.Equal(want, got) {
		t.Errorf("get next:\n%s", cmp.Diff(want, got))
	}
}

func TestMassPublish(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, discard := newTestDB(t)
	defer discard()

	for i := 0; i <= 500; i++ {
		id := fmt.Sprintf("%.3d", i)
		_, err := db.Pub(ctx, deq.Event{
			ID:      id,
			Topic:   "TestMassPublish",
			Payload: []byte(id),
		})
		if err != nil {
			t.Fatalf("create event %d: %v", i, err)
		}
	}

	channel := db.Channel("Channel1", "TestMassPublish")
	defer channel.Close()

	channel.SetIdleTimeout(time.Second / 3)

	var events1 []deq.Event
	err := channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {
		events1 = append(events1, e)
		return nil, nil
	})
	if err != nil {
		t.Fatalf("streaming events before publishing: %v", err)
	}

	for i := 500; i < 1000; i++ {
		id := fmt.Sprintf("%.3d", i)
		_, err = db.Pub(ctx, deq.Event{
			ID:      id,
			Topic:   "TestMassPublish",
			Payload: []byte(id),
		})
		if err != nil {
			t.Fatalf("Error Creating Event: %v", err)
		}
	}

	var events2 []deq.Event
	err = channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {
		events1 = append(events1, e)
		return nil, nil
	})
	if err != nil {
		t.Fatalf("streaming events after publishing: %v", err)
	}

	events := append(events1, events2...)

	var missed []int
outer:
	for i := 0; i < 1000; i++ {
		for _, m := range events {
			if string(m.Payload) == fmt.Sprintf("%.3d", i) {
				continue outer
			}
		}
		missed = append(missed, i)
	}

	if len(missed) > 0 {
		t.Fatalf("Missed messages: %v", missed)
	}
}
