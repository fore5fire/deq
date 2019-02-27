package deq

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq/ack"
)

func TestSub(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Round(0) gets rid of leap-second info, which will be lost in serialization
	createTime := time.Now().Round(0)

	// Publish some events
	events := struct {
		Before, After, ExpectedBefore, ExpectedAfter, ExpectedResponses []Event
	}{
		Before: []Event{
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
		ExpectedBefore: []Event{
			{
				ID:           "before-event1",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: EventStateQueued,
				State:        EventStateQueued,
			},
			{
				ID:           "before-event2",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: EventStateQueued,
				State:        EventStateQueued,
			},
		},
		After: []Event{
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
		ExpectedAfter: []Event{
			{
				ID:           "after-event1",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: EventStateQueued,
				State:        EventStateQueued,
			},
			{
				ID:           "after-event2",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: EventStateQueued,
				State:        EventStateQueued,
			},
		},
		ExpectedResponses: []Event{
			{
				ID:           "before-event1",
				Topic:        "Response-TopicA",
				CreateTime:   createTime,
				DefaultState: EventStateQueued,
				State:        EventStateQueued,
			},
			{
				ID:           "before-event2",
				Topic:        "Response-TopicA",
				CreateTime:   createTime,
				DefaultState: EventStateQueued,
				State:        EventStateQueued,
			},
			{
				ID:           "after-event1",
				Topic:        "Response-TopicA",
				CreateTime:   createTime,
				DefaultState: EventStateQueued,
				State:        EventStateQueued,
			},
			{
				ID:           "after-event2",
				Topic:        "Response-TopicA",
				CreateTime:   createTime,
				DefaultState: EventStateQueued,
				State:        EventStateQueued,
			},
		},
	}

	for _, e := range events.Before {
		_, err := db.Pub(e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	errc := make(chan error)
	recieved := make(chan Event)
	responses := make(chan Event)

	// Subscribe to events
	go func() {
		channel := db.Channel("test-channel", "TopicA")
		defer channel.Close()

		errc <- channel.Sub(ctx, func(e Event) (*Event, ack.Code) {

			recieved <- e

			return &Event{
				ID:         e.ID,
				Topic:      "Response-TopicA",
				CreateTime: createTime,
			}, ack.DequeueOK
		})
	}()

	// Subscribe to response events
	go func() {
		channel := db.Channel("test-channel", "Response-TopicA")
		defer channel.Close()

		errc <- channel.Sub(ctx, func(e Event) (*Event, ack.Code) {

			responses <- e

			return nil, ack.DequeueOK
		})
	}()

	// Check Sub error just before we cancel the context
	defer func() {
		select {
		case err := <-errc:
			t.Errorf("subscribe: %v", err)
		default:
		}
	}()

	// Verify that events were recieved by handler
	var actual []Event
	for e := range recieved {
		actual = append(actual, e)
		if len(actual) >= len(events.ExpectedBefore) {
			break
		}
	}
	if !cmp.Equal(events.ExpectedBefore, actual) {
		t.Errorf("pre-sub recieved events:\n%s", cmp.Diff(events.ExpectedBefore, actual))
	}

	// Publish some more events now that we're already subscribed
	for _, e := range events.After {
		_, err := db.Pub(e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	// Verify that events were recieved by handler
	actual = nil
	for e := range recieved {
		actual = append(actual, e)
		if len(actual) >= len(events.ExpectedAfter) {
			break
		}
	}
	if !cmp.Equal(events.ExpectedAfter, actual) {
		t.Errorf("post-sub recieved events:\n%s", cmp.Diff(events.ExpectedAfter, actual))
	}

	// Verify that response events were published
	actual = nil
	for e := range responses {
		actual = append(actual, e)
		if len(actual) >= len(events.ExpectedResponses) {
			break
		}
	}
	if !cmp.Equal(events.ExpectedResponses, actual) {
		t.Errorf("response events:\n%s", cmp.Diff(events.ExpectedResponses, actual))
	}
}

func TestAwait(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	expected := Event{
		ID:           "event1",
		Topic:        "test-topic",
		CreateTime:   time.Now(),
		DefaultState: EventStateQueued,
		State:        EventStateQueued,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	channel := db.Channel("test-channel", expected.Topic)
	defer channel.Close()

	type AwaitResponse struct {
		Event Event
		Err   error
	}
	recieved := make(chan AwaitResponse)

	go func() {
		e, err := channel.Await(ctx, expected.ID)
		recieved <- AwaitResponse{
			Event: e,
			Err:   err,
		}
	}()
	time.Sleep(time.Millisecond * 50)
	_, err := db.Pub(expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	response := <-recieved
	if response.Err != nil {
		t.Fatalf("await before pub: %v", response.Err)
	}
	if !cmp.Equal(expected, response.Event) {
		t.Errorf("await before pub:\n%s", cmp.Diff(expected, response.Event))
	}

	e, err := channel.Await(ctx, expected.ID)
	if err != nil {
		t.Fatalf("await after pub: %v", err)
	}
	if !cmp.Equal(expected, e) {
		t.Errorf("await after pub:\n%s", cmp.Diff(expected, e))
	}
}

func TestAwaitChannelTimeout(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second/4)
	defer cancel()

	_, err := db.Pub(Event{
		ID:         "event1",
		Topic:      "topic",
		CreateTime: time.Now(),
	})
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	channel := db.Channel("channel", "topic")
	sub := channel.NewEventStateSubscription("event1")
	defer sub.Close()

	_, err = sub.Next(ctx)
	if err == nil {
		t.Fatalf("await dequeue returned without dequeue")
	}
	if err != ctx.Err() {
		t.Errorf("await dequeue: %v", err)
	}
}

func TestAwaitChannelClose(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	ctx := context.Background()

	_, err := db.Pub(Event{
		ID:         "event1",
		Topic:      "topic",
		CreateTime: time.Now(),
	})
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	channel := db.Channel("channel", "topic")
	sub := channel.NewEventStateSubscription("event1")
	go func() {
		time.Sleep(time.Second / 4)
		sub.Close()
	}()

	_, err = sub.Next(ctx)
	if err == nil {
		t.Fatalf("await dequeue returned without dequeue")
	}
	if err != ErrSubscriptionClosed {
		t.Errorf("await dequeue: %v", err)
	}
}

func TestAwaitChannel(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	_, err := db.Pub(Event{
		ID:         "event1",
		Topic:      "topic",
		CreateTime: time.Now(),
	})
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	go func() {
		time.Sleep(time.Second / 4)
		err := db.Channel("channel", "topic").SetEventState("event1", EventStateDequeuedOK)
		if err != nil {
			log.Printf("set event state: %v", err)
		}
	}()

	channel := db.Channel("channel", "topic")
	sub := channel.NewEventStateSubscription("event1")
	defer sub.Close()

	state, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("await dequeue: %v", err)
	}
	if state != EventStateDequeuedOK {
		t.Fatalf("returned incorrect state: %v", state)
	}
}

func TestGet(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	expected := Event{
		ID:         "event1",
		Topic:      "topic",
		CreateTime: time.Now(),
	}

	_, err := db.Pub(expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	expected.DefaultState = EventStateQueued
	expected.State = EventStateQueued

	channel := db.Channel("channel", expected.Topic)

	actual, err := channel.Get(expected.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}

	err = channel.SetEventState(expected.ID, EventStateDequeuedOK)
	if err != nil {
		t.Fatalf("set event state: %v", err)
	}

	expected.State = EventStateDequeuedOK

	actual, err = channel.Get(expected.ID)
	if err != nil {
		t.Fatalf("get after set state: %v", err)
	}
	if !cmp.Equal(actual, expected) {
		t.Errorf("get after set state:\n%s", cmp.Diff(expected, actual))
	}
}

func TestDequeue(t *testing.T) {
	t.Parallel()

	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	expected := Event{
		ID:         "event1",
		Topic:      "topic",
		CreateTime: time.Now(),
		State:      EventStateQueued,
	}

	_, err = db.Pub(expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	channel := db.Channel("channel", expected.Topic)

	err = channel.SetEventState(expected.ID, EventStateDequeuedError)
	if err != nil {
		t.Fatalf("set event state: %v", err)
	}

	db.Close()

	db, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db second time: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	e, err := db.Channel("channel", expected.Topic).Next(ctx)
	if err == nil {
		t.Fatalf("recieved dequeued event: %v", e)
	}
}
