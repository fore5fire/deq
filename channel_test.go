package deq

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

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
		State:      EventStateQueued,
	}

	_, err := db.Pub(expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

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
