package deq

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestDel(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	expected := Event{
		ID:         "event1",
		Topic:      "topic",
		CreateTime: time.Now(),
	}

	e, err := db.Pub(expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}
	if !cmp.Equal(e, expected) {
		t.Errorf("pub:\n%s", cmp.Diff(e, expected))
	}

	err = db.Del(expected.Topic, expected.ID)
	if err != nil {
		t.Fatalf("del: %v", err)
	}

	_, err = db.Channel("channel", expected.Topic).Get(expected.ID)
	if err == nil {
		t.Fatalf("returned deleted event")
	}
	if err != ErrNotFound {
		t.Fatalf("get deleted: %v", err)
	}
}

func TestPub(t *testing.T) {
	db, discard := newTestDB()
	defer discard()

	expected := Event{
		ID:           "event1",
		Topic:        "topic",
		CreateTime:   time.Now(),
		DefaultState: EventStateQueued,
		State:        EventStateQueued,
	}

	channel := db.Channel("channel", expected.Topic)

	_, err := db.Pub(expected)
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

	expected.State = EventStateQueued

	event, err = channel.Get(expected.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !cmp.Equal(event, expected) {
		t.Errorf("get:\n%s", cmp.Diff(expected, event))
	}
}
