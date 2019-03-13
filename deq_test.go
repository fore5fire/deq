package deq

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func newTestDB() (*Store, func()) {
	dir, err := ioutil.TempDir("", "test-pub")
	if err != nil {
		panic("create temp dir: " + err.Error())
	}

	db, err := Open(Options{
		Dir: dir,
	})
	if err != nil {
		panic("open db: " + err.Error())
	}

	return db, func() {
		db.Close()
		os.RemoveAll(dir)
	}
}

func TestDel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, discard := newTestDB()
	defer discard()

	expected := Event{
		ID:           "event1",
		Topic:        "topic",
		CreateTime:   time.Now(),
		DefaultState: EventStateQueued,
		State:        EventStateQueued,
	}

	_, err := db.Pub(ctx, expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	err = db.Del(expected.Topic, expected.ID)
	if err != nil {
		t.Fatalf("del: %v", err)
	}

	channel := db.Channel("channel", expected.Topic)
	defer channel.Close()

	_, err = channel.Get(expected.ID)
	if err == nil {
		t.Fatalf("returned deleted event")
	}
	if err != ErrNotFound {
		t.Fatalf("get deleted: %v", err)
	}
}

func TestPub(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

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

	expected.State = EventStateQueued

	event, err = channel.Get(expected.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !cmp.Equal(event, expected) {
		t.Errorf("get:\n%s", cmp.Diff(expected, event))
	}
}

func TestPubDuplicate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

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
	defer channel.Close()

	// Publish the event
	_, err := db.Pub(ctx, expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	// Publish and verify event with same id and payload
	_, err = db.Pub(ctx, expected)
	if err != nil {
		t.Fatalf("identifical duplicate pub: %v", err)
	}

	// Publish and verify event with same id and different payload
	expected.Payload = []byte{1}
	_, err = db.Pub(ctx, expected)
	if err != ErrAlreadyExists {
		t.Fatalf("modified duplicate pub: %v", err)
	}
	expected.Payload = nil

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
