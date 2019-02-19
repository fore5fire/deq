package deq

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq/api/v1/deq"
)

func TestTopics(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	ctx := context.Background()

	events := []deq.Event{
		{
			Id:         "event1",
			Topic:      "topic1",
			CreateTime: time.Now().UnixNano(),
		},
		{
			Id:         "event2",
			Topic:      "topic1",
			CreateTime: time.Now().UnixNano(),
		},
		{
			Id:         "event1",
			Topic:      "topic2",
			CreateTime: time.Now().UnixNano(),
		},
		{
			Id:         "event2",
			Topic:      "topic2",
			CreateTime: time.Now().UnixNano(),
		},
		{
			Id:         "event2",
			Topic:      "topic3",
			CreateTime: time.Now().UnixNano(),
		},
	}

	for _, e := range events {
		_, err := db.Pub(e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	expected := []string{"topic1", "topic2", "topic3"}
	topics, err := db.Topics(ctx)
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	if !cmp.Equal(topics, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, topics))
	}
}

func TestDel(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	expected := deq.Event{
		Id:         "event1",
		Topic:      "topic",
		CreateTime: time.Now().UnixNano(),
	}

	e, err := db.Pub(expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}
	if !cmp.Equal(e, expected) {
		t.Errorf("pub:\n%s", cmp.Diff(e, expected))
	}

	err = db.Del(expected.Topic, expected.Id)
	if err != nil {
		t.Fatalf("del: %v", err)
	}

	_, err = db.Channel("channel", expected.Topic).Get(expected.Id)
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

	expected := deq.Event{
		Id:           "event1",
		Topic:        "topic",
		CreateTime:   time.Now().UnixNano(),
		DefaultState: deq.EventState_QUEUED,
		State:        deq.EventState_QUEUED,
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

	expected.State = deq.EventState_QUEUED

	event, err = channel.Get(expected.Id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !cmp.Equal(event, expected) {
		t.Errorf("get:\n%s", cmp.Diff(expected, event))
	}
}
