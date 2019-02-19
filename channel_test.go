package deq

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq/api/v1/deq"
)

func TestList(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	ctx := context.Background()

	createTime := time.Now().UnixNano()

	created := []deq.Event{
		{
			Id:         "event2",
			Topic:      "topic1",
			CreateTime: createTime,
		},
		{
			Id:         "event1",
			Topic:      "topic2",
			CreateTime: createTime,
		},
		{
			Id:         "event1",
			Topic:      "topic1",
			CreateTime: createTime,
		},
	}

	for _, e := range created {
		_, err := db.Pub(e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	expected := []deq.Event{
		{
			Id:         "event1",
			Topic:      "topic1",
			CreateTime: createTime,
			State:      deq.EventState_QUEUED,
		},
		{
			Id:         "event2",
			Topic:      "topic1",
			CreateTime: createTime,
			State:      deq.EventState_QUEUED,
		},
	}

	channel := db.Channel("channel1", "topic1")
	events, err := channel.List(ctx, ListOpts{})
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	if !cmp.Equal(events, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, events))
	}
}

func TestAwaitChannelTimeout(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second/4)
	defer cancel()

	_, err := db.Pub(deq.Event{
		Id:         "event1",
		Topic:      "topic",
		CreateTime: time.Now().UnixNano(),
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

	_, err := db.Pub(deq.Event{
		Id:         "event1",
		Topic:      "topic",
		CreateTime: time.Now().UnixNano(),
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

	_, err := db.Pub(deq.Event{
		Id:         "event1",
		Topic:      "topic",
		CreateTime: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	go func() {
		time.Sleep(time.Second / 4)
		err := db.Channel("channel", "topic").SetEventState("event1", deq.EventState_DEQUEUED_OK)
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
	if state != deq.EventState_DEQUEUED_OK {
		t.Fatalf("returned incorrect state: %s", state.String())
	}
}

func TestGet(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	expected := deq.Event{
		Id:         "event1",
		Topic:      "topic",
		CreateTime: time.Now().UnixNano(),
		State:      deq.EventState_QUEUED,
	}

	_, err := db.Pub(expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	channel := db.Channel("channel", expected.Topic)

	e, err := channel.Get(expected.Id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !proto.Equal(&e, &expected) {
		t.Errorf("expected: %v, got %v", expected, e)
	}

	err = channel.SetEventState(expected.Id, deq.EventState_DEQUEUED_OK)
	if err != nil {
		t.Fatalf("set event state: %v", err)
	}

	expected.State = deq.EventState_DEQUEUED_OK

	e, err = channel.Get(expected.Id)
	if err != nil {
		t.Fatalf("get after set state: %v", err)
	}
	if !proto.Equal(&e, &expected) {
		t.Errorf("get after set state: expected: %v, got %v", expected, e)
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

	expected := &deq.Event{
		Id:         "event1",
		Topic:      "topic",
		CreateTime: time.Now().UnixNano(),
		State:      deq.EventState_QUEUED,
	}

	_, err = db.Pub(*expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	channel := db.Channel("channel", expected.Topic)

	err = channel.SetEventState(expected.Id, deq.EventState_DEQUEUED_ERROR)
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
