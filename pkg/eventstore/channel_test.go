package eventstore

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq/api/v1/deq"
)

func TestAwaitChannelTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second/4)
	defer cancel()

	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	err = db.Pub(deq.Event{
		Id:         "event1",
		Topic:      "topic",
		CreateTime: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	channel := db.Channel("channel", "topic")

	_, ok := <-channel.AwaitEventStateChange(ctx, "event1")
	if ok {
		t.Fatalf("await dequeue returned without dequeue")
	}
	if channel.Err() != ctx.Err() {
		t.Errorf("await dequeue: %v", err)
	}
}

func TestAwaitChannel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	err = db.Pub(deq.Event{
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

	state, ok := <-channel.AwaitEventStateChange(ctx, "event1")
	if !ok {
		t.Fatalf("await dequeue: %v", channel.Err())
	}
	if state != deq.EventState_DEQUEUED_OK {
		t.Fatalf("returned incorrect state: %s", state.String())
	}
}

func TestGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	expected := deq.Event{
		Id:         "event1",
		Topic:      "topic",
		CreateTime: time.Now().UnixNano(),
		State:      deq.EventState_QUEUED,
	}

	err = db.Pub(expected)
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

	err = db.Pub(*expected)
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
