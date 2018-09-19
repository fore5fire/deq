package eventstore

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deqd/api/v1/deq"
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

	_, err = db.Channel("channel", "topic").AwaitDequeue(ctx, "event1")
	if err == nil {
		t.Fatalf("await dequeue returned without dequeue")
	}
	if err != ctx.Err() {
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
	state, err := db.Channel("channel", "topic").AwaitDequeue(ctx, "event1")
	if err != nil {
		t.Fatalf("await dequeue: %v", err)
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

	expected := &deq.Event{
		Id:         "event1",
		Topic:      "topic",
		CreateTime: time.Now().UnixNano(),
	}

	err = db.Pub(*expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	e, err := db.Channel("channel", expected.Topic).Get(expected.Id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !proto.Equal(e, expected) {
		t.Errorf("expected: %v, got %v", expected, e)
	}
}
