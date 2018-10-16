package eventstore

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"gitlab.com/katcheCode/deq/api/v1/deq"
)

func TestDel(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-del")
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
	dir, err := ioutil.TempDir("", "test-pub")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	expected := &deq.Event{
		Id:           "event1",
		Topic:        "topic",
		CreateTime:   time.Now().UnixNano(),
		DefaultState: deq.EventState_QUEUED,
	}

	channel := db.Channel("channel", expected.Topic)

	err = db.Pub(*expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	event := <-channel.Follow()
	if !reflect.DeepEqual(event, expected) {
		t.Errorf("expected %v, got %v", expected, event)
	}

	expected.State = deq.EventState_QUEUED

	event, err = channel.Get(expected.Id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !reflect.DeepEqual(event, expected) {
		t.Errorf("expected %v, got %v", expected, event)
	}
}
