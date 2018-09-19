package eventstore

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deqd/api/v1/deq"
	"gitlab.com/katcheCode/deqd/pkg/eventstore/data"
)

func TestWriteEvent(t *testing.T) {

	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	defer db.Close()
	if err != nil {
		t.Fatal("open db: ", err)
	}

	txn := db.NewTransaction(true)
	defer txn.Discard()

	e := &deq.Event{
		Topic:        "topic",
		Id:           "event1",
		CreateTime:   time.Now().UnixNano(),
		Payload:      []byte{1, 2, 3},
		DefaultState: deq.EventState_DEQUEUED_OK,
		// Should be ignored.
		State: deq.EventState_DEQUEUED_ERROR,
	}

	err = writeEvent(txn, e)
	if err != nil {
		t.Fatal("write event: ", err)
	}

	expected := map[interface{}]proto.Message{
		data.EventKey{
			ID:         e.Id,
			Topic:      e.Topic,
			CreateTime: time.Unix(0, e.CreateTime),
		}: &data.EventPayload{
			Payload:           e.Payload,
			DefaultEventState: e.DefaultState,
		},
		data.EventTimeKey{
			ID:    e.Id,
			Topic: e.Topic,
		}: &data.EventTimePayload{
			CreateTime: e.CreateTime,
		},
	}

	it := txn.NewIterator(badger.DefaultIteratorOptions)

	for it.Rewind(); it.Valid(); it.Next() {

		key, err := data.Unmarshal(it.Item().Key())
		if err != nil {
			t.Error("unmarshal key: ", err)
			continue
		}
		if expected[key] == nil {
			t.Errorf("unexpected key %s", key)
			continue
		}

		val, err := it.Item().Value()
		if err != nil {
			t.Fatal("get item: ", err)
		}
		payload, err := data.UnmarshalPayload(val, key)
		if err != nil {
			t.Fatal("unmarshal payload: ", err)
		}
		if !proto.Equal(expected[key], payload) {
			t.Errorf("expected %v, got %v", expected[key], payload)
		}

		delete(expected, key)
	}
	it.Close()

	if len(expected) > 0 {
		t.Errorf("Missed events: %+v", expected)
	}

	err = txn.Commit(nil)
	if err != nil {
		t.Error("commit: ", err)
	}
}

func TestDel(t *testing.T) {
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
