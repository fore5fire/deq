package data

import (
	fmt "fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
)

func TestWriteEvent(t *testing.T) {

	db := NewInMemoryDB()
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	// Setup existing channels - currently we have to ack an existing event on the
	// channels we want
	err := WriteEvent(txn, &deq.Event{
		Topic:      "topic",
		ID:         "event0",
		CreateTime: time.Now(),
		Payload:    []byte{1},
	})
	if err != nil {
		t.Fatal("write event: ", err)
	}
	err = WriteEvent(txn, &deq.Event{
		Topic:      "topic",
		ID:         "event00",
		CreateTime: time.Now(),
		Payload:    []byte{1},
	})
	if err != nil {
		t.Fatal("write event: ", err)
	}

	dequeuePayload := ChannelPayload{
		EventState: EventState_OK,
	}

	channelKey := ChannelKey{
		ID:      "event0",
		Topic:   "topic",
		Channel: "channel",
	}
	err = SetChannelEvent(txn, &channelKey, &dequeuePayload)
	if err != nil {
		t.Fatalf("set event state: %v", err)
	}
	channelKey.ID = "event00"
	err = SetChannelEvent(txn, &channelKey, &dequeuePayload)
	if err != nil {
		t.Fatalf("set event state: %v", err)
	}
	channelKey.Channel = "channel2"
	err = SetChannelEvent(txn, &channelKey, &dequeuePayload)
	if err != nil {
		t.Fatalf("set event state: %v", err)
	}

	// Write actual event
	expected := &deq.Event{
		Topic:      "topic",
		ID:         "event1",
		CreateTime: time.Now(),
		Payload:    []byte{1, 2, 3},
		// Should make State start as deq.StateDequeuedOK
		DefaultState: deq.StateOK,
		// Should be ignored.
		State: deq.StateInvalid,
	}

	err = WriteEvent(txn, expected)
	if err != nil {
		t.Fatal("write event: ", err)
	}

	expected.State = deq.StateOK

	actual, err := GetEvent(txn, expected.Topic, expected.ID, "channel")
	if err != nil {
		t.Fatalf("get event on channel: %v", err)
	}
	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
	actual, err = GetEvent(txn, expected.Topic, expected.ID, "channel2")
	if err != nil {
		t.Fatalf("get event on channel2: %v", err)
	}
	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}

	// expected.State = deq.StateQueued

	actual, err = GetEvent(txn, expected.Topic, expected.ID, "newchannel")
	if err != nil {
		t.Fatalf("get event on newchannel: %v", err)
	}
	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}

	err = txn.Commit()
	if err != nil {
		t.Error("commit: ", err)
	}
}

func TestWriteEventWithIndexes(t *testing.T) {

	db := NewInMemoryDB()
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	expected := &deq.Event{
		Topic:      "topic",
		ID:         "event1",
		CreateTime: time.Now(),
		Payload:    []byte{1, 2, 3},
		// Should make State start as deq.StateDequeuedOK
		DefaultState: deq.StateOK,
		// Should be ignored.
		State: deq.StateInvalid,
		Indexes: []string{
			"abc",
			"123",
			"qwerty",
		},
	}

	// Write the event
	err := WriteEvent(txn, expected)
	if err != nil {
		t.Fatal("write event: ", err)
	}

	expected.State = deq.StateOK

	// Verify the event itself
	actual, err := GetEvent(txn, expected.Topic, expected.ID, "channel")
	if err != nil {
		t.Fatalf("get event on channel: %v", err)
	}
	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}

	// Verify the index payloads were created
	expectPayload := &IndexPayload{
		EventId:    expected.ID,
		CreateTime: expected.CreateTime.UnixNano(),
	}

	for i, index := range expected.Indexes {
		actualPayload := new(IndexPayload)
		err = GetIndexPayload(txn, &IndexKey{
			Topic: expected.Topic,
			Value: index,
		}, actualPayload)
		if err != nil {
			t.Fatalf("get index payload %d: %v", i, err)
		}
		if !cmp.Equal(expectPayload, actualPayload) {
			t.Errorf("get index payload %d:\n%v", i, cmp.Diff(expectPayload, actualPayload))
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Error("commit: ", err)
	}
}

func BenchmarkWriteEvent(b *testing.B) {

	dir, err := ioutil.TempDir("", "test-write-event")
	if err != nil {
		b.Fatal("create temp dir:", err)
	}
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir)
	bdb, err := badger.Open(opts)
	if err != nil {
		b.Fatal("open db:", err)
	}

	db := DBFromBadger(bdb)
	defer db.Close()

	for i := 0; i < b.N; i++ {

		txn := db.NewTransaction(true)
		defer txn.Discard()

		expected := deq.Event{
			Topic:        "topic",
			ID:           fmt.Sprintf("event%d", i),
			CreateTime:   time.Now(),
			Payload:      []byte{1, 2, 3},
			DefaultState: deq.StateOK,
		}

		err = WriteEvent(txn, &expected)
		if err != nil {
			b.Fatal("write event:", err)
		}

		err = txn.Commit()
		if err != nil {
			b.Error("commit:", err)
		}
	}
}
