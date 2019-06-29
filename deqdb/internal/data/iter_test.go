package data

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
)

func TestEmptyEventIter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	db := NewInMemoryDB()
	defer db.Close()

	txn := db.NewTransaction(false)
	defer txn.Discard()

	iter, err := NewEventIter(txn, "topic1", "channel1", nil)
	if err != nil {
		t.Fatalf("create iterator: %v", err)
	}
	defer iter.Close()

	for iter.Next(ctx) {
		t.Errorf("iterate empty db")
	}
}

func TestEventIter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	db := NewInMemoryDB()
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	createTime := time.Now()

	created := []*deq.Event{
		{
			ID:         "event2",
			Topic:      "topic1",
			CreateTime: createTime,
		},
		{
			ID:         "event1",
			Topic:      "topic2",
			CreateTime: createTime,
		},
		{
			ID:         "event1",
			Topic:      "topic1",
			CreateTime: createTime,
		},
	}

	for _, e := range created {
		err := WriteEvent(txn, e)
		if err != nil {
			t.Fatalf("write event: %v", err)
		}
	}

	expected := []deq.Event{
		{
			ID:           "event1",
			Topic:        "topic1",
			CreateTime:   createTime,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
		{
			ID:           "event2",
			Topic:        "topic1",
			CreateTime:   createTime,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
	}

	var actual []deq.Event

	iter, err := NewEventIter(txn, "topic1", "channel1", nil)
	if err != nil {
		t.Fatalf("create iter: %v", err)
	}
	defer iter.Close()

	for iter.Next(ctx) {
		actual = append(actual, iter.Event())
	}
	if iter.Err() != nil {
		t.Fatalf("iterate: %v", iter.Err())
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}

func TestEventIterReversed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db := NewInMemoryDB()
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	createTime := time.Now()

	created := []*deq.Event{
		{
			ID:         "event2",
			Topic:      "topic1",
			CreateTime: createTime,
		},
		{
			ID:         "event1",
			Topic:      "topic2",
			CreateTime: createTime,
		},
		{
			ID:         "event1",
			Topic:      "topic1",
			CreateTime: createTime,
		},
	}

	for _, e := range created {
		err := WriteEvent(txn, e)
		if err != nil {
			t.Fatalf("write event: %v", err)
		}
	}

	expected := []deq.Event{
		{
			ID:           "event2",
			Topic:        "topic1",
			CreateTime:   createTime,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
		{
			ID:           "event1",
			Topic:        "topic1",
			CreateTime:   createTime,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
	}

	var actual []deq.Event

	iter, err := NewEventIter(txn, "topic1", "channel1", &deq.IterOptions{
		Reversed: true,
	})
	if err != nil {
		t.Fatalf("create iter: %v", err)
	}

	for iter.Next(ctx) {
		actual = append(actual, iter.Event())
	}
	if iter.Err() != nil {
		t.Fatalf("iterate: %v", iter.Err())
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}

func TestEmptyIndexIter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	db := NewInMemoryDB()
	defer db.Close()

	txn := db.NewTransaction(false)
	defer txn.Discard()

	iter, err := NewIndexIter(txn, "topic1", "channel1", nil)
	if err != nil {
		t.Fatalf("create iter: %v", err)
	}
	defer iter.Close()

	for iter.Next(ctx) {
		t.Errorf("iterate empty db")
	}
}

func TestIndexIter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db := NewInMemoryDB()
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	firstTime := time.Now().Round(0)
	secondTime := firstTime.Add(time.Second).Round(0)

	created := []*deq.Event{
		{
			ID:         "event2",
			Topic:      "topic1",
			CreateTime: firstTime,
			Indexes:    []string{"index3"},
		},
		{
			ID:         "event1",
			Topic:      "topic2",
			CreateTime: firstTime,
			Indexes:    []string{"index2"},
		},
		{
			ID:         "event3",
			Topic:      "topic1",
			CreateTime: firstTime,
			Indexes:    []string{"index1", "index4", "index0"},
		},
		{
			ID:         "event4",
			Topic:      "topic1",
			CreateTime: secondTime,
			Indexes:    []string{"index1"},
		},
		{
			ID:         "event1",
			Topic:      "topic1",
			CreateTime: secondTime,
		},
	}

	for _, e := range created {
		err := WriteEvent(txn, e)
		if err != nil {
			t.Fatalf("write event: %v", err)
		}
	}

	expected := []deq.Event{
		{
			ID:           "event3",
			Topic:        "topic1",
			Indexes:      []string{"index1", "index4", "index0"},
			CreateTime:   firstTime,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
		{
			ID:           "event4",
			Topic:        "topic1",
			Indexes:      []string{"index1"},
			CreateTime:   secondTime,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
		{
			ID:           "event2",
			Topic:        "topic1",
			Indexes:      []string{"index3"},
			CreateTime:   firstTime,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
		{
			ID:           "event3",
			Topic:        "topic1",
			Indexes:      []string{"index1", "index4", "index0"},
			CreateTime:   firstTime,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
	}

	var actual []deq.Event

	iter, err := NewIndexIter(txn, "topic1", "channel1", nil)
	if err != nil {
		t.Fatalf("create iter: %v", err)
	}
	defer iter.Close()

	for iter.Next(ctx) {
		actual = append(actual, iter.Event())
	}
	if iter.Err() != nil {
		t.Fatalf("iterate: %v", iter.Err())
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}

func TestIndexIterReversed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db := NewInMemoryDB()
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	createTime := time.Now()

	created := []*deq.Event{
		{
			ID:         "event2",
			Topic:      "topic1",
			CreateTime: createTime,
			Indexes:    []string{"index3"},
		},
		{
			ID:         "event1",
			Topic:      "topic2",
			CreateTime: createTime,
			Indexes:    []string{"index2"},
		},
		{
			ID:         "event3",
			Topic:      "topic1",
			CreateTime: createTime,
			Indexes:    []string{"index1"},
		},
		{
			ID:         "event4",
			Topic:      "topic1",
			CreateTime: createTime,
			Indexes:    []string{"index1"},
		},
		{
			ID:         "event1",
			Topic:      "topic1",
			CreateTime: createTime,
		},
	}

	for _, e := range created {
		err := WriteEvent(txn, e)
		if err != nil {
			t.Fatalf("write event: %v", err)
		}
	}

	expected := []deq.Event{
		{
			ID:           "event2",
			Topic:        "topic1",
			Indexes:      []string{"index3"},
			CreateTime:   createTime,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
		{
			ID:           "event4",
			Topic:        "topic1",
			Indexes:      []string{"index1"},
			CreateTime:   createTime,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
	}

	var actual []deq.Event

	iter, err := NewIndexIter(txn, "topic1", "channel1", &deq.IterOptions{
		Reversed: true,
	})
	if err != nil {
		t.Fatalf("create iter: %v", err)
	}
	defer iter.Close()

	for iter.Next(ctx) {
		actual = append(actual, iter.Event())
	}
	if iter.Err() != nil {
		t.Fatalf("iterate: %v", iter.Err())
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}

func TestIndexIterLimits(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db := NewInMemoryDB()
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	createTime := time.Now()

	created := []*deq.Event{
		{
			ID:         "event2",
			Topic:      "topic1",
			CreateTime: createTime,
			Indexes:    []string{"index3"},
		},
		{
			ID:         "event1",
			Topic:      "topic2",
			CreateTime: createTime,
			Indexes:    []string{"index2"},
		},
		{
			ID:         "event3",
			Topic:      "topic1",
			CreateTime: createTime,
			Indexes:    []string{"index1"},
		},
		{
			ID:         "event4",
			Topic:      "topic1",
			CreateTime: createTime,
			Indexes:    []string{"index1"},
		},
		{
			ID:         "event1",
			Topic:      "topic1",
			CreateTime: createTime,
			Indexes:    []string{"index9"},
		},
	}

	for _, e := range created {
		err := WriteEvent(txn, e)
		if err != nil {
			t.Fatalf("write event: %v", err)
		}
	}

	expected := []deq.Event{
		{
			ID:           "event2",
			Topic:        "topic1",
			Indexes:      []string{"index3"},
			CreateTime:   createTime,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
	}

	var actual []deq.Event

	iter, err := NewIndexIter(txn, "topic1", "channel1", &deq.IterOptions{
		Min: "index10",
		Max: "index50",
	})
	if err != nil {
		t.Fatalf("create iter: %v", err)
	}
	defer iter.Close()

	for iter.Next(ctx) {
		actual = append(actual, iter.Event())
	}
	if iter.Err() != nil {
		t.Fatalf("iterate: %v", iter.Err())
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}
