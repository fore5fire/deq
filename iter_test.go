package deq

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestEmptyTopicIter(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	iter := db.NewTopicIter(DefaultIterOpts)
	defer iter.Close()

	for iter.Next() {
		t.Errorf("iterate empty db: %v", iter.Topic())
	}
}

func TestTopicIter(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	events := []Event{
		{
			ID:         "event1",
			Topic:      "TopicA",
			CreateTime: time.Now(),
		},
		{
			ID:         "event2",
			Topic:      "TopicA",
			CreateTime: time.Now(),
		},
		{
			ID:         "event1",
			Topic:      "TopicB",
			CreateTime: time.Now(),
		},
		{
			ID:         "event2",
			Topic:      "TopicB",
			CreateTime: time.Now(),
		},
		{
			ID:         "event2",
			Topic:      "TopicC",
			CreateTime: time.Now(),
		},
	}

	for _, e := range events {
		_, err := db.Pub(e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	expected := []string{"TopicA", "TopicB", "TopicC"}
	var actual []string

	iter := db.NewTopicIter(DefaultIterOpts)
	defer iter.Close()

	for iter.Next() {
		actual = append(actual, iter.Topic())
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}

func TestTopicIterReversed(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	events := []Event{
		{
			ID:         "event1",
			Topic:      "TopicA",
			CreateTime: time.Now(),
		},
		{
			ID:         "event2",
			Topic:      "TopicA",
			CreateTime: time.Now(),
		},
		{
			ID:         "event1",
			Topic:      "TopicB",
			CreateTime: time.Now(),
		},
		{
			ID:         "event2",
			Topic:      "TopicB",
			CreateTime: time.Now(),
		},
		{
			ID:         "event2",
			Topic:      "TopicC",
			CreateTime: time.Now(),
		},
	}

	for _, e := range events {
		_, err := db.Pub(e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	expected := []string{"TopicC", "TopicB", "TopicA"}
	var actual []string

	opts := DefaultIterOpts
	opts.Reversed = true
	iter := db.NewTopicIter(opts)
	defer iter.Close()

	for iter.Next() {
		actual = append(actual, iter.Topic())
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}

func TestTopicIterMin(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	events := []Event{
		{
			ID:         "event1",
			Topic:      "TopicA",
			CreateTime: time.Now(),
		},
		{
			ID:         "event2",
			Topic:      "TopicA",
			CreateTime: time.Now(),
		},
		{
			ID:         "event1",
			Topic:      "TopicB",
			CreateTime: time.Now(),
		},
		{
			ID:         "event2",
			Topic:      "TopicB",
			CreateTime: time.Now(),
		},
		{
			ID:         "event2",
			Topic:      "TopicC",
			CreateTime: time.Now(),
		},
	}

	for _, e := range events {
		_, err := db.Pub(e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	expected := []string{"TopicB", "TopicC"}
	var actual []string

	opts := DefaultIterOpts
	opts.Min = "TopicAA"
	iter := db.NewTopicIter(opts)
	defer iter.Close()

	for iter.Next() {
		actual = append(actual, iter.Topic())
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}

	expected = []string{"TopicA", "TopicB", "TopicC"}
	actual = nil

	// Test inclusive boundry
	opts.Min = "TopicA"
	iter2 := db.NewTopicIter(opts)
	defer iter2.Close()

	for iter2.Next() {
		actual = append(actual, iter2.Topic())
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}

func TestTopicIterMax(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	events := []Event{
		{
			ID:         "event1",
			Topic:      "TopicA",
			CreateTime: time.Now(),
		},
		{
			ID:         "event2",
			Topic:      "TopicA",
			CreateTime: time.Now(),
		},
		{
			ID:         "event1",
			Topic:      "TopicB",
			CreateTime: time.Now(),
		},
		{
			ID:         "event2",
			Topic:      "TopicB",
			CreateTime: time.Now(),
		},
		{
			ID:         "event2",
			Topic:      "TopicC",
			CreateTime: time.Now(),
		},
	}

	for _, e := range events {
		_, err := db.Pub(e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	expected := []string{"TopicA", "TopicB"}
	var actual []string

	opts := DefaultIterOpts
	opts.Max = "TopicBB"
	iter := db.NewTopicIter(opts)
	defer iter.Close()

	for iter.Next() {
		actual = append(actual, iter.Topic())
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}

	expected = []string{"TopicA", "TopicB", "TopicC"}
	actual = nil

	// Test inclusive boundry
	opts.Max = "TopicC"
	iter2 := db.NewTopicIter(opts)
	defer iter2.Close()

	for iter2.Next() {
		actual = append(actual, iter2.Topic())
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("test inclusive boundry:\n%s", cmp.Diff(expected, actual))
	}
}

func TestEmptyEventIter(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	channel := db.Channel("channel1", "topic1")
	iter := channel.NewEventIter(DefaultIterOpts)
	defer iter.Close()

	for iter.Next() {
		t.Errorf("iterate empty db")
	}
}

func TestEventIter(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	createTime := time.Now()

	created := []Event{
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
		_, err := db.Pub(e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	expected := []Event{
		{
			ID:           "event1",
			Topic:        "topic1",
			CreateTime:   createTime,
			DefaultState: EventStateQueued,
			State:        EventStateQueued,
		},
		{
			ID:           "event2",
			Topic:        "topic1",
			CreateTime:   createTime,
			DefaultState: EventStateQueued,
			State:        EventStateQueued,
		},
	}

	var actual []Event

	channel := db.Channel("channel1", "topic1")
	iter := channel.NewEventIter(DefaultIterOpts)
	defer iter.Close()

	for iter.Next() {
		e, err := iter.Event()
		if err != nil {
			t.Fatalf("get event from iter: %v", err)
		}

		actual = append(actual, e)
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}

func TestEventIterReversed(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB()
	defer discard()

	createTime := time.Now()

	created := []Event{
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
		_, err := db.Pub(e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	expected := []Event{
		{
			ID:           "event2",
			Topic:        "topic1",
			CreateTime:   createTime,
			DefaultState: EventStateQueued,
			State:        EventStateQueued,
		},
		{
			ID:           "event1",
			Topic:        "topic1",
			CreateTime:   createTime,
			DefaultState: EventStateQueued,
			State:        EventStateQueued,
		},
	}

	var actual []Event

	channel := db.Channel("channel1", "topic1")
	opts := DefaultIterOpts
	opts.Reversed = true
	iter := channel.NewEventIter(opts)
	defer iter.Close()

	for iter.Next() {
		e, err := iter.Event()
		if err != nil {
			t.Fatalf("get event from iter: %v", err)
		}

		actual = append(actual, e)
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}
