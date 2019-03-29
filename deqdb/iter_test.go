package deqdb

import (
	"context"
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

	ctx := context.Background()

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
		_, err := db.Pub(ctx, e)
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

	ctx := context.Background()

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
		_, err := db.Pub(ctx, e)
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

	ctx := context.Background()

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
		_, err := db.Pub(ctx, e)
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

	ctx := context.Background()

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
		_, err := db.Pub(ctx, e)
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
	defer channel.Close()

	iter := channel.NewEventIter(DefaultIterOpts)
	defer iter.Close()

	for iter.Next() {
		t.Errorf("iterate empty db")
	}
}

func TestEventIter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

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
		_, err := db.Pub(ctx, e)
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
	defer channel.Close()

	iter := channel.NewEventIter(DefaultIterOpts)
	defer iter.Close()

	for iter.Next() {
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
		_, err := db.Pub(ctx, e)
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
	defer channel.Close()

	opts := DefaultIterOpts
	opts.Reversed = true
	iter := channel.NewEventIter(opts)
	defer iter.Close()

	for iter.Next() {
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

	db, discard := newTestDB()
	defer discard()

	channel := db.Channel("channel1", "topic1")
	defer channel.Close()

	iter := channel.NewIndexIter(DefaultIterOpts)
	defer iter.Close()

	for iter.Next() {
		t.Errorf("iterate empty db")
	}
}

func TestIndexIter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, discard := newTestDB()
	defer discard()

	firstTime := time.Now().Round(0)
	secondTime := firstTime.Add(time.Second).Round(0)

	created := []Event{
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
		_, err := db.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	expected := []Event{
		{
			ID:           "event3",
			Topic:        "topic1",
			Indexes:      []string{"index1", "index4", "index0"},
			CreateTime:   firstTime,
			DefaultState: EventStateQueued,
			State:        EventStateQueued,
		},
		{
			ID:           "event4",
			Topic:        "topic1",
			Indexes:      []string{"index1"},
			CreateTime:   secondTime,
			DefaultState: EventStateQueued,
			State:        EventStateQueued,
		},
		{
			ID:           "event2",
			Topic:        "topic1",
			Indexes:      []string{"index3"},
			CreateTime:   firstTime,
			DefaultState: EventStateQueued,
			State:        EventStateQueued,
		},
		{
			ID:           "event3",
			Topic:        "topic1",
			Indexes:      []string{"index1", "index4", "index0"},
			CreateTime:   firstTime,
			DefaultState: EventStateQueued,
			State:        EventStateQueued,
		},
	}

	var actual []Event

	channel := db.Channel("channel1", "topic1")
	defer channel.Close()

	iter := channel.NewIndexIter(DefaultIterOpts)
	defer iter.Close()

	for iter.Next() {
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

	db, discard := newTestDB()
	defer discard()

	createTime := time.Now()

	created := []Event{
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
		_, err := db.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	expected := []Event{
		{
			ID:           "event2",
			Topic:        "topic1",
			Indexes:      []string{"index3"},
			CreateTime:   createTime,
			DefaultState: EventStateQueued,
			State:        EventStateQueued,
		},
		{
			ID:           "event4",
			Topic:        "topic1",
			Indexes:      []string{"index1"},
			CreateTime:   createTime,
			DefaultState: EventStateQueued,
			State:        EventStateQueued,
		},
	}

	var actual []Event

	channel := db.Channel("channel1", "topic1")
	defer channel.Close()

	opts := DefaultIterOpts
	opts.Reversed = true
	iter := channel.NewIndexIter(opts)
	defer iter.Close()

	for iter.Next() {
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

	db, discard := newTestDB()
	defer discard()

	createTime := time.Now()

	created := []Event{
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
		_, err := db.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	expected := []Event{
		{
			ID:           "event2",
			Topic:        "topic1",
			Indexes:      []string{"index3"},
			CreateTime:   createTime,
			DefaultState: EventStateQueued,
			State:        EventStateQueued,
		},
	}

	var actual []Event

	channel := db.Channel("channel1", "topic1")
	defer channel.Close()

	opts := DefaultIterOpts
	opts.Min = "index10"
	opts.Max = "index50"
	iter := channel.NewIndexIter(opts)
	defer iter.Close()

	for iter.Next() {
		actual = append(actual, iter.Event())
	}
	if iter.Err() != nil {
		t.Fatalf("iterate: %v", iter.Err())
	}

	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}
