package deqdb

import (
	"context"
	"io/ioutil"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/deqopt"
)

func TestSub(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, discard := newTestDB(t)
	defer discard()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Round(0) gets rid of leap-second info, which will be lost in serialization
	createTime := time.Now().Round(0)

	// Publish some events
	events := struct {
		Before, After, ExpectedBefore, ExpectedAfter, ExpectedResponses []deq.Event
	}{
		Before: []deq.Event{
			{
				ID:         "before-event1",
				Topic:      "TopicA",
				CreateTime: createTime,
			},
			{
				ID:         "before-event2",
				Topic:      "TopicA",
				CreateTime: createTime,
			},
			{
				ID:         "before-event1",
				Topic:      "TopicB",
				CreateTime: createTime,
			},
		},
		ExpectedBefore: []deq.Event{
			{
				ID:           "before-event1",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
				SendCount:    1,
			},
			{
				ID:           "before-event2",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
				SendCount:    1,
			},
		},
		After: []deq.Event{
			{
				ID:         "~after-event1",
				Topic:      "TopicA",
				CreateTime: createTime,
			},
			{
				ID:         "~after-event2",
				Topic:      "TopicA",
				CreateTime: createTime,
			},
			{
				ID:         "~after-event1",
				Topic:      "TopicB",
				CreateTime: createTime,
			},
		},
		ExpectedAfter: []deq.Event{
			{
				ID:           "~after-event1",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
				SendCount:    1,
			},
			{
				ID:           "~after-event2",
				Topic:        "TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
				SendCount:    1,
			},
		},
		ExpectedResponses: []deq.Event{
			{
				ID:           "before-event1",
				Topic:        "Response-TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
				SendCount:    1,
			},
			{
				ID:           "before-event2",
				Topic:        "Response-TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
				SendCount:    1,
			},
			{
				ID:           "~after-event1",
				Topic:        "Response-TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
				SendCount:    1,
			},
			{
				ID:           "~after-event2",
				Topic:        "Response-TopicA",
				CreateTime:   createTime,
				DefaultState: deq.StateQueued,
				State:        deq.StateQueued,
				SendCount:    1,
			},
		},
	}

	for _, e := range events.Before {
		_, err := db.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	done := make(chan bool)
	received := make(chan deq.Event, 10)
	responses := make(chan deq.Event, 10)

	// Subscribe to events.
	channel := db.Channel("test-channel", "TopicA")
	defer channel.Close()
	go func() {
		defer func() { done <- true }()

		err := channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {
			t.Logf("got event %s %s", e.Topic, e.ID)
			select {
			case received <- e:
				defer t.Logf("sent event %s %s", "Response-TopicA", e.ID)
				return &deq.Event{
					ID:         e.ID,
					Topic:      "Response-TopicA",
					CreateTime: createTime,
				}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
		if err != nil && err != ErrChannelClosed && err != ctx.Err() {
			t.Errorf("sub TopicA: %v", err)
		}
	}()

	// Subscribe to response events
	responseChannel := db.Channel("test-channel", "Response-TopicA")
	defer responseChannel.Close()
	go func() {
		defer func() { done <- true }()

		err := responseChannel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {
			t.Logf("got event %s %s", e.Topic, e.ID)
			select {
			case responses <- e:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
		if err != nil && err != ErrChannelClosed && err != ctx.Err() {
			t.Errorf("sub Response-TopicA: %v", err)
		}
	}()

	// Verify that events were received by handler
	var actual []deq.Event
	for len(actual) < len(events.ExpectedBefore) {
		select {
		case recv := <-received:
			actual = append(actual, recv)
			t.Log("ExpectedBefore", recv.Topic, recv.ID)
		case <-time.After(time.Second):
			t.Fatal("pre-sub received events: timed out")
		}
	}
	if !cmp.Equal(events.ExpectedBefore, actual) {
		t.Errorf("pre-sub received events:\n%s", cmp.Diff(events.ExpectedBefore, actual))
	}

	// Publish some more events now that we're already subscribed
	for _, e := range events.After {
		_, err := db.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	// Verify that events were received by handler
	actual = nil
	for len(actual) < len(events.ExpectedAfter) {
		select {
		case recv := <-received:
			actual = append(actual, recv)
			t.Log("ExpectedAfter", recv.Topic, recv.ID)
		case <-time.After(time.Second):
			t.Fatal("post-sub received events: timed out")
		}
	}
	if !cmp.Equal(events.ExpectedAfter, actual) {
		t.Errorf("post-sub received events:\n%s", cmp.Diff(events.ExpectedAfter, actual))
	}

	// Verify that response events were published
	actual = nil
	for len(actual) < len(events.ExpectedResponses) {
		select {
		case resp := <-responses:
			actual = append(actual, resp)
			t.Log("ExpectedResponses", resp.Topic, resp.ID)
		case <-time.After(time.Second):
			t.Fatal("response events: timed out")
		}
	}
	sort.SliceStable(actual, func(i, j int) bool {
		return actual[i].ID < actual[j].ID
	})
	if !cmp.Equal(events.ExpectedResponses, actual) {
		t.Errorf("response events:\n%s", cmp.Diff(events.ExpectedResponses, actual))
	}

	// Verify that subscriptions close correctly.
	cancel()
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("close subs: timed out")
		}
	}
}

func TestAwait(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB(t)
	defer discard()

	expected := deq.Event{
		ID:           "event1",
		Topic:        "test-topic",
		CreateTime:   time.Now(),
		DefaultState: deq.StateQueued,
		State:        deq.StateQueued,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	channel := db.Channel("test-channel", expected.Topic)
	defer channel.Close()

	type AwaitResponse struct {
		Event deq.Event
		Err   error
	}

	// Start awaiting in background before we
	done := make(chan bool)
	go func() {
		defer close(done)
		e, err := channel.Get(ctx, expected.ID, deqopt.Await())
		if err != nil {
			t.Errorf("await before pub: %v", err)
			return
		}
		if !cmp.Equal(expected, e) {
			t.Errorf("await before pub:\n%s", cmp.Diff(expected, e))
		}
	}()

	time.Sleep(time.Millisecond * 50)
	_, err := db.Pub(ctx, expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	// Check background await.
	select {
	case <-done:
	case <-time.After(time.Second * 3):
		t.Fatal("await before pub: timed out")
	}

	e, err := channel.Get(ctx, expected.ID, deqopt.Await())
	if err != nil {
		t.Fatalf("await after pub: %v", err)
	}
	if !cmp.Equal(expected, e) {
		t.Errorf("await after pub:\n%s", cmp.Diff(expected, e))
	}
}

func TestAwaitChannelTimeout(t *testing.T) {
	t.Parallel()

	db, discard := newTestDB(t)
	defer discard()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second/4)
	defer cancel()

	_, err := db.Pub(ctx, deq.Event{
		ID:         "event1",
		Topic:      "topic",
		CreateTime: time.Now(),
	})
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	channel := db.Channel("channel", "topic")
	defer channel.Close()

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

	db, discard := newTestDB(t)
	defer discard()

	ctx := context.Background()

	_, err := db.Pub(ctx, deq.Event{
		ID:         "event1",
		Topic:      "topic",
		CreateTime: time.Now(),
	})
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	channel := db.Channel("channel", "topic")
	defer channel.Close()

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

	ctx := context.Background()

	db, discard := newTestDB(t)
	defer discard()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	_, err := db.Pub(ctx, deq.Event{
		ID:         "event1",
		Topic:      "topic",
		CreateTime: time.Now(),
	})
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	go func() {
		time.Sleep(time.Second / 4)
		channel := db.Channel("channel", "topic")
		defer channel.Close()

		err := channel.SetEventState(ctx, "event1", deq.StateOK)
		if err != nil {
			t.Errorf("set event state: %v", err)
		}
	}()

	channel := db.Channel("channel", "topic")
	defer channel.Close()

	sub := channel.NewEventStateSubscription("event1")
	defer sub.Close()

	state, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("await dequeue: %v", err)
	}
	if state != deq.StateOK {
		t.Fatalf("returned incorrect state: %v", state)
	}
}

func TestGet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, discard := newTestDB(t)
	defer discard()

	expected := deq.Event{
		ID:         "event1",
		Topic:      "topic",
		CreateTime: time.Now(),
	}

	_, err := db.Pub(ctx, expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	expected.DefaultState = deq.StateQueued
	expected.State = deq.StateQueued

	channel := db.Channel("channel", expected.Topic)
	defer channel.Close()

	actual, err := channel.Get(ctx, expected.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}

	err = channel.SetEventState(ctx, expected.ID, deq.StateOK)
	if err != nil {
		t.Fatalf("set event state: %v", err)
	}

	expected.State = deq.StateOK

	actual, err = channel.Get(ctx, expected.ID)
	if err != nil {
		t.Fatalf("get after set state: %v", err)
	}
	if !cmp.Equal(actual, expected) {
		t.Errorf("get after set state:\n%s", cmp.Diff(expected, actual))
	}
}

func TestGetIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, discard := newTestDB(t)
	defer discard()

	expected := deq.Event{
		ID:           "event1",
		Topic:        "topic",
		CreateTime:   time.Now(),
		DefaultState: deq.StateQueued,
		State:        deq.StateQueued,
		Indexes:      []string{"index1", "index3"},
	}

	_, err := db.Pub(ctx, expected)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	channel := db.Channel("channel", expected.Topic)
	defer channel.Close()

	actual, err := channel.Get(ctx, "index1", deqopt.UseIndex())
	if err != nil {
		t.Fatalf("get first index: %v", err)
	}
	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}

	actual, err = channel.Get(ctx, "index2", deqopt.UseIndex())
	if err != nil && err != deq.ErrNotFound {
		t.Fatalf("get missing index: %v", err)
	}
	if err == nil {
		t.Errorf("get missing index: found event %v", actual)
	}

	actual, err = channel.Get(ctx, "index3", deqopt.UseIndex())
	if err != nil {
		t.Fatalf("get second index: %v", err)
	}
	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}

func TestBatchGetAllow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, discard := newTestDB(t)
	defer discard()

	pub := []deq.Event{
		{
			ID:           "event1",
			Topic:        "topic",
			CreateTime:   time.Now().Round(0),
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
		{
			ID:           "event2",
			Topic:        "topic",
			CreateTime:   time.Now().Round(0),
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
		{
			ID:           "event3",
			Topic:        "topic",
			CreateTime:   time.Now().Round(0),
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
		{
			ID:           "event4",
			Topic:        "topic",
			CreateTime:   time.Now().Round(0),
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
	}

	for _, e := range pub {
		_, err := db.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	expected := map[string]deq.Event{
		"event1": pub[0],
		"event3": pub[2],
		"event4": pub[3],
	}

	ids := []string{
		"event1", "event3", "event4",
	}

	channel := db.Channel("channel", pub[0].Topic)
	defer channel.Close()

	actual, err := channel.BatchGet(ctx, ids)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}

func TestBatchGetAllowNotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, discard := newTestDB(t)
	defer discard()

	pub := []deq.Event{
		{
			ID:           "event1",
			Topic:        "topic",
			CreateTime:   time.Now().Round(0),
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
		{
			ID:           "event2",
			Topic:        "topic",
			CreateTime:   time.Now().Round(0),
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
		{
			ID:           "event3",
			Topic:        "topic",
			CreateTime:   time.Now().Round(0),
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
		{
			ID:           "event4",
			Topic:        "topic",
			CreateTime:   time.Now().Round(0),
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
		},
	}

	for _, e := range pub {
		_, err := db.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	expected := map[string]deq.Event{
		"event1": pub[0],
		"event3": pub[2],
		"event4": pub[3],
	}

	ids := []string{
		"event1", "event3", "event4", "event5", "event7",
	}

	channel := db.Channel("channel", pub[0].Topic)
	defer channel.Close()

	actual, err := channel.BatchGet(ctx, ids, deqopt.AllowNotFound())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}

func TestBatchGetIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, discard := newTestDB(t)
	defer discard()

	now := time.Now().Round(0)
	after := now.Add(time.Second)

	pub := []deq.Event{
		{
			ID:           "event1",
			Topic:        "topic",
			CreateTime:   now,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
			Indexes:      []string{"1"},
		},
		{
			ID:           "event2",
			Topic:        "topic",
			CreateTime:   after,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
			Indexes:      []string{"1"},
		},
		{
			ID:           "event3",
			Topic:        "topic",
			CreateTime:   now,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
			Indexes:      []string{"2"},
		},
		{
			ID:           "event4",
			Topic:        "topic",
			CreateTime:   now,
			DefaultState: deq.StateQueued,
			State:        deq.StateQueued,
			Indexes:      []string{"3"},
		},
	}

	for _, e := range pub {
		_, err := db.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
	}

	expected := map[string]deq.Event{
		"1": pub[1],
		"2": pub[2],
		"3": pub[3],
	}

	indexes := []string{
		"1", "2", "3",
	}

	channel := db.Channel("channel", pub[0].Topic)
	defer channel.Close()

	actual, err := channel.BatchGet(ctx, indexes, deqopt.UseIndex())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !cmp.Equal(actual, expected) {
		t.Errorf("\n%s", cmp.Diff(expected, actual))
	}
}

func TestDequeue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	expected := deq.Event{
		ID:         "event1",
		Topic:      "topic",
		CreateTime: time.Now(),
		State:      deq.StateQueued,
	}

	func() {
		db, err := Open(Options{
			Dir:             dir,
			UpgradeIfNeeded: true,
		})
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		defer db.Close()

		_, err = db.Pub(ctx, expected)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}

		channel := db.Channel("channel", expected.Topic)
		defer channel.Close()

		err = channel.SetEventState(ctx, expected.ID, deq.StateInternal)
		if err != nil {
			t.Fatalf("set event state: %v", err)
		}
	}()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db second time: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	channel := db.Channel("channel", expected.Topic)
	defer channel.Close()

	e, err := channel.Next(ctx)
	if err == nil {
		t.Fatalf("received dequeued event: %v", e)
	}
}
