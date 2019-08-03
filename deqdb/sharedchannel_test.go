package deqdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/ack"
)

func TestRequeue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, discard := newTestDB(t)
	defer discard()

	e, err := db.Pub(ctx, deq.Event{
		ID:      "TestRequeueEvent",
		Topic:   "TestRequeue",
		Payload: []byte("Hello world of requeue!"),
	})
	if err != nil {
		t.Fatalf("Error Creating Event: %v", err)
	}

	expect := []deq.Event{
		{
			ID:           "TestRequeueEvent",
			Topic:        "TestRequeue",
			Payload:      []byte("Hello world of requeue!"),
			DefaultState: deq.StateQueued,
			CreateTime:   e.CreateTime,
			State:        deq.StateQueued,
			Selector:     "TestRequeueEvent",
			SendCount:    1,
		},
		{
			ID:           "TestRequeueEvent",
			Topic:        "TestRequeue",
			Payload:      []byte("Hello world of requeue!"),
			DefaultState: deq.StateQueued,
			CreateTime:   e.CreateTime,
			State:        deq.StateQueued,
			Selector:     "TestRequeueEvent",
			SendCount:    2,
		},
		{
			ID:           "TestRequeueEvent",
			Topic:        "TestRequeue",
			Payload:      []byte("Hello world of requeue!"),
			DefaultState: deq.StateQueued,
			CreateTime:   e.CreateTime,
			State:        deq.StateQueuedLinear,
			Selector:     "TestRequeueEvent",
			SendCount:    3,
		},
		{
			ID:           "TestRequeueEvent",
			Topic:        "TestRequeue",
			Payload:      []byte("Hello world of requeue!"),
			DefaultState: deq.StateQueued,
			CreateTime:   e.CreateTime,
			State:        deq.StateQueuedLinear,
			Selector:     "TestRequeueEvent",
			SendCount:    4,
		},
		{
			ID:           "TestRequeueEvent",
			Topic:        "TestRequeue",
			Payload:      []byte("Hello world of requeue!"),
			DefaultState: deq.StateQueued,
			CreateTime:   e.CreateTime,
			State:        deq.StateQueuedConstant,
			Selector:     "TestRequeueEvent",
			SendCount:    5,
		},
		{
			ID:           "TestRequeueEvent",
			Topic:        "TestRequeue",
			Payload:      []byte("Hello world of requeue!"),
			DefaultState: deq.StateQueued,
			CreateTime:   e.CreateTime,
			State:        deq.StateQueuedConstant,
			Selector:     "TestRequeueEvent",
			SendCount:    6,
		},
		{
			ID:           "TestRequeueEvent",
			Topic:        "TestRequeue",
			Payload:      []byte("Hello world of requeue!"),
			DefaultState: deq.StateQueued,
			CreateTime:   e.CreateTime,
			State:        deq.StateQueuedConstant,
			Selector:     "TestRequeueEvent",
			SendCount:    7,
		},
		{
			ID:           "TestRequeueEvent",
			Topic:        "TestRequeue",
			Payload:      []byte("Hello world of requeue!"),
			DefaultState: deq.StateQueued,
			CreateTime:   e.CreateTime,
			State:        deq.StateQueuedConstant,
			Selector:     "TestRequeueEvent",
			SendCount:    8,
		},
		{
			ID:           "TestRequeueEvent",
			Topic:        "TestRequeue",
			Payload:      []byte("Hello world of requeue!"),
			DefaultState: deq.StateQueued,
			CreateTime:   e.CreateTime,
			State:        deq.StateQueuedConstant,
			Selector:     "TestRequeueEvent",
			SendCount:    9,
		},
		{
			ID:           "TestRequeueEvent",
			Topic:        "TestRequeue",
			Payload:      []byte("Hello world of requeue!"),
			DefaultState: deq.StateQueued,
			CreateTime:   e.CreateTime,
			State:        deq.StateQueuedConstant,
			Selector:     "TestRequeueEvent",
			SendCount:    10,
		},
	}

	channel := db.Channel("TestChannel1", "TestRequeue")
	defer channel.Close()

	channel.SetIdleTimeout(time.Second)
	channel.SetInitialResendDelay(time.Second / 10)

	var actual []deq.Event
	err = channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {

		actual = append(actual, e)

		if e.SendCount < 2 {
			return nil, errors.New("This error triggers retries with exponential backoff")
		}
		if e.SendCount < 4 {
			return nil, ack.Error(ack.RequeueLinear, "This error triggers retries with linear backoff")
		}
		if e.SendCount < 10 {
			return nil, ack.Error(ack.RequeueConstant, "This error triggers retries with no backoff")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}
	if !cmp.Equal(expect, actual) {
		t.Errorf("Sub:\n%s", cmp.Diff(expect, actual))
	}
}
