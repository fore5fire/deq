package legacyclient

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq"
	api "gitlab.com/katcheCode/deq/api/v1/deq"
	"google.golang.org/grpc"
)

// Publisher publishes events via the Pub method
type Publisher struct {
	client api.DEQClient
	opts   PublisherOpts
}

// PublisherOpts provides options used by a Producer
type PublisherOpts struct {
	AwaitChannel string
}

// NewPublisher constructs a new Publisher.
// conn can be used by multiple Publishers and Subscribers in parallel
func NewPublisher(conn *grpc.ClientConn, opts PublisherOpts) *Publisher {
	return &Publisher{
		client: api.NewDEQClient(conn),
		opts:   opts,
	}
}

// Pub publishes a new event.
func (p *Publisher) Pub(ctx context.Context, e Event) (Event, error) {

	if e.ID == "" {
		return Event{}, fmt.Errorf("e.ID is required")
	}

	payload, err := proto.Marshal(e.Msg)
	if err != nil {
		return Event{}, fmt.Errorf("marshal payload: %v", err)
	}

	var createTime int64
	if !e.CreateTime.IsZero() {
		createTime = e.CreateTime.UnixNano()
	}

	event, err := p.client.Pub(ctx, &api.PubRequest{
		Event: &api.Event{
			Id:         e.ID,
			Topic:      proto.MessageName(e.Msg),
			CreateTime: createTime,
			Payload:    payload,
		},
		AwaitChannel: p.opts.AwaitChannel,
	})
	if err != nil {
		return Event{}, err
	}

	return protoToEvent(event, e.Msg, nil), nil
}

func protoToEvent(event *api.Event, msg Message, sub *Subscriber) Event {

	state := deq.StateUnspecified
	switch event.State {
	case api.Event_QUEUED:
		state = deq.StateQueued
	case api.Event_QUEUED_LINEAR:
		state = deq.StateQueuedLinear
	case api.Event_QUEUED_CONSTANT:
		state = deq.StateQueuedConstant
	case api.Event_OK:
		state = deq.StateOK
	case api.Event_INTERNAL:
		state = deq.StateInternal
	case api.Event_INVALID:
		state = deq.StateInvalid
	case api.Event_SEND_LIMIT_REACHED:
		state = deq.StateSendLimitReached
	case api.Event_DEQUEUED_ERROR:
		state = deq.StateDequeuedError
	}

	return Event{
		ID:           event.Id,
		Msg:          msg,
		CreateTime:   time.Unix(0, event.CreateTime),
		State:        state,
		RequeueCount: int(event.SendCount),
	}
}
