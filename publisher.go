package deq

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/types"
	"gitlab.com/katcheCode/deqd/api/v1/deq"
	"google.golang.org/grpc"
)

// Event is a publishable event
type Event struct {
	ID  string
	Msg deq.Message
}

// Publisher is a deq client for publishing events
type Publisher struct {
	deqc *deq.Client
}

// NewPublisher returns a new publisher to deq using a deq ClientConn
func NewPublisher(deqConn *grpc.ClientConn) *Publisher {
	deqc := deq.NewClient(deqConn)

	return &Publisher{
		deqc: deqc,
	}
}

// Pub publishes an event with message m
func (p *Publisher) Pub(ctx context.Context, e *Event) error {
	payload, err := types.MarshalAny(e.Msg)
	if err != nil {
		return fmt.Errorf("marshal payload: %v", err)
	}
	_, err = p.deqc.CreateEvent(ctx, &deq.CreateEventRequest{
		Event: &deq.Event{
			Id:      []byte(e.ID),
			Payload: payload,
		},
	})
	if err != nil {
		return err
	}

	return nil
}
