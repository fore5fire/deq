package deq

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/types"
	"gitlab.com/katcheCode/deqd/api/v1/deq"
	"google.golang.org/grpc"
)

// Publisher is a deq client for publishing events
type Publisher struct {
	deqc *deq.Client
}

// Pub publishes an event with message m
func (p *Publisher) Pub(ctx context.Context, m deq.Message) error {
	payload, err := types.MarshalAny(m)
	if err != nil {
		return fmt.Errorf("marshal payload: %v", err)
	}
	_, err = p.deqc.CreateEvent(ctx, &deq.CreateEventRequest{
		Event: &deq.Event{
			Payload: payload,
		},
	})
	if err != nil {
		return err
	}

	return nil
}

// NewPublisher returns a new publisher to deq using a deq ClientConn
func NewPublisher(deqConn *grpc.ClientConn) *Publisher {
	deqc := deq.NewClient(deqConn)

	return &Publisher{
		deqc: deqc,
	}
}
