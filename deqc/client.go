/*
Package deqc provides a go client for DEQ.

To connect to a DEQ server, dial the server with GRPC:

	conn, err := grpc.Dial("deq.example.com", grpc.WithInsecure())
	if err != nil {
		// Handle error
	}

grpc connections are multiplexed, so you can create multiple publishers and subscribers with different settings
using a single connection.


To publish events to a DEQ server, create a new Publisher:

	publisher := deq.NewPublisher(conn, deq.PublisherOpts{})

and call Pub:

	event, err := publisher.Pub(ctx, deq.Event{
		ID: "some-id", // IDs are used for idempotency, so choose them appropriately.
		Msg: &types.Empty{}, // Add payload as protobuf message here.
	})
	if err != nil {
		// Handle error
	}


To subscribe to events from a DEQ server, create a new Subscriber:

	subscriber := deq.NewSubscriber(conn, deq.SubscriberOpts{
		Channel: "some-channel",
	})

and call Sub:

	err := subscriber.Sub(ctx, &types.Empty{}, func(e deq.Event) ack.Code {
		msg := e.Msg.(*types.Empty)
		// process the event
		return ack.DequeueOK
	})
	if err != nil {
		// Subscription failed, handle error
	}

subscribers can also Get and Await events.

*/
package deqc

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq"
	api "gitlab.com/katcheCode/deq/api/v1/deq"
	"google.golang.org/grpc"
)

// Event is a deserialized event that is sent to or recieved from deq.
type Event struct {
	ID           string
	Msg          Message
	CreateTime   time.Time
	State        EventState
	RequeueCount int
}

// EventState is the queue state of an event
type EventState string

const (
	// EventStateUnspecified indicates the event state is unspecified.
	EventStateUnspecified EventState = ""
	// EventStateQueued indicates the event is queued on this channel.
	EventStateQueued = "Queued"
	// EventStateDequeuedOK indicates the event was processed with no error on this channel.
	EventStateDequeuedOK = "DequeuedOK"
	// EventStateDequeuedError indicates the event was processed with an error and should not be retried.
	EventStateDequeuedError = "DequeuedError"
)

// Topic returns the topic of this event by inspecting it's Message type, or an empty string if the
// topic could not be determined.
func (e Event) Topic() string {
	return proto.MessageName(e.Msg)
}

// Equal returns true if e and other are equivelant and false otherwise.
func (e Event) Equal(other Event) bool {
	return e.ID == other.ID &&
		proto.Equal(e.Msg, other.Msg) &&
		e.CreateTime == other.CreateTime &&
		e.State == other.State &&
		e.RequeueCount == other.RequeueCount
}

// Message is a message payload that is sent by deq
type Message proto.Message

type Client struct {
	client api.DEQClient
	opts   PublisherOpts
}

// NewClient constructs a new client.
// conn can be used by multiple Publishers and Subscribers in parallel
func NewClient(conn *grpc.ClientConn) *Client {
	return &Client{
		client: api.NewDEQClient(conn),
	}
}

// Pub publishes a new event.
func (c *Client) Pub(ctx context.Context, e deq.Event) (deq.Event, error) {

	if e.ID == "" {
		return deq.Event{}, fmt.Errorf("e.ID is required")
	}

	defaultState := api.EventState_UNSPECIFIED_STATE
	switch e.DefaultState {
	case deq.EventStateDequeuedOK:
		defaultState = api.EventState_DEQUEUED_OK
	case deq.EventStateDequeuedError:
		defaultState = api.EventState_DEQUEUED_ERROR
	case deq.EventStateQueued:
		defaultState = api.EventState_QUEUED
	}

	event, err := c.client.Pub(ctx, &api.PubRequest{
		Event: &api.Event{
			Id:           e.ID,
			Topic:        e.Topic,
			CreateTime:   e.CreateTime.UnixNano(),
			Payload:      e.Payload,
			DefaultState: defaultState,
		},
	})
	if err != nil {
		return deq.Event{}, err
	}

	state := deq.EventStateUnspecified
	switch event.State {
	case api.EventState_DEQUEUED_OK:
		state = deq.EventStateDequeuedOK
	case api.EventState_DEQUEUED_ERROR:
		state = deq.EventStateDequeuedError
	case api.EventState_QUEUED:
		state = deq.EventStateQueued
	}

	dState := deq.EventStateUnspecified
	switch event.DefaultState {
	case api.EventState_DEQUEUED_OK:
		dState = deq.EventStateDequeuedOK
	case api.EventState_DEQUEUED_ERROR:
		dState = deq.EventStateDequeuedError
	case api.EventState_QUEUED:
		dState = deq.EventStateQueued
	}

	return deq.Event{
		ID:           event.Id,
		Topic:        event.Topic,
		CreateTime:   time.Unix(0, event.CreateTime),
		Payload:      event.Payload,
		DefaultState: dState,
		State:        state,
	}, nil
}
