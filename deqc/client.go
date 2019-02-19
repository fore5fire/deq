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
	"time"

	"github.com/gogo/protobuf/proto"
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
