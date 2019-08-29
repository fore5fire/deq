/*
Package deqclient provides a go client for DEQ.

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
package deqclient

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/katcheCode/deq"
	api "gitlab.com/katcheCode/deq/api/v1/deq"
	"gitlab.com/katcheCode/deq/deqerr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Client struct {
	client api.DEQClient
}

// New constructs a new client.
//
// conn can be used by multiple Publishers and Subscribers in parallel
func New(conn *grpc.ClientConn) *Client {
	return &Client{
		client: api.NewDEQClient(conn),
	}
}

// Pub publishes a new event.
func (c *Client) Pub(ctx context.Context, e deq.Event) (deq.Event, error) {

	if e.ID == "" {
		return deq.Event{}, fmt.Errorf("e.ID is required")
	}

	event, err := c.client.Pub(ctx, &api.PubRequest{
		Event: eventToProto(e),
	})
	if err != nil {
		return deq.Event{}, errFromGRPC(ctx, err)
	}

	return eventFromProto(selectedEvent{
		Event:    event,
		Selector: event.Id,
	}), nil
}

// Del deletes a previously published event.
func (c *Client) Del(ctx context.Context, topic, id string) error {
	_, err := c.client.Del(ctx, &api.DelRequest{
		Topic:   topic,
		EventId: id,
	})
	if err != nil {
		return errFromGRPC(ctx, err)
	}

	return nil
}

func eventStateFromProto(state api.Event_State) deq.State {
	switch state {
	case api.Event_OK:
		return deq.StateOK
	case api.Event_QUEUED:
		return deq.StateQueued
	case api.Event_QUEUED_LINEAR:
		return deq.StateQueuedLinear
	case api.Event_QUEUED_CONSTANT:
		return deq.StateQueuedConstant
	case api.Event_INTERNAL:
		return deq.StateInternal
	case api.Event_INVALID:
		return deq.StateInvalid
	case api.Event_SEND_LIMIT_REACHED:
		return deq.StateSendLimitReached
	case api.Event_DEQUEUED_ERROR:
		return deq.StateDequeuedError
	default:
		return deq.StateUnspecified
	}
}

func eventStateToProto(state deq.State) api.Event_State {
	switch state {
	case deq.StateOK:
		return api.Event_OK
	case deq.StateQueued:
		return api.Event_QUEUED
	case deq.StateQueuedLinear:
		return api.Event_QUEUED_LINEAR
	case deq.StateQueuedConstant:
		return api.Event_QUEUED_CONSTANT
	case deq.StateInternal:
		return api.Event_INTERNAL
	case deq.StateInvalid:
		return api.Event_INVALID
	case deq.StateSendLimitReached:
		return api.Event_SEND_LIMIT_REACHED
	case deq.StateDequeuedError:
		return api.Event_DEQUEUED_ERROR
	default:
		return api.Event_UNSPECIFIED_STATE
	}
}

func eventFromProto(e selectedEvent) deq.Event {

	// If CreateTime is a zero as a unix timestamp, don't convert it because time.Time{}.UnixNano() != 0
	var createTime time.Time
	if e.Event.CreateTime != 0 {
		createTime = time.Unix(0, e.Event.CreateTime)
	}

	return deq.Event{
		ID:              e.Event.Id,
		Topic:           e.Event.Topic,
		Payload:         e.Event.Payload,
		CreateTime:      createTime,
		Indexes:         e.Event.Indexes,
		DefaultState:    eventStateFromProto(e.Event.DefaultState),
		State:           eventStateFromProto(e.Event.State),
		SendCount:       int(e.Event.SendCount),
		Selector:        e.Selector,
		SelectorVersion: e.Event.SelectorVersion,
	}
}

func eventToProto(e deq.Event) *api.Event {

	// If CreateTime is a zero as a go time, don't convert it because time.Time{}.UnixNano() != 0
	var createTime int64
	if !e.CreateTime.IsZero() {
		createTime = e.CreateTime.UnixNano()
	}

	return &api.Event{
		Id:              e.ID,
		Topic:           e.Topic,
		Payload:         e.Payload,
		CreateTime:      createTime,
		Indexes:         e.Indexes,
		DefaultState:    eventStateToProto(e.DefaultState),
		State:           eventStateToProto(e.State),
		SendCount:       int32(e.SendCount),
		SelectorVersion: e.SelectorVersion,
	}
}

func errFromGRPC(ctx context.Context, err error) error {

	if err == nil {
		return nil
	}

	if ctx.Err() == err {
		return deqerr.FromContext(ctx)
	}

	var code deqerr.Code
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.NotFound:
		return deq.ErrNotFound
	case codes.Unavailable:
		code = deqerr.Unavailable
	case codes.AlreadyExists:
		code = deqerr.Dup
	case codes.Internal:
		code = deqerr.Internal
	case codes.InvalidArgument:
		code = deqerr.Invalid
	case codes.Canceled, codes.DeadlineExceeded:
		code = deqerr.Canceled
	default:
		code = deqerr.Unknown
	}
	return deqerr.Wrap(code, err)
}
