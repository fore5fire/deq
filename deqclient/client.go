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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Client struct {
	client api.DEQClient
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

	event, err := c.client.Pub(ctx, &api.PubRequest{
		Event: eventToProto(e),
	})
	if err != nil {
		return deq.Event{}, err
	}

	return eventFromProto(event), nil
}

// Del deletes a previously published event.
func (c *Client) Del(ctx context.Context, topic, id string) error {
	_, err := c.client.Del(ctx, &api.DelRequest{
		Topic:   topic,
		EventId: id,
	})
	if status.Code(err) == codes.NotFound {
		return deq.ErrNotFound
	}
	if err != nil {
		return err
	}

	return nil
}

func eventStateFromProto(state api.EventState) deq.EventState {
	switch state {
	case api.EventState_DEQUEUED_ERROR:
		return deq.EventStateDequeuedError
	case api.EventState_DEQUEUED_OK:
		return deq.EventStateDequeuedOK
	case api.EventState_QUEUED:
		return deq.EventStateQueued
	default:
		return deq.EventStateUnspecified
	}
}

func eventStateToProto(state deq.EventState) api.EventState {
	switch state {
	case deq.EventStateDequeuedError:
		return api.EventState_DEQUEUED_ERROR
	case deq.EventStateDequeuedOK:
		return api.EventState_DEQUEUED_OK
	case deq.EventStateQueued:
		return api.EventState_QUEUED
	default:
		return api.EventState_UNSPECIFIED_STATE
	}
}

func eventFromProto(e *api.Event) deq.Event {

	// If CreateTime is a zero as a unix timestamp, don't convert it because time.Time{}.UnixNano() != 0
	var createTime time.Time
	if e.CreateTime == 0 {
		createTime = time.Unix(0, e.CreateTime)
	}

	return deq.Event{
		ID:           e.Id,
		Topic:        e.Topic,
		Payload:      e.Payload,
		CreateTime:   createTime,
		Indexes:      e.Indexes,
		DefaultState: eventStateFromProto(e.DefaultState),
		State:        eventStateFromProto(e.State),
		RequeueCount: int(e.RequeueCount),
	}
}

func eventToProto(e deq.Event) *api.Event {

	// If CreateTime is a zero as a go time, don't convert it because time.Time{}.UnixNano() != 0
	var createTime int64
	if !e.CreateTime.IsZero() {
		createTime = e.CreateTime.UnixNano()
	}

	return &api.Event{
		Id:           e.ID,
		Topic:        e.Topic,
		Payload:      e.Payload,
		CreateTime:   createTime,
		Indexes:      e.Indexes,
		DefaultState: eventStateToProto(e.DefaultState),
		State:        eventStateToProto(e.State),
		RequeueCount: int32(e.RequeueCount),
	}
}
