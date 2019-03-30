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
	"io"
	"sync"
	"time"

	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/ack"
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

type clientChannel struct {
	client      *Client
	deqClient   api.DEQClient
	name, topic string
}

func (c *Client) Channel(name, topic string) deq.Channel {
	return &clientChannel{
		client:    c,
		deqClient: c.client,
		name:      name,
		topic:     topic,
	}
}

func (c *clientChannel) Get(ctx context.Context, id string) (deq.Event, error) {
	e, err := c.deqClient.Get(ctx, &api.GetRequest{
		EventId: id,
		Topic:   c.topic,
		Channel: c.name,
	})
	if status.Code(err) == codes.NotFound {
		return deq.Event{}, deq.ErrNotFound
	}
	if err != nil {
		return deq.Event{}, err
	}

	return eventFromProto(e), nil
}

func (c *clientChannel) Await(ctx context.Context, id string) (deq.Event, error) {
	e, err := c.deqClient.Get(ctx, &api.GetRequest{
		EventId: id,
		Topic:   c.topic,
		Channel: c.name,
		Await:   true,
	})
	if status.Code(err) == codes.NotFound {
		return deq.Event{}, deq.ErrNotFound
	}
	if err != nil {
		return deq.Event{}, err
	}

	return eventFromProto(e), nil
}

func (c *clientChannel) SetEventState(ctx context.Context, id string, state deq.EventState) error {
	var code api.AckCode
	switch state {
	case deq.EventStateDequeuedError:
		code = api.AckCode_DEQUEUE_ERROR
	case deq.EventStateDequeuedOK:
		code = api.AckCode_DEQUEUE_OK
	case deq.EventStateQueued:
		code = api.AckCode_REQUEUE_CONSTANT
	}

	_, err := c.deqClient.Ack(ctx, &api.AckRequest{
		Channel: c.name,
		Topic:   c.topic,
		EventId: id,
		Code:    code,
	})
	if status.Code(err) == codes.NotFound {
		return deq.ErrNotFound
	}
	if err != nil {
		return err
	}

	return nil
}

func (c *clientChannel) Sub(ctx context.Context, handler deq.SubHandler) error {
	stream, err := c.deqClient.Sub(ctx, &api.SubRequest{
		Channel: c.name,
		Topic:   c.topic,
	})
	if err != nil {
		return err
	}

	errc := make(chan error, 1)

	type Result struct {
		req  deq.Event
		resp *deq.Event
		code ack.Code
	}

	// Wait for background goroutine to cleanup before returning
	var wg sync.WaitGroup
	defer wg.Wait()

	results := make(chan Result, 30)
	defer close(results)

	ctx, cancel := context.WithCancel(ctx)

	// workers handle results without blocking processing of next event
	const numWorkers = 3
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			defer cancel()

			for result := range results {

				if result.resp != nil {
					_, err := c.client.Pub(ctx, *result.resp)
					if err != nil {
						select {
						case errc <- err:
						default:
						}
						continue
					}
				}

				c.deqClient.Ack(ctx, &api.AckRequest{
					Channel: c.name,
					Topic:   c.topic,
					EventId: result.req.ID,
				})
			}
		}()
	}

	for {
		e, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		event := eventFromProto(e)

		response, code := handler(ctx, event)
		select {
		case results <- Result{event, response, code}:
		case err := <-errc:
			return err
		}
	}
}

func (c *clientChannel) RequeueEvent(ctx context.Context, e deq.Event, delay time.Duration) error {
	_, err := c.deqClient.Ack(ctx, &api.AckRequest{
		Channel: c.name,
		Topic:   c.topic,
		EventId: e.ID,
		Code:    api.AckCode_REQUEUE_EXPONENTIAL,
	})
	if status.Code(err) == codes.NotFound {
		return deq.ErrNotFound
	}
	if err != nil {
		return err
	}

	return nil
}

func (c *clientChannel) Close() {

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
