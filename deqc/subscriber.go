package deqc

import (
	"context"
	"fmt"
	"io"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq/ack"
	api "gitlab.com/katcheCode/deq/api/v1/deq"
	"google.golang.org/grpc"
)

// Subscriber allows subscribing to events of particular types
type Subscriber struct {
	client api.DEQClient
	opts   SubscriberOpts
}

// SubscriberOpts are options for a Subscriber
type SubscriberOpts struct {
	// Required
	Channel      string
	IdleTimeout  time.Duration
	RequeueDelay time.Duration
	// Follow       bool
	// MinID   string
	// MaxID   string
}

// NewSubscriber creates a new Subscriber.
// conn can be used by multiple Producers and Consumers in parallel
func NewSubscriber(conn *grpc.ClientConn, opts SubscriberOpts) *Subscriber {
	if opts.Channel == "" {
		panic("opts.Channel is required")
	}
	return &Subscriber{api.NewDEQClient(conn), opts}
}

// Handler is a handler for DEQ events.
type Handler interface {
	HandleEvent(Event) ack.Code
	// NewMessage() Message
}

// HandlerFunc is the function type that can be used for registering HandlerFuncs
type HandlerFunc func(Event) ack.Code

// HandleEvent implements the Handler interface
func (f HandlerFunc) HandleEvent(e Event) ack.Code {
	return f(e)
}

// type handler struct {
// 	handlerFunc HandlerFunc
// }

// Sub begins listening for events on the requested channel.
//
// m is used to determine the subscribed type url. All messages passed to handler are guarenteed to be of the same concrete type as m
// Because github.com/gogo/protobuf is used to lookup the typeURL and concrete type of m, m must be a registered type with the same instance of gogo/proto as this binary (ie. no vendoring or github.com/golang/protobuf)
func (sub *Subscriber) Sub(ctx context.Context, m Message, handler HandlerFunc) error {

	msgName := proto.MessageName(m)
	msgType := proto.MessageType(msgName)

	stream, err := sub.client.Sub(ctx, &api.SubRequest{
		Channel:                  sub.opts.Channel,
		IdleTimeoutMilliseconds:  int32(sub.opts.IdleTimeout / time.Millisecond),
		Follow:                   sub.opts.IdleTimeout <= 0,
		RequeueDelayMilliseconds: int32(sub.opts.RequeueDelay / time.Millisecond),
		// MinId:   c.opts.MinID,
		// MaxId:   c.opts.MaxID,
		Topic: msgName,
	})
	if err != nil {
		return err
	}
	defer stream.CloseSend()

	wg := sync.WaitGroup{}

	for {
		event, err := stream.Recv()
		if err != nil {
			// wait for running requests to complete
			wg.Wait()
			if err == io.EOF {
				return nil
			}
			return err
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			msg := reflect.New(msgType.Elem()).Interface().(Message)
			err := proto.Unmarshal(event.Payload, msg)
			if err != nil {
				_, err = sub.client.Ack(ctx, &api.AckRequest{
					Channel: sub.opts.Channel,
					Topic:   msgName,
					EventId: event.Id,
					Code:    api.AckCode_DEQUEUE_ERROR,
				})
				if err != nil {
					// TODO: how to expose error?
					log.Printf("deq: unmarshal message for event %s failed: ack: %v", event.Id, err)
				}
				return
			}

			code := handler.HandleEvent(protoToEvent(event, msg, sub))

			_, err = sub.client.Ack(ctx, &api.AckRequest{
				Channel: sub.opts.Channel,
				Topic:   msgName,
				EventId: event.Id,
				Code:    api.AckCode(code),
			})
			if err != nil {
				// TODO: How to expose error?
				log.Printf("deq: event %s handled: ack: %v", event.Id, err)
				return
			}
		}()
	}
}

// Get returns an event for a given id and message type (topic).
//
// The message topic is inferred from the type of `result`. The event payload is also deserialized
// into `result`, such that `e.Msg == result`.
func (sub *Subscriber) Get(ctx context.Context, eventID string, result Message) (e Event, err error) {

	event, err := sub.client.Get(ctx, &api.GetRequest{
		EventId: eventID,
		Channel: sub.opts.Channel,
		Topic:   proto.MessageName(result),
	})
	if err != nil {
		return Event{}, err
	}

	err = proto.Unmarshal(event.Payload, result)
	if err != nil {
		return Event{}, fmt.Errorf("unmarshal payload: %v", err)
	}

	res := protoToEvent(event, result, sub)

	return res, nil
}

// Await returns an event for a given id, waiting until it is published if it doesn't exist.
//
// The message topic is inferred from the type of `result`. The event payload is also deserialized
// into `result`, such that `e.Msg == result`.
func (sub *Subscriber) Await(ctx context.Context, eventID string, result Message) (Event, error) {
	e, err := sub.client.Get(ctx, &api.GetRequest{
		EventId: eventID,
		Channel: sub.opts.Channel,
		Topic:   proto.MessageName(result),
		Await:   true,
	})
	if err != nil {
		return Event{}, err
	}

	err = proto.Unmarshal(e.Payload, result)
	if err != nil {
		return Event{}, fmt.Errorf("unmarshal payload: %v", err)
	}

	res := protoToEvent(e, result, sub)

	return res, nil
}

// ResetTimeout resets the requeue timeout of this event
func (sub *Subscriber) ResetTimeout(ctx context.Context, event Event) error {

	_, err := sub.client.Ack(ctx, &api.AckRequest{
		Channel: sub.opts.Channel,
		Topic:   event.Topic(),
		EventId: event.ID,
		Code:    api.AckCode_RESET_TIMEOUT,
	})
	if err != nil {
		return err
	}

	return nil
}
