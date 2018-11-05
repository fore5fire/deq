package deq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"log"

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
	Channel      string
	IdleTimeout  time.Duration
	RequeueDelay time.Duration
	Follow       bool
	// MinID   string
	// MaxID   string
}

// NewSubscriber creates a new Subscriber.
// conn can be used by multiple Producers and Consumers in parallel
func NewSubscriber(conn *grpc.ClientConn, opts SubscriberOpts) *Subscriber {
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
// m is used to determine the subscribed type url. All messages passed to handler are guarenteed to be of the same concrete type as m
// Because github.com/gogo/protobuf is used to lookup the typeURL and concrete type of m, m must be a registered type with the same instance of gogo/proto as this binary (ie. no vendoring or github.com/golang/protobuf)
func (sub *Subscriber) Sub(ctx context.Context, m Message, handler HandlerFunc) error {

	msgName := proto.MessageName(m)
	msgType := proto.MessageType(msgName)

	stream, err := sub.client.Sub(ctx, &api.SubRequest{
		Channel:                 sub.opts.Channel,
		IdleTimeoutMilliseconds: int32(sub.opts.IdleTimeout / time.Millisecond),
		Follow:                  sub.opts.Follow,
		// RequeueDelayMilliseconds: int32(sub.opts.RequeueDelay / time.Millisecond),
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
	return &Publisher{api.NewDEQClient(conn), opts}
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

	event, err := p.client.Pub(ctx, &api.PubRequest{
		Event: &api.Event{
			Id:         e.ID,
			Topic:      proto.MessageName(e.Msg),
			CreateTime: e.CreateTime.UnixNano(),
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

	var state EventState
	switch event.State {
	case api.EventState_QUEUED:
		state = EventStateQueued
	case api.EventState_DEQUEUED_OK:
		state = EventStateDequeuedOK
	case api.EventState_DEQUEUED_ERROR:
	default:
		state = EventStateUnspecified
	}

	return Event{
		ID:           event.Id,
		Msg:          msg,
		CreateTime:   time.Unix(0, event.CreateTime),
		State:        state,
		RequeueCount: int(event.RequeueCount),
		sub:          sub,
	}
}

// Event is a deserialized event that is sent to or recieved from deq.
type Event struct {
	ID           string
	Msg          Message
	CreateTime   time.Time
	State        EventState
	RequeueCount int
	sub          *Subscriber
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

// ResetTimeout resets the requeue timeout of this event
func (e *Event) ResetTimeout(ctx context.Context) error {
	if e.sub == nil {
		return errors.New("ResetTimeout is only valid for events created by a Subscriber")
	}

	_, err := e.sub.client.Ack(ctx, &api.AckRequest{
		Channel: e.sub.opts.Channel,
		Topic:   e.Topic(),
		EventId: e.ID,
		Code:    api.AckCode_RESET_TIMEOUT,
	})
	if err != nil {
		return err
	}

	return nil
}

// Topic returns the topic of this event by inspecting it's Message type, or
// an empty string if the topic could not be determined.
func (e *Event) Topic() string {
	return proto.MessageName(e.Msg)
}

// Message is a message payload that is sent by deq
type Message proto.Message
