package deq

import (
	"context"
	"errors"
	"fmt"
	"reflect"

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
	Channel string
	// MinID   string
	// MaxID   string
	Follow bool
}

// NewSubscriber creates a new Subscriber.
// conn can be used by multiple Producers and Consumers in parallel
func NewSubscriber(conn *grpc.ClientConn, opts SubscriberOpts) *Subscriber {
	return &Subscriber{api.NewDEQClient(conn), opts}
}

// Handler is a handler for DEQ events.
type Handler interface {
	HandleEvent(context.Context, Event) ack.Code
	// NewMessage() Message
}

// HandlerFunc is the function type that can be used for registering HandlerFuncs
type HandlerFunc func(context.Context, Event) ack.Code

// HandleEvent implements the Handler interface
func (f HandlerFunc) HandleEvent(ctx context.Context, e Event) ack.Code {
	return f(ctx, e)
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
		Channel: sub.opts.Channel,
		// MinId:   c.opts.MinID,
		// MaxId:   c.opts.MaxID,
		Follow: sub.opts.Follow,
		Topic:  msgName,
	})
	if err != nil {
		return err
	}
	defer stream.CloseSend()
	for {
		event, err := stream.Recv()
		if err != nil {
			return err
		}

		go func() {
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
					log.Printf("deq: unmarshal message failed: ack: %v", err)
				}
				return
			}

			code := handler.HandleEvent(ctx, Event{
				ID:  event.Id,
				Msg: msg,
				// State:
				sub: sub,
			})

			_, err = sub.client.Ack(ctx, &api.AckRequest{
				Channel: sub.opts.Channel,
				Topic:   msgName,
				EventId: event.Id,
				Code:    api.AckCode(code),
			})
			if err != nil {
				// TODO: How to expose error?
				log.Println(event)
				log.Printf("deq: event handled: ack: %v", err)
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
func (p *Publisher) Pub(ctx context.Context, e Event) error {

	if e.ID == "" {
		return fmt.Errorf("e.ID is required")
	}

	payload, err := proto.Marshal(e.Msg)
	if err != nil {
		return fmt.Errorf("marshal payload: %v", err)
	}

	_, err = p.client.Pub(ctx, &api.PubRequest{
		Event: &api.Event{
			Id:      e.ID,
			Topic:   proto.MessageName(e.Msg),
			Payload: payload,
		},
		AwaitChannel: p.opts.AwaitChannel,
	})
	if err != nil {
		return err
	}

	return nil
}

// Event is a deserialized event that is sent to or recieved from deq.
type Event struct {
	ID  string
	Msg Message
	sub *Subscriber
}

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
