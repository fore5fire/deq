package deq

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"log"

	"github.com/gogo/protobuf/proto"
	api "gitlab.com/katcheCode/deqd/api/v1/deq"
	"google.golang.org/grpc"
)

// Consumer allows subscribing to events of particular types
type Consumer struct {
	client api.DEQClient
	opts   ConsumerOpts
}

// ConsumerOpts are options for a Consumer
type ConsumerOpts struct {
	Channel string
	MinID   string
	MaxID   string
	Follow  bool
}

// NewConsumer creates a new Consumer.
// conn can be used by multiple Producers and Consumers in parallel
func NewConsumer(conn *grpc.ClientConn, opts ConsumerOpts) *Consumer {
	return &Consumer{api.NewDEQClient(conn), opts}
}

// Handler is a handler for DEQ events.
type Handler interface {
	HandleEvent(context.Context, Event) AckCode
	// NewMessage() Message
}

// HandlerFunc is the function type that can be used for registering HandlerFuncs
type HandlerFunc func(context.Context, Event) AckCode

// HandleEvent implements the Handler interface
func (f HandlerFunc) HandleEvent(ctx context.Context, e Event) AckCode {
	return f(ctx, e)
}

// type handler struct {
// 	handlerFunc HandlerFunc
// }

// Sub begins listening for events on the requested channel.
// m is used to determine the subscribed type url. All messages passed to handler are guarenteed to be of the same concrete type as m
// Because github.com/gogo/protobuf is used to lookup the typeURL and concrete type of m, m must be a registered type with the same instance of gogo/proto as this binary (ie. no vendoring or github.com/golang/protobuf)
func (c *Consumer) Sub(ctx context.Context, m Message, handler HandlerFunc) error {

	msgName := proto.MessageName(m)
	msgType := proto.MessageType(msgName)

	stream, err := c.client.Sub(ctx, &api.SubRequest{
		Channel: c.opts.Channel,
		MinId:   c.opts.MinID,
		MaxId:   c.opts.MaxID,
		Follow:  c.opts.Follow,
		Topic:   msgName,
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
				_, err = c.client.Ack(ctx, &api.AckRequest{
					Channel: c.opts.Channel,
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
				consumer: c,
			})

			_, err = c.client.Ack(ctx, &api.AckRequest{
				Channel: c.opts.Channel,
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

// Producer publishes events via the Pub method
type Producer struct {
	client api.DEQClient
	opts   ProducerOpts
}

// ProducerOpts provides options used by a Producer
type ProducerOpts struct {
	AwaitChannel      string
	AwaitMilliseconds uint32
}

// NewProducer constructs a new Producer.
// conn can be used by multiple Producers and Consumers in parallel
func NewProducer(conn *grpc.ClientConn, opts ProducerOpts) *Producer {
	return &Producer{api.NewDEQClient(conn), opts}
}

// Pub publishes a new event.
func (p *Producer) Pub(ctx context.Context, e Event) error {

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
		AwaitChannel:      p.opts.AwaitChannel,
		AwaitMilliseconds: p.opts.AwaitMilliseconds,
	})
	if err != nil {
		return err
	}

	return nil
}

// Event is a deserialized event that is sent to or recieved from deq.
type Event struct {
	ID       string
	Msg      Message
	consumer *Consumer
}

// ResetTimeout resets the requeue timeout of this event
func (e *Event) ResetTimeout(ctx context.Context) error {
	if e.consumer == nil {
		return errors.New("ResetTimeout is only valid for events created by a consumer")
	}

	_, err := e.consumer.client.Ack(ctx, &api.AckRequest{
		Channel: e.consumer.opts.Channel,
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
	// Return an empty string instead of panicing if not found
	defer recover()
	return proto.MessageName(e.Msg)
}

// AckCode is a code used when acknowledging an event
type AckCode api.AckCode

const (
	// AckCodeDequeueOK dequeues an event, indicating it was processed successfully
	AckCodeDequeueOK = AckCode(api.AckCode_DEQUEUE_OK)
	// AckCodeDequeueError dequeues an event, indicating it was not processed successfully
	AckCodeDequeueError = AckCode(api.AckCode_DEQUEUE_ERROR)
	// AckCodeRequeueConstant requeues an event with no backoff
	AckCodeRequeueConstant = AckCode(api.AckCode_REQUEUE_CONSTANT)
	// AckCodeRequeueLinear requires an event with a linear backoff
	AckCodeRequeueLinear = AckCode(api.AckCode_REQUEUE_LINEAR)
	// AckCodeRequeueExponential requeues an event with an exponential backoff
	AckCodeRequeueExponential = AckCode(api.AckCode_REQUEUE_EXPONENTIAL)
)

// Message is a message payload that is sent by deq
type Message proto.Message
