package deq

import (
	"context"
	"errors"

	"github.com/gogo/protobuf/proto"
	api "gitlab.com/katcheCode/deqd/api/v1/deq"
	"google.golang.org/grpc"
)

// Consumer allows subscribing to events of particular types
type Consumer struct {
	client api.DEQClient
	opts   ConsumerOptions
}

type ConsumerOptions struct {
	Channel string
	MinID   string
	MaxID   string
	Follow  bool
}

// NewConsumer creates a new Consumer
func NewConsumer(conn *grpc.ClientConn, opts ConsumerOptions) *Consumer {
	return &Consumer{api.NewDEQClient(conn), opts}
}

// Handler is a handler for DEQ events.
type Handler interface {
	HandleEvent(context.Context, Event) api.AckCode
	NewMessage() Message
}

// HandlerFunc is the function type that can be used for registering HandlerFuncs
type HandlerFunc func(context.Context, Event) api.AckCode

// type handler struct {
// 	handlerFunc HandlerFunc
// }

// Sub begins listening for events on the requested channel
func (c *Consumer) Sub(ctx context.Context, handler Handler) error {

	stream, err := c.client.Sub(ctx, &api.SubRequest{
		Channel: c.opts.Channel,
		MinId:   c.opts.MinID,
		MaxId:   c.opts.MaxID,
		Follow:  c.opts.Follow,
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
			// msg := reflect.New(msgType.Elem()).Interface().(Message)
			msg := handler.NewMessage()
			err := proto.Unmarshal(event.Payload, msg)
			if err != nil {
				_, err = c.client.Ack(ctx, &api.AckRequest{
					Channel: c.opts.Channel,
					EventId: event.Id,
					Code:    api.AckCode_DEQUEUE_FAILED,
				})
				if err != nil {
					// how to expose error?
				}
				return
			}

			code := handler.HandleEvent(ctx, Event{
				ID:      event.Id,
				TypeURL: event.TypeUrl,
				Message: msg,
			})

			_, err = c.client.Ack(ctx, &api.AckRequest{
				Channel: c.opts.Channel,
				EventId: event.Id,
				Code:    code,
			})
			if err != nil {
				// How to expose error?
				return
			}
		}()
	}
}

type Producer struct {
	client api.DEQClient
	opts   ProducerOptions
}

type ProducerOptions struct {
	// Number of miliseconds to wait before queueing on this channel
	AwaitChannel      string
	AwaitMilliseconds uint32
}

func NewProducer(conn *grpc.ClientConn, opts ProducerOptions) *Producer {
	return &Producer{api.NewDEQClient(conn), opts}
}

func (p *Producer) Pub(ctx context.Context, e Event) error {

	_, err := p.client.Pub(ctx, &api.PubRequest{
		Event:             &api.Event{},
		AwaitChannel:      p.opts.AwaitChannel,
		AwaitMilliseconds: p.opts.AwaitMilliseconds,
	})
	if err != nil {
		return err
	}

	return nil
}

//
// func (c *Client) handle(ctx context.Context, errc chan error) error {
//
// 	for {
//
// 		go func() {
// 			typeURL := strings.TrimPrefix(event.GetPayload().GetTypeUrl(), "type.googleapis.com/")
// 			handler := c.handlers[typeURL]
// 			if handler == nil {
//
// 				return
// 			}
// 			if messageType == nil {
// 				messageType = proto.MessageType("type.googleapis.com/" + typeURL)
// 			}
// 			if messageType == nil {
// 				errc <- errors.New("deq: registered for handler not registered with protobuf: " + typeURL)
// 				return
// 			}
// 			message := reflect.New(messageType.Elem()).Interface().(Message)
// 			err = types.UnmarshalAny(event.Payload, message)
//
// 			status := api.Acknowledgment_DEQUEUE // Special Skip ?
// 			err = handler.HandleEvent(ctx, event, message)
// 			if err == ErrWillNotProcess {
// 				status = Event_WILL_NOT_PROCESS
// 			}
// 			if err != nil {
// 				// TODO: We probably need to give someone a chance to handle this
// 				// log.Printf("Failed to reduce event of type %s: %v", event.GetPayload().GetTypeUrl(), err)
// 				status = Event_PENDING
// 			}
// 			_, err = c.UpdateEventStatus(ctx, &UpdateEventStatusRequest{
// 				Channel:     channel,
// 				Key:         event.GetKey(),
// 				EventStatus: status,
// 			})
// 			if err != nil {
// 				errc <- errors.New("Failed to mark event as processed: " + err.Error())
// 				return
// 			}
// 		}()
// 	}
// }

type Event struct {
	ID      []byte
	TypeURL string
	Message Message
}

// ErrWillNotProcess should be returned from a handler to indicate that the event status should be set to WILL_NOT_PROCESS instead of PROCESSED
var ErrWillNotProcess = errors.New("will not process")

// Message is a message payload that is sent by deq
type Message proto.Message
