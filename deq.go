package deq

import (
	"context"
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	api "gitlab.com/katcheCode/deqd/api/v1/deq"
	"google.golang.org/grpc"
	"reflect"
)

// Client provides a convience layer for DEQClient
type Client struct {
	api.DEQClient
	// handlers map[string]Handler
}

// NewClient creates a new Client
func NewClient(conn *grpc.ClientConn) *Client {
	return &Client{
		api.NewDEQClient(conn),
		// map[string]Handler{},
	}
}

// // Handler is a handler for DEQ events.
// type Handler interface {
// 	HandleEvent(context.Context, Event) error
// }

// HandlerFunc is the function type that can be used for registering HandlerFuncs
type HandlerFunc func(context.Context, Event) api.AckCode

// type handler struct {
// 	handlerFunc HandlerFunc
// }

// Sub begins listening for events on the requested channel
func (c *Consumer) Sub(ctx context.Context, req *api.SubRequest, hf HandlerFunc) error {

	msgType := proto.MessageType(req.TypeUrl)
	if msgType == nil {
		return fmt.Errorf("typeURL %s has no registered type with gogo/protobuf", req.TypeUrl)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := c.DEQClient.Sub(ctx, req)
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
				_, err = c.Ack(ctx, &api.AckRequest{
					Channel: req.Channel,
					EventId: event.Id,
					Code:    api.AckCode_DEQUEUE_FAILED,
				})
				if err != nil {
					// how to expose error?
				}
				return
			}

			code := hf(ctx, Event{
				ID:      event.Id,
				TypeURL: event.TypeUrl,
				Message: msg,
			})

			_, err = c.Ack(ctx, &api.AckRequest{
				Channel: req.Channel,
				EventId: event.Id,
				Code:    code,
			})
			if err != nil {
				// How to expose error?
			}
		}()
	}
}

func (c *Producer) Pub(ctx context.Context, e Event) {

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
