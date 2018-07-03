//go:generate protoc -I=. -I$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogofaster_out=plugins=grpc,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types:. deq.proto

package deq

import (
	"context"
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc"
	"reflect"
	"strings"
)

// Client provides a convience layer for DEQClient
type Client struct {
	DEQClient
	handlers map[string]Handler
}

// NewClient creates a new Client
func NewClient(conn *grpc.ClientConn) *Client {
	return &Client{
		NewDEQClient(conn),
		map[string]Handler{},
	}
}

// Handler is a handler for DEQ events.
type Handler interface {
	HandleEvent(context.Context, *Event, Message) error
}

// HandlerFunc is the function type that can be used for registering HandlerFuncs
type HandlerFunc func(context.Context, *Event, Message) error

type handler struct {
	handlerFunc HandlerFunc
}

func (h *handler) HandleEvent(ctx context.Context, e *Event, m Message) error {
	return h.handlerFunc(ctx, e, m)
}

// Handle registers the handler for a given typeURL. If a handler already exists for the typeURL, Handle panics
func (c *Client) Handle(typeURL string, h Handler) {
	url := strings.TrimPrefix(typeURL, "type.googleapis.com/")
	if c.handlers[url] != nil {
		panic(fmt.Sprintf("DEQ: Attempted to register a handler for type %s, which already has a registered handler.", typeURL))
	}
	c.handlers[url] = h
}

// HandleFunc registers the handler func for a given typeURL. If a handler already exists for the typeURL, HandleFunc panics
func (c *Client) HandleFunc(typeURL string, handlerFunc HandlerFunc) {
	c.Handle(typeURL, &handler{handlerFunc})
}

// Stream opens an event stream with deq and routes events to their designated handlers. Any event without a handler is marked WILL_NO_PROCESS
func (c *Client) Stream(ctx context.Context, channel string) error {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errc := make(chan error)
	go c.listen(ctx, channel, errc)

	return <-errc
}

func (c *Client) listen(ctx context.Context, channel string, errc chan error) {
	stream, err := c.StreamEvents(ctx, &StreamEventsRequest{
		Channel: channel,
		Follow:  true,
	})
	if err != nil {
		errc <- errors.New("DEQ: Failed to open event stream: " + err.Error())
		return
	}
	defer stream.CloseSend()

	for {
		event, err := stream.Recv()
		if err != nil {
			errc <- errors.New("Event stream failed: " + err.Error())
			return
		}
		go func() {
			typeURL := strings.TrimPrefix(event.GetPayload().GetTypeUrl(), "type.googleapis.com/")
			handler := c.handlers[typeURL]
			if handler == nil {
				_, err = c.UpdateEventStatus(ctx, &UpdateEventStatusRequest{
					Channel:     channel,
					Key:         event.GetKey(),
					EventStatus: Event_WILL_NOT_PROCESS,
				})
				return
			}
			messageType := proto.MessageType(typeURL)
			if messageType == nil {
				messageType = proto.MessageType("type.googleapis.com/" + typeURL)
			}
			if messageType == nil {
				errc <- errors.New("deq: registered for handler not registered with protobuf: " + typeURL)
				return
			}
			message := reflect.New(messageType.Elem()).Interface().(Message)
			err = types.UnmarshalAny(event.Payload, message)

			status := Event_PROCESSED
			err = handler.HandleEvent(ctx, event, message)
			if err == ErrWillNotProcess {
				status = Event_WILL_NOT_PROCESS
			}
			if err != nil {
				// TODO: We probably need to give someone a chance to handle this
				// log.Printf("Failed to reduce event of type %s: %v", event.GetPayload().GetTypeUrl(), err)
				status = Event_PENDING
			}
			_, err = c.UpdateEventStatus(ctx, &UpdateEventStatusRequest{
				Channel:     channel,
				Key:         event.GetKey(),
				EventStatus: status,
			})
			if err != nil {
				errc <- errors.New("Failed to mark event as processed: " + err.Error())
				return
			}
		}()
	}
}

// ErrWillNotProcess should be returned from a handler to indicate that the event status should be set to WILL_NOT_PROCESS instead of PROCESSED
var ErrWillNotProcess = errors.New("will not process")

// Message is a message payload that is sent by deq
type Message proto.Message
