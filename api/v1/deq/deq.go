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
	HandleEvent(context.Context, *Event, proto.Message) error
}

type handler struct {
	handlerFunc func(context.Context, *Event, proto.Message) error
}

func (h *handler) HandleEvent(ctx context.Context, e *Event, m proto.Message) error {
	return h.handlerFunc(ctx, e, m)
}

// Handle registers the handler for a given typeURL. If a handler already exists for the typeURL, Handle panics
func (c *Client) Handle(typeURL string, h Handler) {
	if c.handlers[typeURL] != nil {
		panic(fmt.Sprintf("DEQ: Attempted to register a handler for type %s, which already has a registered handler.", typeURL))
	}
	c.handlers[typeURL] = h
}

// HandleFunc registers the handler func for a given typeURL. If a handler already exists for the typeURL, HandleFunc panics
func (c *Client) HandleFunc(typeURL string, handlerFunc func(context.Context, *Event, proto.Message) error) {
	c.Handle(typeURL, &handler{handlerFunc})
}

// Stream opens an event stream with deq and routes events to their designated handlers. Any event without a handler is marked WILL_NO_PROCESS
func (c *Client) Stream(ctx context.Context, channel string) error {
	stream, err := c.StreamEvents(ctx, &StreamEventsRequest{
		Channel: channel,
		Follow:  true,
	})
	if err != nil {
		return errors.New("DEQ: Failed to open event stream: " + err.Error())
	}
	defer stream.CloseSend()

	for {
		event, err := stream.Recv()
		if err != nil {
			return errors.New("Event stream failed: " + err.Error())
		}
		typeURL := event.GetPayload().GetTypeUrl()
		handler := c.handlers[typeURL]
		if handler == nil {
			_, err = c.UpdateEventStatus(ctx, &UpdateEventStatusRequest{
				Channel:     channel,
				Key:         event.GetKey(),
				EventStatus: Event_WILL_NOT_PROCESS,
			})
			continue
		}
		messageType := proto.MessageType(typeURL)
		message := reflect.New(messageType).Interface().(proto.Message)
		err = types.UnmarshalAny(event.Payload, message)
		err = handler.HandleEvent(ctx, event, message)
		if err != nil {
			// log.Printf("Failed to reduce event of type %s: %v", event.GetPayload().GetTypeUrl(), err)
			//  Not sure yet
			continue
		}
		_, err = c.UpdateEventStatus(ctx, &UpdateEventStatusRequest{
			Channel:     channel,
			Key:         event.GetKey(),
			EventStatus: Event_PROCESSED,
		})
		if err != nil {
			return errors.New("Failed to mark event as processed: " + err.Error())
		}
	}
}
