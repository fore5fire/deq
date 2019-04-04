package deqclient

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/ack"
	api "gitlab.com/katcheCode/deq/api/v1/deq"
	"google.golang.org/grpc"
)

type testSubDEQClient struct {
	api.DEQClient
	Events chan *api.Event
}
type testSubClient struct {
	deq.Client
}

func (c testSubClient) Pub(ctx context.Context, e deq.Event) (deq.Event, error) {
	return e, nil
}

type testSubDEQStream struct {
	api.DEQ_SubClient
	Events chan *api.Event
}

func (c testSubDEQClient) Sub(context.Context, *api.SubRequest, ...grpc.CallOption) (api.DEQ_SubClient, error) {
	return testSubDEQStream{
		Events: c.Events,
	}, nil
}

func (c testSubDEQClient) Ack(context.Context, *api.AckRequest, ...grpc.CallOption) (*api.AckResponse, error) {
	return &api.AckResponse{}, nil
}

func (s testSubDEQStream) Recv() (*api.Event, error) {
	e, ok := <-s.Events
	if !ok {
		return nil, context.Canceled
	}
	return e, nil
}

func TestSub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	expected := deq.Event{
		ID:         "test-event",
		Topic:      "recieved",
		CreateTime: time.Now().Round(0),
	}

	in := make(chan *api.Event)

	channel := &clientChannel{
		client: testSubClient{},
		deqClient: testSubDEQClient{
			Events: in,
		},
	}

	out := make(chan deq.Event)

	go func() {
		channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, ack.Code) {
			out <- e
			return &deq.Event{
				ID:    "response",
				Topic: "TestSub-response",
			}, ack.DequeueOK
		})
	}()

	for i := 0; i < 20; i++ {
		in <- &api.Event{
			Id:         expected.ID,
			Topic:      expected.Topic,
			CreateTime: expected.CreateTime.UnixNano(),
		}
		e := <-out
		if !cmp.Equal(expected, e) {
			t.Errorf("recieved event:\n%s", cmp.Diff(expected, e))
		}
	}
}
