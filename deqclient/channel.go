package deqclient

import (
	"context"
	"io"
	"sync"
	"time"

	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/ack"
	api "gitlab.com/katcheCode/deq/api/v1/deq"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

func (c *clientChannel) GetIndex(ctx context.Context, index string) (deq.Event, error) {
	e, err := c.deqClient.Get(ctx, &api.GetRequest{
		EventId:  index,
		Topic:    c.topic,
		Channel:  c.name,
		UseIndex: true,
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
