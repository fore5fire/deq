package deqclient

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"gitlab.com/katcheCode/deq/deqopt"

	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/ack"
	api "gitlab.com/katcheCode/deq/api/v1/deq"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type clientChannel struct {
	client      deq.Client
	deqClient   api.DEQClient
	name, topic string

	initialDelay time.Duration
	idleTimeout  time.Duration
}

func (c *Client) Channel(name, topic string) deq.Channel {
	if name == "" {
		panic("name is required")
	}
	if topic == "" {
		panic("topic is required")
	}
	if strings.Contains(topic, "\x00") {
		panic("topic is invalid")
	}
	return &clientChannel{
		client:    c,
		deqClient: c.client,
		name:      name,
		topic:     topic,
	}
}

// SetInitialResendDelay sets the initial send delay of an event's first resend on the channel.
// This is used as a base value from which any backoff is applied.
func (c *clientChannel) SetInitialResendDelay(delay time.Duration) {
	c.initialDelay = delay
}

func (c *clientChannel) SetIdleTimeout(idleTimeout time.Duration) {
	c.idleTimeout = idleTimeout
}

func (c *clientChannel) Get(ctx context.Context, event string, options ...deqopt.GetOption) (deq.Event, error) {

	opts := deqopt.NewGetOptionSet(options)

	e, err := c.deqClient.Get(ctx, &api.GetRequest{
		Event:    event,
		Topic:    c.topic,
		Channel:  c.name,
		Await:    opts.Await,
		UseIndex: opts.UseIndex,
	})
	if status.Code(err) == codes.NotFound {
		return deq.Event{}, deq.ErrNotFound
	}
	if err != nil {
		return deq.Event{}, err
	}

	return eventFromProto(e), nil
}

func (c *clientChannel) BatchGet(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]deq.Event, error) {

	opts := deqopt.NewBatchGetOptionSet(options)

	if opts.Await {
		return nil, fmt.Errorf("BatchGet with option Await() is not yet implemented")
	}

	resp, err := c.deqClient.BatchGet(ctx, &api.BatchGetRequest{
		Events:        ids,
		Topic:         c.topic,
		Channel:       c.name,
		UseIndex:      opts.UseIndex,
		AllowNotFound: opts.AllowNotFound,
	})
	if err != nil {
		return nil, err
	}

	result := make(map[string]deq.Event, len(resp.Events))
	for id, e := range resp.Events {
		result[id] = eventFromProto(e)
	}

	return result, nil
}

func (c *clientChannel) SetEventState(ctx context.Context, id string, state deq.State) error {
	var code api.AckCode
	switch state {
	case deq.StateOK:
		code = api.AckCode_OK
	case deq.StateQueued:
		code = api.AckCode_REQUEUE_CONSTANT
	case deq.StateInvalid:
		code = api.AckCode_INVALID
	case deq.StateInternal:
		code = api.AckCode_INTERNAL
	case deq.StateDequeuedError:
		code = api.AckCode_DEQUEUE_ERROR
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := c.deqClient.Sub(ctx, &api.SubRequest{
		Channel:                 c.name,
		Topic:                   c.topic,
		Follow:                  c.idleTimeout == 0,
		ResendDelayMilliseconds: int32(c.initialDelay.Nanoseconds() / int64(time.Millisecond)),
		IdleTimeoutMilliseconds: int32(c.idleTimeout.Nanoseconds() / int64(time.Millisecond)),
	})
	if err != nil {
		return err
	}

	errc := make(chan error, 1)

	type Result struct {
		req  deq.Event
		resp *deq.Event
		err  error
	}

	// Wait for background goroutine to cleanup before returning
	var wg sync.WaitGroup
	defer wg.Wait()

	results := make(chan Result, 30)
	defer close(results)

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
					if err != nil && status.Code(err) != codes.Unavailable {
						log.Printf("publish response: %v", err)
					}
					if err != nil {
						select {
						case errc <- err:
						default:
						}
						continue
					}
				}

				if result.err != nil {
					// TODO: post error value back to DEQ.
					log.Printf("handle channel %q topic %q event %q: %v", c.name, c.topic, result.req.ID, result.err)
				}

				ackCode := ack.ErrorCode(result.err)
				if ackCode == ack.NoOp {
					continue
				}

				code := codeToProto(ackCode)
				_, err := c.deqClient.Ack(ctx, &api.AckRequest{
					Channel: c.name,
					Topic:   c.topic,
					EventId: result.req.ID,
					Code:    code,
				})
				if err != nil {
					select {
					case errc <- err:
					default:
					}
					continue
				}
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

		response, err := handler(ctx, event)
		select {
		case results <- Result{event, response, err}:
		case err := <-errc:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *clientChannel) Next(ctx context.Context) (deq.Event, error) {
	stream, err := c.deqClient.Sub(ctx, &api.SubRequest{
		Channel: c.name,
		Topic:   c.topic,
		Follow:  true,
	})
	if err != nil {
		return deq.Event{}, err
	}

	e, err := stream.Recv()
	if err != nil {
		return deq.Event{}, err
	}

	return eventFromProto(e), nil
}

func (c *clientChannel) RequeueEvent(ctx context.Context, e deq.Event, delay time.Duration) error {
	_, err := c.deqClient.Ack(ctx, &api.AckRequest{
		Channel: c.name,
		Topic:   c.topic,
		EventId: e.ID,
		Code:    api.AckCode_REQUEUE,
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

func codeToProto(code ack.Code) api.AckCode {
	switch code {
	case ack.OK:
		return api.AckCode_OK
	case ack.Requeue:
		return api.AckCode_REQUEUE
	case ack.RequeueLinear:
		return api.AckCode_REQUEUE_LINEAR
	case ack.RequeueConstant:
		return api.AckCode_REQUEUE_CONSTANT
	case ack.Invalid:
		return api.AckCode_INVALID
	case ack.Internal:
		return api.AckCode_INTERNAL
	case ack.DequeueError:
		return api.AckCode_DEQUEUE_ERROR
	default:
		return api.AckCode_UNSPECIFIED
	}
}
