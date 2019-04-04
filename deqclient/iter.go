package deqclient

import (
	"context"
	"sync"

	"gitlab.com/katcheCode/deq"
	api "gitlab.com/katcheCode/deq/api/v1/deq"
)

type eventIter struct {
	next    chan *api.Event
	current deq.Event
	client  api.DEQClient

	cancel func()
	errMut sync.Mutex
	err    error

	reversed bool
}

func (c *clientChannel) NewEventIter(opts deq.IterOpts) deq.EventIter {

	ctx, cancel := context.WithCancel(context.Background())

	it := &eventIter{
		next:     make(chan *api.Event, opts.PrefetchCount/2),
		cancel:   cancel,
		client:   c.deqClient,
		reversed: opts.Reversed,
	}

	go it.loadEvents(ctx, &api.ListRequest{
		Topic:    c.topic,
		Channel:  c.name,
		MinId:    opts.Min,
		MaxId:    opts.Max,
		Reversed: opts.Reversed,
		PageSize: int32(opts.PrefetchCount/2 + opts.PrefetchCount%2),
	})

	return it
}

func (c *clientChannel) NewIndexIter(opts deq.IterOpts) deq.EventIter {

	if opts.PrefetchCount < 0 {
		panic("opts.PrefetchCount cannot be negative")
	}

	ctx, cancel := context.WithCancel(context.Background())

	it := &eventIter{
		next:   make(chan *api.Event, opts.PrefetchCount/2),
		cancel: cancel,
		client: c.deqClient,
	}

	go it.loadEvents(ctx, &api.ListRequest{
		Topic:    c.topic,
		Channel:  c.name,
		MinId:    opts.Min,
		MaxId:    opts.Max,
		Reversed: opts.Reversed,
		UseIndex: true,
		PageSize: int32((opts.PrefetchCount+1)/2 + (opts.PrefetchCount+1)%2),
	})

	return it
}

func (it *eventIter) Next(ctx context.Context) bool {
	select {
	case e, ok := <-it.next:
		if !ok {
			return false
		}
		it.current = eventFromProto(e)
		return true
	case <-ctx.Done():
		it.setErr(ctx.Err())
		return false
	}
}

func (it *eventIter) Event() deq.Event {
	return it.current
}

func (it *eventIter) Err() error {
	it.errMut.Lock()
	defer it.errMut.Unlock()
	return it.err
}

func (it *eventIter) Close() {
	it.cancel()
}

func (it *eventIter) setErr(err error) {
	it.errMut.Lock()
	defer it.errMut.Unlock()
	it.err = err
}

func (it *eventIter) loadEvents(ctx context.Context, request *api.ListRequest) {

	// Let the reader know when we're done
	defer close(it.next)

	// make a local copy of request so we can modify it freely.
	req := *request

	for req.MinId < req.MaxId {

		list, err := it.client.List(ctx, &req)
		if err != nil {
			it.setErr(err)
			return
		}

		if len(list.Events) == 0 {
			break
		}

		// Set bounds to return results just after the last id of the last page.
		last := list.Events[len(list.Events)-1].Id
		if it.reversed {
			req.MaxId = last
		} else {
			req.MinId = last + "\x01"
		}

		for _, e := range list.Events {
			select {
			case <-ctx.Done():
				it.setErr(ctx.Err())
				return
			case it.next <- e:
			}
		}
	}
}
