package deqclient

import (
	"context"
	"errors"
	"sync"

	"gitlab.com/katcheCode/deq"
	api "gitlab.com/katcheCode/deq/api/v1/deq"
)

type selectedEvent struct {
	Event    *api.Event
	Selector string
}

type eventIter struct {
	next    chan selectedEvent
	current deq.Event
	client  api.DEQClient

	cancel func()
	errMut sync.Mutex
	err    error

	reversed bool
	selector string
}

func (c *clientChannel) NewEventIter(opts *deq.IterOptions) deq.EventIter {

	if opts == nil {
		opts = &deq.IterOptions{}
	}

	var prefetchCount int
	switch {
	case opts.PrefetchCount < -1:
		panic("opts.PrefetchCount cannot be less than -1")
	case opts.PrefetchCount == -1:
		prefetchCount = 0
	case opts.PrefetchCount == 0:
		prefetchCount = 20
	default:
		prefetchCount = opts.PrefetchCount
	}

	max := "\xff\xff\xff\xff"
	if opts.Max != "" {
		max = opts.Max
	}

	ctx, cancel := context.WithCancel(context.Background())

	it := &eventIter{
		next:     make(chan selectedEvent, prefetchCount/2),
		cancel:   cancel,
		client:   c.deqClient,
		reversed: opts.Reversed,
		err:      errors.New("iteration not started"),
	}

	go it.loadEvents(ctx, &api.ListRequest{
		Topic:    c.topic,
		Channel:  c.name,
		MinId:    opts.Min,
		MaxId:    max,
		Reversed: opts.Reversed,
		PageSize: int32(prefetchCount/2+prefetchCount%2) + 1,
	})

	return it
}

func (c *clientChannel) NewIndexIter(opts *deq.IterOptions) deq.EventIter {

	if opts == nil {
		opts = &deq.IterOptions{}
	}

	var prefetchCount int
	switch {
	case opts.PrefetchCount < -1:
		panic("opts.PrefetchCount cannot be less than -1")
	case opts.PrefetchCount == -1:
		prefetchCount = 0
	case opts.PrefetchCount == 0:
		prefetchCount = 5
	default:
		prefetchCount = opts.PrefetchCount
	}

	max := "\xff\xff\xff\xff"
	if opts.Max != "" {
		max = opts.Max
	}

	ctx, cancel := context.WithCancel(context.Background())

	it := &eventIter{
		next:     make(chan selectedEvent, (prefetchCount)/2),
		cancel:   cancel,
		client:   c.deqClient,
		reversed: opts.Reversed,
		err:      errors.New("iteration not started"),
	}

	go it.loadEvents(ctx, &api.ListRequest{
		Topic:    c.topic,
		Channel:  c.name,
		MinId:    opts.Min,
		MaxId:    max,
		Reversed: opts.Reversed,
		UseIndex: true,
		PageSize: int32(prefetchCount/2+prefetchCount%2) + 1,
	})

	return it
}

func (it *eventIter) Next(ctx context.Context) bool {
	it.err = nil

	select {
	case next, ok := <-it.next:
		if !ok {
			return false
		}
		it.current = eventFromProto(next.Event)
		it.selector = next.Selector
		return true
	case <-ctx.Done():
		it.setErr(ctx.Err())
		return false
	}
}

func (it *eventIter) Event() deq.Event {
	if it.err != nil {
		panic("Event() is only valid when Err() returns nil")
	}
	return it.current
}

func (it *eventIter) Selector() string {
	if it.err != nil {
		panic("Selector() is only valid when Err() returns nil")
	}
	return it.selector
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
		lastEvent := list.Events[len(list.Events)-1]
		last := lastEvent.Id
		if req.UseIndex {
			last = lastEvent.Indexes[lastEvent.SelectedIndex]
		}
		if it.reversed {
			req.MaxId = last
		} else {
			req.MinId = last + "\x01"
		}

		for _, e := range list.Events {

			var selector string
			if !req.UseIndex {
				selector = e.Id
			} else if e.SelectedIndex != -1 {
				selector = e.Indexes[e.SelectedIndex]
			}

			select {
			case <-ctx.Done():
				it.setErr(ctx.Err())
				return
			case it.next <- selectedEvent{
				Event:    e,
				Selector: selector,
			}:
			}
		}
	}
}
