package deqdb

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v2"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/ack"
	"gitlab.com/katcheCode/deq/deqdb/internal/data"
	"gitlab.com/katcheCode/deq/deqerr"
	"gitlab.com/katcheCode/deq/deqopt"
)

// Channel allows multiple listeners to coordinate processing of events.
//
// All methods of Channel are safe for concurrent use unless otherwise specified.
type Channel struct {
	topic      string
	name       string
	shared     *sharedChannel
	idle       bool
	done       chan struct{}
	errMutex   sync.Mutex
	err        error
	db         data.DB
	store      *Store
	sharedDone func()

	initialDelay time.Duration
	idleTimeout  time.Duration

	info, debug Logger
}

// Channel returns the channel for a given name
func (s *Store) Channel(name, topic string) *Channel {
	shared, sharedDone := s.listenSharedChannel(name, topic)

	// DON'T FORGET TO ADD CHECK FOR FAILED CHANNEL

	return &Channel{
		name:         name,
		topic:        topic,
		shared:       shared,
		done:         make(chan struct{}),
		db:           s.db,
		store:        s,
		sharedDone:   sharedDone,
		initialDelay: time.Second * 2,

		info:  s.info,
		debug: s.debug,
	}
}

// SetInitialResendDelay sets the initial send delay of an event's first resend on the channel.
// This is used as a base value from which any backoff is applied.
func (c *Channel) SetInitialResendDelay(delay time.Duration) {
	c.initialDelay = delay
}

// SetIdleTimeout sets the duration of consecutive idling needed for subscriptions on the channel
// to be cancelled automatically. Defaults to no timeout.
func (c *Channel) SetIdleTimeout(idleTimeout time.Duration) {
	c.idleTimeout = idleTimeout
}

// Next returns the next event in the queue.
//
// Events returned by next are after the duration returned by requeueDelayFunc has elapsed. If
// requeueDelayFunc is nil, events are requeued immediately. To dequeue an event, see
// store.UpdateEventStatus to dequeue an event.
func (c *Channel) Next(ctx context.Context) (deq.Event, error) {

	for {
		select {
		case <-ctx.Done():
			return deq.Event{}, deqerr.FromContext(ctx)
		case <-c.done:
			return deq.Event{}, ErrChannelClosed
		case e := <-c.shared.out:
			if e == nil {
				return deq.Event{}, c.Err()
			}
			txn := c.db.NewTransaction(false)
			defer txn.Discard()

			// TODO: don't allow deleted events to get sent out.
			channel := data.ChannelPayload{
				EventState: data.EventStateToProto(e.DefaultState),
			}
			err := data.GetChannelEvent(txn, &data.ChannelKey{
				Channel: c.name,
				Topic:   c.topic,
				ID:      e.ID,
			}, &channel)
			if err != nil && err != deq.ErrNotFound {
				return deq.Event{}, deqerr.Errorf(deqerr.Unavailable, "get current event state: %v", err)
			}

			// var sendCount data.SendCount
			// err = data.GetSendCount(txn, &data.SendCountKey{
			// 	Channel: c.name,
			// 	Topic:   c.topic,
			// 	ID:      e.ID,
			// }, &sendCount)
			// if err != nil && err != deq.ErrNotFound {
			// 	return deq.Event{}, fmt.Errorf("get current send count: %v", err)
			// }

			e.SendCount++
			e.State = data.EventStateFromProto(channel.EventState)

			var delayFunc sendDelayFunc
			switch e.State {
			default:
				c.debug.Printf("channel %q: next: skipping non-queued event %q %q", c.name, e.Topic, e.ID)
				// Don't send non-queued events.
				continue
			case deq.StateQueued:
				delayFunc = sendDelayExp
			case deq.StateQueuedLinear:
				delayFunc = sendDelayLinear
			case deq.StateQueuedConstant:
				delayFunc = sendDelayConstant
			}

			max := delayFunc(c.initialDelay, e.SendCount)
			min := delayFunc(c.initialDelay, e.SendCount-1)
			delay := randomSendDelay(min, max)
			c.debug.Printf("channel %q: next: rescheduling event %q %q after %v", c.name, e.Topic, e.ID, delay)

			schedule := time.Now().Add(delay)
			err = c.shared.IncrementSendCount(ctx, e)
			if err != nil {
				return deq.Event{}, err
			}

			c.shared.ScheduleEvent(e, schedule)

			e.Selector = e.ID

			c.debug.Printf("channel %s: next: sending event %q %q", c.name, e.Topic, e.ID)
			return *e, nil
		}
	}
}

// Sub subscribes to this channel's event queue, calling handler for each event received. If c has
// multiple accessors of it's event queue, only one will receive each event per requeue.
//
// Sub blocks until an error occurs or the context is done. If Sub returns, it always returns an
// error.
//
// The Event returned by handler is published if non-nil, and ack.Code is processed according to the
// rules specified in the gitlab.com/katcheCode/deq/ack package. Sub only handles one event at a
// time. To handle multiple events concurrently subscribe with the same handler on multiple
// goroutines. For example:
//
//   errc := make(chan error, 1)
//   for i := 0; i < workerCount; i++ {
//     go run() {
//       select {
//       case errc <- channel.Sub(ctx, handler):
//       default: // Don't block
//       }
//     }()
//   }
//   err := <-errc
//   ...
func (c *Channel) Sub(ctx context.Context, handler deq.SubHandler) error {

	errc := make(chan error, 1)

	type Result struct {
		req  deq.Event
		resp *deq.Event
		err  error
	}

	// Wait for background goroutines to cleanup before returning
	var wg sync.WaitGroup
	defer wg.Wait()

	results := make(chan Result, 30)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// workers handle results without blocking processing of next event
	const numWorkers = 3
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			for {
				err := func() error {
					select {
					case <-c.done:
						return ErrChannelClosed
					case <-ctx.Done():
						return ctx.Err()
					case result := <-results:

						ackCode := ack.ErrorCode(result.err)
						reachedLimit := result.req.SendCount >= sendLimit

						if result.resp != nil {
							_, err := c.store.Pub(ctx, *result.resp)
							if err != nil {
								// Log the error and compensating action
								action := "will retry"
								if reachedLimit {
									action = "send limit exceeded, not retrying"
								}
								log.Printf("publish response %q %q to %q %q on channel %q: %v - %s", result.resp.Topic, result.resp.ID, result.req.Topic, result.req.ID, c.name, err, action)
								// Make sure the event is queued unless it has reached its resend limit.
								ackCode = ack.Requeue
							}
						}

						if result.err != nil && ackCode != ack.NoOp {
							// TODO: post error value back to DEQ.
							c.info.Printf("handle channel %q topic %q event %q: %v", c.name, c.topic, result.req.ID, result.err)
						}

						// TODO: figure out a better way to avoid requeue looping (maybe requeue differentiate on the server between requeuing and resetting the send count? Or allow raising the requeue limit per event?)
						// If the send limit has been reached, don't requeue it.
						if reachedLimit && (ackCode == ack.Requeue || ackCode == ack.RequeueLinear || ackCode == ack.RequeueConstant) {
							return nil
						}

						var state deq.State
						switch ackCode {
						case ack.DequeueOK:
							state = deq.StateOK
						case ack.Invalid:
							state = deq.StateInvalid
						case ack.Internal:
							state = deq.StateInternal
						case ack.DequeueError:
							state = deq.StateDequeuedError
						case ack.RequeueConstant:
							state = deq.StateQueuedConstant
						case ack.RequeueLinear:
							state = deq.StateQueuedLinear
						case ack.Requeue:
							state = deq.StateQueued
						case ack.NoOp:
							return nil
						default:
							return deqerr.Errorf(deqerr.Invalid, "handler returned unrecognized ack.Code")
						}

						err := c.SetEventState(ctx, result.req.ID, state)
						if err != nil {
							return deqerr.Errorf(deqerr.GetCode(err), "set event state: %v", err)
						}

						return nil
					}
				}()
				if err != nil {
					select {
					case errc <- err:
					default:
						// Don't block - if no one's listening, they've already returned because of another
						// error
						return
					}
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(results)
		nextCtx, nextCancel := ctx, func() {}
		for {
			err := func() error {
				// If we're using idle timeout, setup a separate context so we can periodically check on the idle
				// state.
				if c.idleTimeout > 0 {
					nextCtx, nextCancel = context.WithTimeout(ctx, c.idleTimeout)
					defer nextCancel()
				}

				e, err := c.Next(nextCtx)
				ctxErr := deqerr.Unwrap(err)
				if err != nil && ctxErr == ctx.Err() {
					// Original context was cancelled.
					return deqerr.FromContext(ctx)
				}
				if err != nil && ctxErr == nextCtx.Err() {
					// The idle timeout triggered, so if the channel is idle (not just slow) then we're done.
					if c.Idle() {
						return io.EOF
					}
					// We're not idle, so try again.
					return nil
				}
				if err != nil {
					// Some non-context error occurred.
					return err
				}

				// Invoke the handler, then pass on the result.
				response, err := handler(ctx, e)
				select {
				case results <- Result{e, response, err}:
					return nil
				case <-ctx.Done():
					return deqerr.FromContext(ctx)
				}
			}()
			if err != nil {
				// EOF means clean shutdown, return a nil to the caller.
				if err == io.EOF {
					err = nil
				}
				select {
				case errc <- err:
				default:
				}
				break
			}
		}
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return deqerr.FromContext(ctx)
	case <-c.done:
		return ErrChannelClosed
	}
}

// func (c *sharedChannel) enqueue(*Event) {
// 	requeue := make(chan *pb.Event, 20)
// 	requeueNow := make(chan struct{}, 1)
// 	defer close(requeue)
//
// 	go func() {
// 		timer := time.NewTimer(time.Hour)
//
// 		for e := range requeue {
// 			if !timer.Stop() {
// 				<-timer.C
// 			}
// 			timer.Reset(requeueDelay)
// 			select {
// 			case <-timer.C:
// 				if channel.Get(e.ID)
// 				channel.RequeueEvent(e)
// 			case <-requeueNow:
// 				channel.RequeueEvent(e)
// 			}
// 		}
//
// 		timer.Stop()
// 	}()
// }

// Idle returns the channel's current idle state.
func (c *Channel) Idle() bool {
	return c.shared.Idle()
}

// Close cleans up resources for this Channel
func (c *Channel) Close() {
	close(c.done)
	c.sharedDone()
}

// Err returns the error that caused this channel to fail, or nil if the channel closed cleanly
func (c *Channel) Err() error {
	c.errMutex.Lock()
	defer c.errMutex.Unlock()
	return c.err
}

// Get returns the event for an event ID, or ErrNotFound if none is found
func (c *Channel) Get(ctx context.Context, event string, options ...deqopt.GetOption) (deq.Event, error) {

	opts := deqopt.NewGetOptionSet(options)

	// Determine whether to get by ID or index.
	get := c.get
	if opts.UseIndex {
		get = c.getIndex
	}

	txn := c.db.NewTransaction(false)
	defer txn.Discard()

	// If we aren't awaiting, just return the result.
	if !opts.Await {
		e, err := get(txn, event)
		if err != nil {
			return deq.Event{}, err
		}
		return *e, nil
	}

	if opts.UseIndex {
		return deq.Event{}, errors.New("options Await() and UseIndex() cannot both be used")
	}

	// Setup the subscription before we try to get, so we don't miss an event just after getting.
	sub := c.NewEventStateSubscription(event)

	e, err := get(txn, event)
	if err == nil {
		// event already exists, no need to wait.
		return *e, nil
	}
	if err != deq.ErrNotFound {
		return deq.Event{}, err
	}

	// Discard the original transaction, we'll need a new one when we try to get again.
	txn.Discard()

	// We got ErrNotFound, so let's wait until the event is published.
	_, err = sub.Next(ctx)
	if err != nil {
		return deq.Event{}, err
	}

	txn = c.db.NewTransaction(false)
	defer txn.Discard()

	e, err = get(txn, event)
	if err != nil {
		return deq.Event{}, deqerr.Errorf(deqerr.Internal, "retry get after await: %v", err)
	}
	return *e, nil
}

func (c *Channel) get(txn data.Txn, id string) (*deq.Event, error) {
	e, err := data.GetEvent(txn, c.topic, id, c.name)
	if err == deq.ErrNotFound {
		return nil, deq.ErrNotFound
	}
	if err != nil {
		return nil, deqerr.Wrap(deqerr.Unavailable, err)
	}

	e.Selector = id

	return e, nil
}

// GetIndex returns the event for an event's index, or ErrNotFound if none is found
func (c *Channel) getIndex(txn data.Txn, index string) (*deq.Event, error) {

	key := data.IndexKey{
		Topic: c.topic,
		Value: index,
	}

	c.debug.Printf("channel %q topic %q: getIndex %q: getting payload for key %+v", key, c.topic, index, key)

	var payload data.IndexPayload
	err := data.GetIndexPayload(txn, &key, &payload)
	if err == deq.ErrNotFound {
		return nil, deq.ErrNotFound
	}
	if err != nil {
		return nil, deqerr.Wrap(deqerr.Unavailable, err)
	}

	c.debug.Printf("channel %q topic %q: getIndex %q: got IndexPayload %+v", c.name, c.topic, index, payload)

	e, err := data.GetEvent(txn, c.topic, payload.EventId, c.name)
	if err == deq.ErrNotFound {
		return nil, deq.ErrNotFound
	}
	if err != nil {
		return nil, deqerr.Wrap(deqerr.Unavailable, err)
	}

	e.Selector = index
	e.SelectorVersion = payload.Version
	return e, nil
}

// BatchGet gets multiple events by ID, returned as a map of ID to event.
//
// ErrNotFound is returned if any event in ids is not found.
func (c *Channel) BatchGet(ctx context.Context, events []string, options ...deqopt.BatchGetOption) (map[string]deq.Event, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	opts := deqopt.NewBatchGetOptionSet(options)

	if opts.Await {
		return nil, deqerr.New(deqerr.Invalid, "BatchGet with option Await() is not yet implemented")
	}

	// Determine whether to get by ID or index.
	get := c.get
	if opts.UseIndex {
		get = c.getIndex
	}

	// Deduplicate requested IDs.
	deduped := make(map[string]struct{}, len(events))
	for _, event := range events {
		deduped[event] = struct{}{}
	}

	// Calculate the number of workers we want running.
	workerCount := 8
	if workerCount > len(deduped) {
		workerCount = len(deduped)
	}

	// Setup channels.
	type Response struct {
		Event    *deq.Event
		Err      error
		Selector string
	}

	requests := make(chan string, len(deduped))
	responses := make(chan Response, len(deduped))

	// Kick off workers.
	for i := 0; i < workerCount; i++ {
		go func() {
			txn := c.db.NewTransaction(false)
			defer txn.Discard()

			for selector := range requests {
				e, err := get(txn, selector)
				if err != nil {
					responses <- Response{Err: err, Selector: selector}
					return
				}

				responses <- Response{Event: e, Selector: selector}
			}
		}()
	}

	// Send requests to workers.
	for selector := range deduped {
		requests <- selector
	}
	close(requests)

	// Read worker responses.
	result := make(map[string]deq.Event, len(deduped))
	for range deduped {
		select {
		case resp := <-responses:
			if resp.Err == deq.ErrNotFound && opts.AllowNotFound {
				continue
			}
			if resp.Err == deq.ErrNotFound {
				return nil, deq.ErrNotFound
			}
			if resp.Err != nil {
				return nil, resp.Err
			}
			result[resp.Selector] = *resp.Event
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return result, nil
}

// SetEventState sets the state of an event for this channel.
func (c *Channel) SetEventState(ctx context.Context, id string, state deq.State) error {

	// Retry for up to 10 conflicts
	for i := 0; i < 10; i++ {
		key := data.ChannelKey{
			Topic:   c.topic,
			Channel: c.name,
			ID:      id,
		}

		txn := c.db.NewTransaction(true)
		defer txn.Discard()

		e, err := data.GetEvent(txn, c.topic, id, c.name)
		if err == deq.ErrNotFound {
			return deq.ErrNotFound
		}
		if err != nil {
			return deqerr.Errorf(deqerr.Unavailable, "lookup event: %v", err)
		}

		wasQueued := e.State == deq.StateQueued ||
			e.State == deq.StateQueuedLinear ||
			e.State == deq.StateQueuedConstant
		nowQueued := state == deq.StateQueued ||
			state == deq.StateQueuedLinear ||
			state == deq.StateQueuedConstant

		channelEvent := data.ChannelPayload{
			EventState: data.EventStateToProto(state),
		}
		err = data.SetChannelEvent(txn, &key, &channelEvent)
		if err != nil {
			return deqerr.Wrap(deqerr.Unavailable, err)
		}

		err = txn.Commit()
		if err == badger.ErrConflict {
			time.Sleep(time.Second / 10)
			continue
		}
		if err != nil {
			return errFromBadger(err)
		}

		c.shared.broadcastEventUpdated(id, state)

		// If the event is just entering the queue, make sure it gets scheduled immediately.
		// TODO: how do we want to handle send count with requeued events? Should we reset it, keep it
		// and send immediately once, or keep it and apply the backoff immediately? Currently we are
		// using option 2, but not much thought went into it.
		if !wasQueued && nowQueued {
			c.shared.in <- e
			c.debug.Printf("event state updated: event %q %q scheduled on channel %q", e.Topic, e.ID, c.name)
		}

		return nil
	}

	return badger.ErrConflict
}

// // EventStatus is the processing state of an event on a particular channel
// type EventStatus int
//
// // An event of type EventStatusPending will cause deqd to requeue the event after waiting the channel's event_timeout_miliseconds setting.
// // EventStatusProcessed and EventStatusWillNotProcess have the same behavior for now.
// const (
// 	EventStatusPending EventStatus = iota
// 	EventStatusProcessed
// 	EventStatusWillNotProcess
// )

var (
	// ErrChannelClosed is returned when calling operations on a closed channel, or if an operation
	// was ended prematurely because the channel closed.
	ErrChannelClosed = deqerr.New(deqerr.Invalid, "channel closed")
)
