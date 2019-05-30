package deqdb

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/ack"
	"gitlab.com/katcheCode/deq/internal/data"
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
	db         *badger.DB
	store      *Store
	sharedDone func()

	backoffFunc BackoffFunc
}

// Channel returns the channel for a given name
func (s *Store) Channel(name, topic string) *Channel {
	shared, sharedDone := s.listenSharedChannel(name, topic)

	// DON'T FORGET TO ADD CHECK FOR FAILED CHANNEL

	return &Channel{
		name:        name,
		topic:       topic,
		shared:      shared,
		done:        make(chan struct{}),
		db:          s.db,
		backoffFunc: ExponentialBackoff(time.Second),
		store:       s,
		sharedDone:  sharedDone,
	}
}

// BackoffFunc sets the function that determines the requeue delay for each event removed from c's
// queue.
//
// The BackoffFunc for a channel defaults to an exponential backoff starting at one second,
// equivelant to calling:
//   c.BackoffFunc(deq.ExponentialBackoff(time.Second))
//
// BackoffFunc is not safe for concurrent use with any of c's methods.
func (c *Channel) BackoffFunc(backoffFunc BackoffFunc) {
	c.backoffFunc = backoffFunc
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
			return deq.Event{}, ctx.Err()
		case e := <-c.shared.out:
			if e == nil {
				return deq.Event{}, c.Err()
			}
			txn := c.db.NewTransaction(false)
			defer txn.Discard()

			// TODO: don't allow deleted events to get sent out.
			channel, err := getChannelEvent(txn, data.ChannelKey{
				Channel: c.name,
				Topic:   c.topic,
				ID:      e.ID,
			}, eventStateToProto(e.DefaultState))
			if err != nil {
				return deq.Event{}, err
			}

			if channel.EventState != data.EventState_QUEUED {
				continue
			}

			delay := c.backoffFunc(*e)
			err = c.RequeueEvent(ctx, *e, delay)
			if err != nil {
				return deq.Event{}, err
			}

			return *e, nil
		}
	}
}

// Sub subscribes to this channel's event queue, calling handler for each event recieved. If c has
// multiple accessor's of it's event queue, only one will recieve each event per requeue.
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
	defer close(results)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// workers handle results without blocking processing of next event
	const numWorkers = 3
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			defer cancel()

			for result := range results {

				if result.resp != nil {
					_, err := c.store.Pub(ctx, *result.resp)
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

				code := ack.ErrorCode(result.err)
				var err error
				switch code {
				case ack.DequeueOK:
					err = c.SetEventState(ctx, result.req.ID, deq.StateOK)
					if err != nil {
						err = fmt.Errorf("set event state: %v", err)
					}
				case ack.Invalid:
					err = c.SetEventState(ctx, result.req.ID, deq.StateInvalid)
					if err != nil {
						err = fmt.Errorf("set event state: %v", err)
					}
				case ack.Internal:
					err = c.SetEventState(ctx, result.req.ID, deq.StateInternal)
					if err != nil {
						err = fmt.Errorf("set event state: %v", err)
					}
				case ack.DequeueError:
					err = c.SetEventState(ctx, result.req.ID, deq.StateDequeuedError)
					if err != nil {
						err = fmt.Errorf("set event state: %v", err)
					}
				case ack.RequeueConstant:
					err = c.RequeueEvent(ctx, result.req, time.Second)
					if err != nil {
						err = fmt.Errorf("requeue event: %v", err)
					}
				case ack.RequeueLinear:
					err = c.RequeueEvent(ctx, result.req, LinearBackoff(time.Second)(result.req))
					if err != nil {
						err = fmt.Errorf("requeue event: %v", err)
					}
				case ack.RequeueExponential:
					err = c.RequeueEvent(ctx, result.req, ExponentialBackoff(time.Second)(result.req))
					if err != nil {
						err = fmt.Errorf("requeue event: %v", err)
					}
				default:
					err = fmt.Errorf("handler returned unrecognized ack.Code")
				}
				if err != nil {
					select {
					case errc <- err:
					default:
					}
				}
			}
		}()
	}

	for {
		e, err := c.Next(ctx)
		if err != nil {
			return err
		}

		response, err := handler(ctx, e)
		select {
		case results <- Result{e, response, err}:
		case err := <-errc:
			return err
		}
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
func (c *Channel) Get(ctx context.Context, eventID string) (deq.Event, error) {
	txn := c.db.NewTransaction(false)
	defer txn.Discard()

	e, err := getEvent(txn, c.topic, eventID, c.name)
	if err != nil {
		return deq.Event{}, err
	}

	return *e, nil
}

// GetIndex returns the event for an event's index, or ErrNotFound if none is found
func (c *Channel) GetIndex(ctx context.Context, index string) (deq.Event, error) {
	txn := c.db.NewTransaction(false)
	defer txn.Discard()

	var payload data.IndexPayload
	err := getIndexPayload(txn, data.IndexKey{
		Topic: c.topic,
		Value: index,
	}, &payload)
	if err != nil {
		return deq.Event{}, err
	}

	e, err := getEvent(txn, c.topic, payload.EventId, c.name)
	if err != nil {
		return deq.Event{}, err
	}

	return *e, nil
}

// BatchGet gets multiple events by ID, returned as a map of ID to event.
//
// ErrNotFound is returned if any event in ids is not found.
func (c *Channel) BatchGet(ctx context.Context, ids []string) (map[string]deq.Event, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Deduplicate requested IDs.
	deduped := make(map[string]struct{}, len(ids))
	for _, index := range ids {
		deduped[index] = struct{}{}
	}

	// Calculate the number of workers we want running.
	workerCount := 8
	if workerCount > len(deduped) {
		workerCount = len(deduped)
	}

	// Setup channels.
	type Response struct {
		Event *deq.Event
		Err   error
	}

	requests := make(chan string, len(deduped))
	responses := make(chan Response, len(deduped))

	// Kick off workers.
	for i := 0; i < workerCount; i++ {
		go func() {
			txn := c.db.NewTransaction(false)
			defer txn.Discard()

			for id := range requests {
				e, err := getEvent(txn, c.topic, id, c.name)
				if err != nil {
					responses <- Response{Err: err}
					return
				}

				responses <- Response{Event: e}
			}
		}()
	}

	// Send requests to workers.
	for id := range deduped {
		requests <- id
	}
	close(requests)

	// Read worker responses.
	result := make(map[string]deq.Event, len(deduped))
	for range deduped {
		select {
		case resp := <-responses:
			if resp.Err != nil {
				return nil, resp.Err
			}
			result[resp.Event.ID] = *resp.Event
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return result, nil
}

// BatchGetIndex gets multiple events by index, returned as a map of index to event.
//
// ErrNotFound is returned if any event in indexes is not found.
func (c *Channel) BatchGetIndex(ctx context.Context, indexes []string) (map[string]deq.Event, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Deduplicate requested indexes.
	deduped := make(map[string]struct{}, len(indexes))
	for _, index := range indexes {
		deduped[index] = struct{}{}
	}

	// Calculate the number of workers we want running.
	workerCount := 8
	if workerCount > len(deduped) {
		workerCount = len(deduped)
	}

	// Setup channels.
	type Response struct {
		Event *deq.Event
		Err   error
		Index string
	}

	requests := make(chan data.IndexKey, len(deduped))
	responses := make(chan Response, len(deduped))

	// Kick off workers.
	for i := 0; i < workerCount; i++ {
		go func() {
			txn := c.db.NewTransaction(false)
			defer txn.Discard()

			for index := range requests {
				var payload data.IndexPayload
				err := getIndexPayload(txn, index, &payload)
				if err != nil {
					responses <- Response{Err: err}
					return
				}

				e, err := getEvent(txn, c.topic, payload.EventId, c.name)
				if err != nil {
					responses <- Response{Err: err}
					return
				}

				responses <- Response{
					Event: e,
					Index: index.Value,
				}
			}
		}()
	}

	// Send requests to workers.
	for index := range deduped {
		requests <- data.IndexKey{
			Topic: c.topic,
			Value: index,
		}
	}
	close(requests)

	// Read worker responses.
	result := make(map[string]deq.Event, len(deduped))
	for range deduped {
		select {
		case resp := <-responses:
			if resp.Err != nil {
				return nil, resp.Err
			}
			result[resp.Index] = *resp.Event
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return result, nil
}

// Await gets an event for the requested event id, waiting for the event to be created if it does
// not already exist.
//
// Await never returns ErrNotFound. If the context expires, the context error is returned
// unmodified.
func (c *Channel) Await(ctx context.Context, eventID string) (deq.Event, error) {

	// start subscription before the read so we won't miss the notification
	sub := c.NewEventStateSubscription(eventID)
	defer sub.Close()

	e, err := c.Get(ctx, eventID)
	if err != nil && err != deq.ErrNotFound {
		return deq.Event{}, err
	}
	if err == nil {
		// event already exists, no need to wait.
		return e, nil
	}

	_, err = sub.Next(ctx)
	if err != nil {
		return deq.Event{}, err
	}

	e, err = c.Get(ctx, eventID)
	if err != nil {
		return deq.Event{}, fmt.Errorf("retry get after await: %v", err)
	}

	return e, nil
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

		channelEvent, err := getChannelEvent(txn, key, data.EventState_UNSPECIFIED_STATE)
		if err != nil {
			return err
		}

		channelEvent.EventState = eventStateToProto(state)

		err = setChannelEvent(txn, key, channelEvent)
		if err != nil {
			return err
		}

		err = txn.Commit(nil)
		if err == badger.ErrConflict {
			time.Sleep(time.Second / 10)
			continue
		}
		if err != nil {
			return err
		}

		c.shared.broadcastEventUpdated(id, state)

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

// RequeueEvent adds the event back into the event queue for this channel
func (c *Channel) RequeueEvent(ctx context.Context, e deq.Event, delay time.Duration) error {
	return c.shared.RequeueEvent(e, delay)
}
