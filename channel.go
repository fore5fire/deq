package deq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq/internal/data"
)

// Channel allows multiple listeners to synchronize processing of events
type Channel struct {
	topic    string
	name     string
	shared   *sharedChannel
	idle     bool
	done     chan error
	errMutex sync.Mutex
	err      error
	db       *badger.DB
}

type channelKey struct {
	name  string
	topic string
}

type sharedChannel struct {
	// Mutex protects idleChans and doneChans
	sync.Mutex
	name           string
	topic          string
	missedMutex    sync.Mutex
	missed         bool
	in             chan *Event
	out            chan *Event
	done           chan error
	idleMutex      sync.RWMutex
	idle           bool
	doneChans      []chan error
	stateSubsMutex sync.RWMutex
	// Pass in a response channel, when the event is dequeued the new state will
	// be sent back on the response channel
	stateSubs map[string]map[*EventStateSubscription]struct{}
	db        *badger.DB
}

// Channel returns the channel for a given name
func (s *Store) Channel(name, topic string) *Channel {
	key := channelKey{name, topic}

	s.sharedChannelsMu.Lock()
	shared, ok := s.sharedChannels[key]
	if !ok {
		shared = &sharedChannel{
			name:      name,
			topic:     topic,
			in:        make(chan *Event, 20),
			out:       make(chan *Event, 20),
			stateSubs: make(map[string]map[*EventStateSubscription]struct{}),
			db:        s.db,
		}
		s.sharedChannels[key] = shared

		go shared.start()
	}
	s.sharedChannelsMu.Unlock()

	// DON'T FORGET TO ADD CHECK FOR FAILED CHANNEL

	done := make(chan error, 1)
	shared.Lock()
	defer shared.Unlock()
	// shared.idleChans = append(shared.idleChans, idle)
	shared.doneChans = append(shared.doneChans, done)

	return &Channel{
		name:   name,
		topic:  topic,
		shared: shared,
		done:   done,
		db:     s.db,
	}
}

// Next returns the next event in the queue, or nil if there are no events
func (c *Channel) Next(ctx context.Context) (Event, error) {

	for {
		select {
		case <-ctx.Done():
			return Event{}, ctx.Err()
		case e := <-c.shared.out:
			if e == nil {
				return Event{}, c.Err()
			}
			txn := c.db.NewTransaction(false)
			defer txn.Discard()

			channel, err := getChannelEvent(txn, data.ChannelKey{
				Channel: c.name,
				Topic:   c.topic,
				ID:      e.ID,
			})
			if err != nil {
				return Event{}, err
			}

			if channel.EventState != data.EventState_QUEUED {
				continue
			}

			return *e, nil
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
	// TODO: clean up sharedChannel stuff
}

// Err returns the error that caused this channel to fail, or nil if the channel closed cleanly
func (c *Channel) Err() error {
	c.errMutex.Lock()
	defer c.errMutex.Unlock()
	return c.err
}

// setErr sets this channel's error
func (c *Channel) setErr(err error) {
	c.errMutex.Lock()
	defer c.errMutex.Unlock()
	c.err = err
}

// Get returns the event for an event ID, or ErrNotFound if none is found
func (c *Channel) Get(eventID string) (Event, error) {
	txn := c.db.NewTransaction(false)
	defer txn.Discard()

	e, err := getEvent(txn, c.topic, eventID, c.name)
	if err != nil {
		return Event{}, err
	}

	return *e, nil
}

// ListOpts is the available options for
type ListOpts struct {
	// MinID and MaxID specify inclusive bounds for the returned results.
	MinID, MaxID string
	// Reversed specifies if the listed results are sorted in reverse order.
	Reversed bool
	// Destination is the slice where listed events are copied. If nil, a slice of length 20 is used.
	// No more than len(Destination) events will be copied.
	Destination []Event
}

// List copies events in a specified range of IDs into opts.Destination and returns a slice of the
// copied events.
//
// See ListOpts for details on specific options.
func (c *Channel) List(ctx context.Context, opts ListOpts) ([]Event, error) {

	if opts.Destination == nil {
		opts.Destination = make([]Event, 20)
	}
	if opts.MaxID == "" {
		opts.MaxID = "\xff\xff\xff\xff"
	}

	txn := c.db.NewTransaction(false)
	defer txn.Discard()

	printKeys(txn)

	iterOpts := badger.DefaultIteratorOptions
	iterOpts.Reverse = opts.Reversed
	it := txn.NewIterator(iterOpts)
	defer it.Close()

	prefix, err := data.EventTopicPrefix(c.topic)
	if err != nil {
		return nil, err
	}
	maxKey := append(prefix, opts.MaxID...)
	dst := opts.Destination[:0]
	// TODO: try to consolidate this loop with catchUp function's loop
	for it.Seek(append(prefix, opts.MinID...)); it.ValidForPrefix(prefix); it.Next() {
		fmt.Println(len(dst), len(opts.Destination))
		if len(dst) >= len(opts.Destination) {
			break
		}
		if opts.MaxID != "" && bytes.Compare(maxKey, it.Item().Key()) < 0 {
			break
		}
		item := it.Item()

		var key data.EventKey
		err = data.UnmarshalTo(item.Key(), &key)
		if err != nil {
			log.Printf("parse event key %s: %v", item.Key(), err)
			continue
		}

		channel, err := getChannelEvent(txn, data.ChannelKey{
			Channel: c.name,
			Topic:   key.Topic,
			ID:      key.ID,
		})
		if err != nil {
			log.Printf("get channel event: %v", err)
			return dst, err
		}

		val, err := item.Value()
		if err != nil {
			return dst, fmt.Errorf("get item value: %v", err)
		}

		var e data.EventPayload
		err = proto.Unmarshal(val, &e)
		if err != nil {
			return dst, fmt.Errorf("unmarshal event: %v", err)
		}

		dst = append(dst, Event{
			ID:           key.ID,
			Topic:        key.Topic,
			CreateTime:   key.CreateTime,
			Payload:      e.Payload,
			RequeueCount: int(channel.RequeueCount),
			State:        protoToEventState(channel.EventState),
			DefaultState: protoToEventState(e.DefaultEventState),
		})
	}

	return dst, nil
}

// func (c *Channel) Await(eventID string) (Event, error) {

// }

// SetEventState sets the state of an event for this channel
func (c *Channel) SetEventState(id string, state EventState) error {

	// Retry for up to 10 conflicts
	for i := 0; i < 10; i++ {
		key := data.ChannelKey{
			Topic:   c.topic,
			Channel: c.name,
			ID:      id,
		}

		txn := c.db.NewTransaction(true)
		defer txn.Discard()

		channelEvent, err := getChannelEvent(txn, key)
		if err != nil {
			return err
		}

		channelEvent.EventState = state.toProto()

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
func (c *Channel) RequeueEvent(e Event, delay time.Duration) error {

	requeue := func() error {
		// log.Printf("REQUEUING %s/%s count: %d", e.Topic, e.ID, e.RequeueCount)
		requeueCount, err := c.incrementSavedRequeueCount(&e)
		if err != nil {
			return err
		}
		e.RequeueCount = requeueCount
		c.shared.out <- &e

		return nil
	}

	if delay == 0 {
		return requeue()
	}

	go func() {
		time.Sleep(delay)
		err := requeue()
		if err != nil {
			log.Printf("requeue event: %v", err)
		}
	}()

	return nil
}

func (c *Channel) incrementSavedRequeueCount(e *Event) (int, error) {

	// retry for up to 10 conflicts.
	for i := 0; i < 10; i++ {

		txn := c.db.NewTransaction(true)
		defer txn.Discard()

		key := data.ChannelKey{
			Channel: c.name,
			Topic:   c.topic,
			ID:      e.ID,
		}

		channelEvent, err := getChannelEvent(txn, key)
		if err != nil {
			return -1, err
		}

		channelEvent.RequeueCount++

		err = setChannelEvent(txn, key, channelEvent)
		if err != nil {
			return -1, err
		}

		err = txn.Commit(nil)
		if err == badger.ErrConflict {
			time.Sleep(time.Second / 10)
			continue
		}
		if err != nil {
			return -1, fmt.Errorf("commit channel event: %v", err)
		}

		return int(channelEvent.RequeueCount), nil
	}

	return -1, badger.ErrConflict
}

// EventStateSubscription allows you to get updates when a particular event's state is updated.
type EventStateSubscription struct {
	C <-chan EventState
	c chan EventState

	eventID string
	channel *Channel
	missed  bool
	latest  EventState
}

var (
	// ErrSubscriptionClosed indicates that the operation was interrupted because Close() was called
	// on the EventStateSubscription.
	ErrSubscriptionClosed = errors.New("subscription closed")
)

// NewEventStateSubscription returns a new EventStateSubscription.
//
// EventStateSubscription begins caching updates as soon as it's created, even before
// Next is called. Close should always be called when the EventStateSubscription
// is no longer needed.
// Example usage:
//
//   sub := channel.NewEventStateSubscription(id)
// 	 defer sub.Close()
// 	 for {
// 	   state := sub.Next(ctx)
//   	 if state != EventState_QUEUED {
//       break
//     }
//   }
//
func (c *Channel) NewEventStateSubscription(id string) *EventStateSubscription {

	sub := &EventStateSubscription{
		eventID: id,
		channel: c,
		c:       make(chan EventState, 3),
	}
	sub.C = sub.c

	c.shared.stateSubsMutex.Lock()
	defer c.shared.stateSubsMutex.Unlock()

	subs := c.shared.stateSubs[id]
	if subs == nil {
		subs = make(map[*EventStateSubscription]struct{})
		c.shared.stateSubs[id] = subs
	}
	subs[sub] = struct{}{}

	return sub
}

// Next blocks until an update is recieved, then returns the new state.
//
// It's possible for a subscription to miss updates if its internal buffer is full. In this case,
// it will skip earlier updates while preserving update order, such that the current state is always
// at the end of a full buffer.
func (sub *EventStateSubscription) Next(ctx context.Context) (EventState, error) {
	select {
	case <-ctx.Done():
		return EventStateUnspecified, ctx.Err()
	case state, ok := <-sub.C:
		if !ok {
			return EventStateUnspecified, ErrSubscriptionClosed
		}
		return state, nil
	}
}

// Close closes the EventStateSubscription.
//
// This method should always be called when the EventStateSubscription is no longer needed.
// For example:
//
// 	sub := channel.NewEventStateSubscription(id)
// 	defer sub.Close()
//
func (sub *EventStateSubscription) Close() {
	sub.channel.shared.stateSubsMutex.Lock()
	defer sub.channel.shared.stateSubsMutex.Unlock()

	subs := sub.channel.shared.stateSubs[sub.eventID]
	delete(subs, sub)
	if len(subs) == 0 {
		delete(sub.channel.shared.stateSubs, sub.eventID)
	}

	close(sub.c)
}

// add adds an event to this subscription. It is not safe for concurrent use.
func (sub *EventStateSubscription) add(state EventState) {
	// If our buffer is full, drop the least recent update
	if len(sub.c) == cap(sub.c) {
		<-sub.c
	}
	sub.c <- state
}

func (s *sharedChannel) Idle() bool {
	s.idleMutex.RLock()
	defer s.idleMutex.RUnlock()
	return s.idle
}

func (s *sharedChannel) getMissed() bool {
	s.missedMutex.Lock()
	defer s.missedMutex.Unlock()
	return s.missed
}

func (s *sharedChannel) setMissed(m bool) {
	s.missedMutex.Lock()
	s.missed = m
	s.missedMutex.Unlock()
}

func (s *sharedChannel) start() {

	cursor, err := s.getCursor(s.topic)
	if err != nil {
		s.broadcastErr(err)
		return
	}

	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		s.setMissed(false)

		// Let's drain our events so have room for some that might come in while we catch up.
		// We'll read these off the disk, so it's ok to discard them
		// s.Lock()
		for i := len(s.in); i > 0; i-- {
			<-s.in
		}
		// s.Unlock()

		cursor, err = s.catchUp(cursor)
		if err != nil {
			s.broadcastErr(err)
		}

		// As long as we're up to date...
		for !s.getMissed() {
			// if we're already idle, we don't want idle getting set over and over.
			// by leaving the timer expired, it won't trigger again.
			// if !s.Idle() && len(s.in) == 0 {
			if len(s.in) == 0 {
				// Only show that we're idle if it lasts for more than a short time
				// TODO: we could get this instead by having the store directly track if
				// it's idle or not. then the channel would be idle only if it's not
				// reading from disk and the store is idle.
				// if !timer.Stop() {
				// 	<-timer.C
				// }
				// timer.Reset(time.Second / 32)
				s.idleMutex.Lock()
				s.idle = true
				s.idleMutex.Unlock()
			}

			select {
			// The timer expired, we're idle
			// case <-timer.C:
			// We've got a new event, lets publish it
			case e := <-s.in:
				s.idleMutex.Lock()
				s.idle = false
				s.idleMutex.Unlock()
				// log.Printf("READING FROM MEMORY %s/%s count: %d", e.Topic, e.ID, e.RequeueCount)
				s.out <- e
				cursor, _ = data.EventKey{
					Topic:      e.Topic,
					CreateTime: e.CreateTime,
					ID:         e.ID,
				}.Marshal()
			}
		}

		// We might have missed an event, lets go back to reading from disk.
		// s.Unlock()
	}
}

// catchUp returns nil instead of new prefix when time to quit
func (s *sharedChannel) catchUp(cursor []byte) ([]byte, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 100

	it := txn.NewIterator(opts)
	defer it.Close()

	var lastKey []byte
	prefix, err := data.EventTopicPrefix(s.topic)
	if err != nil {
		return nil, err
	}

	// TODO: is there any way to not read over all events when we get behind without losing requeued
	// events? Currently we ignore the cursor, can we still use it?
	for it.Seek(append(prefix, 0)); it.ValidForPrefix(prefix); it.Next() {

		item := it.Item()
		lastKey = item.KeyCopy(lastKey)

		var key data.EventKey
		err = data.UnmarshalTo(lastKey, &key)
		if err != nil {
			log.Printf("parse event key %s: %v", lastKey, err)
			continue
		}

		channel, err := getChannelEvent(txn, data.ChannelKey{
			Channel: s.name,
			Topic:   key.Topic,
			ID:      key.ID,
		})
		if err != nil {
			log.Printf("get channel event: %v", err)
			continue
		}
		if channel.EventState != data.EventState_QUEUED {
			// Not queued, don't send
			continue
		}

		val, err := item.Value()
		if err != nil {
			return nil, err
		}

		var e data.EventPayload
		err = proto.Unmarshal(val, &e)
		if err != nil {
			log.Printf("unmarshal event: %v", err)
			continue
		}

		// log.Printf("READING FROM DISK %s/%s count: %d", key.Topic, key.ID, channel.RequeueCount)

		s.out <- &Event{
			ID:           key.ID,
			Topic:        key.Topic,
			CreateTime:   key.CreateTime,
			Payload:      e.Payload,
			RequeueCount: int(channel.RequeueCount),
			State:        protoToEventState(channel.EventState),
			DefaultState: protoToEventState(e.DefaultEventState),
		}
	}

	return lastKey, nil
}

func (s *sharedChannel) getCursor(topic string) ([]byte, error) {
	// txn := s.db.NewTransaction(false)
	// defer txn.Discard()
	//
	// opts := badger.DefaultIteratorOptions
	// iter := txn.NewIterator(opts)
	// defer iter.Close()

	prefix, err := data.EventTopicPrefix(s.topic)
	if err != nil {
		return nil, err
	}
	current := prefix
	// for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
	// 	current = iter.Item().Key()
	// 	topic, id, err := parseEventKey(current)
	// 	if err != nil {
	// 		log.Printf("getCursor: parse event key %s: %v - skipping", current, err)
	// 		continue
	// 	}
	// 	_, err = txn.Get(current)
	// 	if err == badger.ErrKeyNotFound {
	//
	// 	}
	// }

	return current, nil
}

func (s *sharedChannel) broadcastEventUpdated(id string, state EventState) {
	s.stateSubsMutex.RLock()
	for sub := range s.stateSubs[id] {
		sub.add(state)
	}
	s.stateSubsMutex.RUnlock()
}

func (s *sharedChannel) broadcastErr(err error) {
	panic(err)
	// for _, donec := range s.doneChans {
	// 	donec <- err
	// }
}
