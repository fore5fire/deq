package eventstore

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq/api/v1/deq"
	"gitlab.com/katcheCode/deq/pkg/eventstore/data"
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
	name            string
	topic           string
	missedMutex     sync.Mutex
	missed          bool
	in              chan *deq.Event
	out             chan *deq.Event
	done            chan error
	idleMutex       sync.RWMutex
	idle            bool
	doneChans       []chan error
	stateChansMutex sync.Mutex
	// Pass in a response channel, when the event is dequeued the new state will
	// be sent back on the response channel
	stateChans map[string]chan struct{}
	db         *badger.DB
}

// Channel returns the channel for a given name
func (s *Store) Channel(name, topic string) *Channel {
	key := channelKey{name, topic}

	s.sharedChannelsMu.Lock()
	shared, ok := s.sharedChannels[key]
	if !ok {
		shared = &sharedChannel{
			name:       name,
			topic:      topic,
			in:         make(chan *deq.Event, 20),
			out:        make(chan *deq.Event, 20),
			stateChans: make(map[string]chan struct{}),
			db:         s.db,
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

			channel, err := getChannelEvent(txn, data.ChannelKey{
				Channel: c.name,
				Topic:   c.topic,
				ID:      e.Id,
			})
			if err != nil {
				return deq.Event{}, err
			}

			if channel.EventState != deq.EventState_QUEUED {
				continue
			}

			return *e, nil
		}
	}
}

// func (c *sharedChannel) enqueue(*deq.Event) {
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
// 				if channel.Get(e.Id)
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
func (c *Channel) Get(eventID string) (deq.Event, error) {
	txn := c.db.NewTransaction(false)
	defer txn.Discard()

	e, err := getEvent(txn, c.topic, eventID, c.name)
	if err != nil {
		return deq.Event{}, err
	}

	return *e, nil
}

// func (c *Channel) Await(eventID string) (deq.Event, error) {

// }

// SetEventState sets the state of an event for this channel
func (c *Channel) SetEventState(id string, state deq.EventState) error {

	txn := c.db.NewTransaction(true)
	defer txn.Discard()

	key := data.ChannelKey{
		Topic:   c.topic,
		Channel: c.name,
		ID:      id,
	}

	channelEvent, err := getChannelEvent(txn, key)
	if err != nil {
		return err
	}

	channelEvent.EventState = state

	err = setChannelEvent(txn, key, channelEvent)
	if err != nil {
		return err
	}

	err = txn.Commit(nil)
	if err != nil {
		return err
	}

	c.shared.broadcastEventUpdated(id)

	return nil
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
func (c *Channel) RequeueEvent(e deq.Event, delay time.Duration) error {

	requeue := func() error {
		requeueCount, err := c.incrementSavedRequeueCount(&e)
		if err != nil {
			return err
		}
		e.RequeueCount = int32(requeueCount)
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

func (c *Channel) incrementSavedRequeueCount(e *deq.Event) (int, error) {

	// retry for up to 10 conflicts.
	for i := 0; i < 10; i++ {

		txn := c.db.NewTransaction(true)
		defer txn.Discard()

		key := data.ChannelKey{
			Channel: c.name,
			Topic:   c.topic,
			ID:      e.Id,
		}

		channelEvent, err := getChannelEvent(txn, key)
		if err != nil {
			return 0, err
		}

		channelEvent.RequeueCount++

		err = setChannelEvent(txn, key, channelEvent)
		if err != nil {
			return 0, err
		}

		err = txn.Commit(nil)
		if err == badger.ErrConflict {
			time.Sleep(time.Second / 10)
			// continue
		}
		if err != nil {
			return 0, nil
		}

		return int(channelEvent.RequeueCount), nil
	}

	return 0, badger.ErrConflict
}

// AwaitEventStateChange blocks until the event is dequeued, then returns it's state.
func (c *Channel) AwaitEventStateChange(ctx context.Context, id string) chan deq.EventState {

	result := make(chan deq.EventState, 1)
	c.shared.stateChansMutex.Lock()
	defer c.shared.stateChansMutex.Unlock()
	await := c.shared.stateChans[id]
	if await == nil {
		await = make(chan struct{})
		c.shared.stateChans[id] = await
	}

	go func() {
		// Wait for state change (context done)
		select {
		case <-ctx.Done():
			c.setErr(ctx.Err())
			close(result)
		case <-await:
			e, err := c.Get(id)
			if err != nil {
				c.setErr(err)
				close(result)
				return
			}
			result <- e.State
		}
	}()

	return result
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
		// Let's drain our events so have room for some that might come in while we catch up.
		// We'll read these off the disk, so it's ok to discard them
		// s.Lock()
		for i := len(s.in); i > 0; i-- {
			<-s.in
		}
		// s.Unlock()

		s.setMissed(false)

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
				s.out <- e
				cursor, _ = data.EventKey{
					Topic:      e.Topic,
					CreateTime: time.Unix(0, e.CreateTime),
					ID:         e.Id,
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
	prefix, err := data.EventPrefix(s.topic)
	if err != nil {
		return nil, err
	}

	for it.Seek(append(cursor, 0)); it.ValidForPrefix(prefix); it.Next() {

		item := it.Item()
		lastKey = item.KeyCopy(lastKey)

		state := deq.EventState_QUEUED
		var key data.EventKey
		err = data.UnmarshalTo(lastKey, &key)
		if err != nil {
			log.Printf("parse event key %s: %v", lastKey, err)
			continue
		}

		channelKey, err := data.ChannelKey{
			Channel: s.name,
			Topic:   key.Topic,
			ID:      key.ID,
		}.Marshal()
		if err != nil {
			log.Printf("marshal channel key: %v", err)
			continue
		}
		channelItem, err := txn.Get(channelKey)
		if err != nil && err != badger.ErrKeyNotFound {
			log.Printf("get event %s channel status: %v", key.ID, err)
			continue
		}
		if err == nil {
			buf, err := channelItem.Value()
			if err != nil {
				log.Printf("get event %s channel status: %v", key.ID, err)
				continue
			}

			var channel data.ChannelPayload
			err = proto.Unmarshal(buf, &channel)
			if err != nil {
				log.Printf("get event %s channel status: %v", key.ID, err)
				continue
			}
			if channel.EventState != deq.EventState_QUEUED {
				// Not queued, don't send
				continue
			}

			state = channel.EventState
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

		s.out <- &deq.Event{
			Id:           key.ID,
			Topic:        key.Topic,
			CreateTime:   key.CreateTime.UnixNano(),
			Payload:      e.Payload,
			State:        state,
			DefaultState: e.DefaultEventState,
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

	prefix, err := data.EventPrefix(s.topic)
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

func (s *sharedChannel) broadcastEventUpdated(id string) {
	s.stateChansMutex.Lock()
	stateChans := s.stateChans[id]
	if stateChans != nil {
		close(stateChans)
		s.stateChans[id] = nil
	}
	s.stateChansMutex.Unlock()
}

func (s *sharedChannel) broadcastErr(err error) {
	panic(err)
	// for _, donec := range s.doneChans {
	// 	donec <- err
	// }
}
