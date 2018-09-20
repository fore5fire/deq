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
	topic  string
	name   string
	shared *sharedChannel
	idle   bool
	done   chan error
	err    error
	db     *badger.DB
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
	stateChans map[string]chan chan deq.EventState
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
			stateChans: make(map[string]chan chan deq.EventState),
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

// Follow returns the next event in the queue, or nil if there are no events
func (c *Channel) Follow() chan *deq.Event {
	return c.shared.out
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
	return c.err
}

// Get returns the event for an event ID, or ErrNotFound if none is found
func (c *Channel) Get(eventID string) (*deq.Event, error) {
	txn := c.db.NewTransaction(false)
	defer txn.Discard()

	return getEvent(txn, c.topic, eventID, c.name)
}

// SetEventState sets the state of an event for this channel
func (c *Channel) SetEventState(id string, state deq.EventState) error {

	txn := c.db.NewTransaction(true)
	defer txn.Discard()

	key := data.ChannelKey{
		Topic:   c.topic,
		Channel: c.name,
		ID:      id,
	}

	err := setEventState(txn, key, state)
	if err != nil {
		return err
	}

	err = txn.Commit(nil)
	if err != nil {
		return err
	}

	c.shared.stateChansMutex.Lock()
	stateChans := c.shared.stateChans[id]
	if stateChans != nil {
		done := false
		for !done {
			select {
			case stateChan := <-stateChans:
				stateChan <- state
			default:
				done = true
			}
		}
		// TODO: cleanup (currently causing a deadlock because they need to have the lock to publish and we need it to read)
		// c.shared.stateChans[id] = nil
	}
	c.shared.stateChansMutex.Unlock()

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
func (c *Channel) RequeueEvent(e *deq.Event) {
	c.shared.out <- e
}

// AwaitDequeue blocks until the event is dequeued, then returns it's state.
func (c *Channel) AwaitDequeue(ctx context.Context, id string) (deq.EventState, error) {

	result := make(chan deq.EventState, 1)
	c.shared.stateChansMutex.Lock()
	if c.shared.stateChans[id] == nil {
		c.shared.stateChans[id] = make(chan chan deq.EventState)
	}
	c.shared.stateChansMutex.Unlock()
	select {
	case <-ctx.Done():
		return deq.EventState_UNSPECIFIED_STATE, ctx.Err()
	case c.shared.stateChans[id] <- result:
		// Just move on.
	}
	select {
	case state := <-result:
		return state, nil
	case <-ctx.Done():
		return deq.EventState_UNSPECIFIED_STATE, ctx.Err()
	}
}

func (s *sharedChannel) Idle() bool {
	s.idleMutex.RLock()
	defer s.idleMutex.RUnlock()
	return s.idle
}

func (s *sharedChannel) Missed() bool {
	s.missedMutex.Lock()
	defer s.missedMutex.Unlock()
	return s.missed
}

func (s *sharedChannel) SetMissed(m bool) {
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

		s.SetMissed(false)

		cursor, err = s.catchUp(cursor)
		if err != nil {
			s.broadcastErr(err)
		}

		// As long as we're up to date...
		for !s.Missed() {
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

func (s *sharedChannel) broadcastErr(err error) {
	panic(err)
	// for _, donec := range s.doneChans {
	// 	donec <- err
	// }
}
