package deqdb

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/internal/data"
)

type channelKey struct {
	name  string
	topic string
}

type sharedChannel struct {
	name  string
	topic string
	db    *badger.DB

	subscriptions int

	missedMutex sync.Mutex
	missed      bool

	in  chan *deq.Event
	out chan *deq.Event
	// done signals goroutines created by the sharedChannel to terminate.
	done chan struct{}
	// wg waits on all goroutines created by the sharedChannel to be done
	wg sync.WaitGroup

	idleMutex sync.RWMutex
	idle      bool

	stateSubsMutex sync.RWMutex
	// Pass in a response channel, when the event is dequeued the new state will be sent back on the
	// response channel
	stateSubs map[string]map[*EventStateSubscription]struct{}

	defaultRequeueLimit int
}

// addChannel adds a listener to a sharedChannel and returns the sharedChannel along with a done
// function that must be called once the shared channel is no longer being used by the caller.
func (s *Store) listenSharedChannel(name, topic string) (*sharedChannel, func()) {
	key := channelKey{name, topic}

	var shared *sharedChannel
	done := func() {
		s.sharedChannelsMu.Lock()

		shared.subscriptions--
		if shared.subscriptions == 0 {
			close(shared.done)
			delete(s.sharedChannels, key)
			defer shared.wg.Wait()
		}

		s.sharedChannelsMu.Unlock()
	}
	defer func() {
		s.sharedChannelsMu.Lock()
		defer s.sharedChannelsMu.Unlock()
		shared.subscriptions++
	}()

	s.sharedChannelsMu.Lock()
	defer s.sharedChannelsMu.Unlock()

	shared, ok := s.sharedChannels[key]
	if ok {
		return shared, done
	}

	shared = &sharedChannel{
		name:  name,
		topic: topic,
		db:    s.db,

		in:  make(chan *deq.Event, 20),
		out: make(chan *deq.Event, 20),

		stateSubs: make(map[string]map[*EventStateSubscription]struct{}),
		done:      make(chan struct{}),

		defaultRequeueLimit: s.defaultRequeueLimit,
	}
	s.sharedChannels[key] = shared

	shared.wg.Add(1)
	go func() {
		defer shared.wg.Done()
		shared.start()
	}()

	return shared, done
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

func (s *sharedChannel) RequeueEvent(e deq.Event, delay time.Duration) error {
	requeue := func() error {
		// retry for up to 10 conflicts.
		for i := 0; i < 10; i++ {
			// log.Printf("REQUEUING %s/%s count: %d", e.Topic, e.ID, e.RequeueCount)

			txn := s.db.NewTransaction(true)
			defer txn.Discard()

			channelPayload, err := incrementSavedRequeueCount(txn, s.name, s.topic, s.defaultRequeueLimit, &e)
			if err != nil {
				return err
			}

			err = txn.Commit(nil)
			if err == badger.ErrConflict {
				log.Printf("[WARN] Requeue Event %s %s: %v: retrying", s.topic, e.ID, err)
				txn.Discard()
				time.Sleep(time.Millisecond * 20)
				continue
			}
			if err != nil {
				return fmt.Errorf("commit channel event: %v", err)
			}

			if channelPayload.EventState != data.EventState_QUEUED {
				log.Printf("channel %s: requeue limit exceeded for topic: %s id: %s - dequeing", s.name, s.topic, e.ID)
				s.broadcastEventUpdated(e.ID, protoToEventState(channelPayload.EventState))
				return nil
			}

			e.RequeueCount = int(channelPayload.RequeueCount)
			select {
			case <-s.done:
				return nil
			case s.out <- &e:
				return nil
			}
		}

		return badger.ErrConflict
	}

	if delay == 0 {
		return requeue()
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-timer.C:
			err := requeue()
			if err != nil {
				log.Printf("requeue event: %v - forcing read from disk", err)
				s.setMissed(true)
			}
		case <-s.done:
		}
	}()

	return nil
}

func (s *sharedChannel) start() {

	cursor, err := s.getCursor(s.topic)
	if err != nil {
		s.broadcastErr(err)
		return
	}

	// timer := time.NewTimer(time.Hour)
	// if !timer.Stop() {
	// 	<-timer.C
	// }
	// defer timer.Stop()

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
			case <-s.done:
				return
			// The timer expired, we're idle
			// case <-timer.C:
			// We've got a new event, lets publish it
			case e := <-s.in:
				s.idleMutex.Lock()
				s.idle = false
				s.idleMutex.Unlock()
				// log.Printf("READING FROM MEMORY %s/%s count: %d", e.Topic, e.ID, e.RequeueCount)
				select {
				case <-s.done:
					return
				case s.out <- e:
					cursor, _ = data.EventKey{
						Topic:      e.Topic,
						CreateTime: e.CreateTime,
						ID:         e.ID,
					}.Marshal(nil)
				}
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
	prefix, err := data.EventPrefixTopic(s.topic)
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

		select {
		case <-s.done:
			return lastKey, nil
		case s.out <- &deq.Event{
			ID:           key.ID,
			Topic:        key.Topic,
			CreateTime:   key.CreateTime,
			Payload:      e.Payload,
			RequeueCount: int(channel.RequeueCount),
			State:        protoToEventState(channel.EventState),
			DefaultState: protoToEventState(e.DefaultEventState),
			Indexes:      e.Indexes,
		}:
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

	prefix, err := data.EventPrefixTopic(s.topic)
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

func (s *sharedChannel) broadcastEventUpdated(id string, state deq.EventState) {
	s.stateSubsMutex.RLock()
	defer s.stateSubsMutex.RUnlock()

	for sub := range s.stateSubs[id] {
		sub.add(state)
	}
}

func (s *sharedChannel) broadcastErr(err error) {
	panic(err)
	// for _, donec := range s.doneChans {
	// 	donec <- err
	// }
}
