package deqdb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/deqdb/internal/data"
	"gitlab.com/katcheCode/deq/deqdb/internal/priority"
)

type channelKey struct {
	name  string
	topic string
}

type sharedChannel struct {
	name  string
	topic string
	db    data.DB

	subscriptions int

	missedMutex sync.Mutex
	missed      bool

	in  chan *deq.Event
	out chan *deq.Event

	// inc is the in queue for incrementing event's send count
	inc chan *deq.Event
	// scheduled is the in queue for scheduling events in the future.
	scheduled chan schedule
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

	info, debug Logger
}

type schedule struct {
	Event *deq.Event
	Time  time.Time
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
			close(shared.inc)
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

		inc:       make(chan *deq.Event, 50),
		scheduled: make(chan schedule, 20),

		stateSubs: make(map[string]map[*EventStateSubscription]struct{}),
		done:      make(chan struct{}),

		defaultRequeueLimit: s.defaultRequeueLimit,

		info:  s.info,
		debug: s.debug,
	}
	s.sharedChannels[key] = shared

	shared.wg.Add(3)
	go func() {
		defer shared.wg.Done()
		shared.start()
	}()
	go func() {
		defer shared.wg.Done()
		shared.startEventScheduler()
	}()
	go func() {
		defer shared.wg.Done()
		shared.startSendCountIncrementer()
	}()

	return shared, done
}

func (s *sharedChannel) Idle() bool {
	s.idleMutex.RLock()
	defer s.idleMutex.RUnlock()
	return s.idle
}

func (s *sharedChannel) setIdle(idle bool) {
	s.idleMutex.Lock()
	s.idle = idle
	s.idleMutex.Unlock()
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

func (s *sharedChannel) startEventScheduler() {

	queue := priority.NewQueue()
	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C

	for {
		// Read the event that needs to be sent out soonest.
		next, t := queue.Peek()
		s.debug.Printf("channel %q: scheduler waiting on %v at %v", s.name, next, t)
		if next != nil {
			timer.Reset(t.Sub(time.Now()))
		}

		select {
		case <-timer.C:
			queue.Pop()
			select {
			case s.in <- next:
				s.debug.Printf("channel %q: event %q %q schedule time reached", s.name, next.Topic, next.ID)
			case <-s.done:
				return
			}
			// No need to stop or clear the timer - it already fired and we just cleared it by reading
			// timer.C
		case sched := <-s.scheduled:
			queue.Add(sched.Event, sched.Time)
			s.debug.Printf("channel %q: event %q %q scheduled at %v", s.name, sched.Event.Topic, sched.Event.ID, sched.Time)
			// Stop and clear the timer - if this new event is earlier than the current timer, we need to
			// change the timer accordingly next loop iteration.
			if next != nil && !timer.Stop() {
				<-timer.C
			}
		case <-s.done:
			return
		}
	}
}

func (s *sharedChannel) startSendCountIncrementer() {

	for e := range s.inc {

		// TODO: batch writes in one transaction to improve performance.
		txn := s.db.NewTransaction(true)
		defer txn.Discard()

		key := data.SendCountKey{
			Channel: s.name,
			Topic:   s.topic,
			ID:      e.ID,
		}

		var sendCount data.SendCount
		err := data.GetSendCount(txn, &key, &sendCount)
		if err != nil && err != deq.ErrNotFound {
			s.info.Printf("increment send count: get channel event: %v", err)
			continue
		}

		if s.defaultRequeueLimit != -1 && int(sendCount.SendCount) >= 40 {
			channelEvent := data.ChannelPayload{
				EventState: data.EventState_SEND_LIMIT_EXCEEDED,
			}
			err = data.SetChannelEvent(txn, &data.ChannelKey{
				Channel: s.name,
				Topic:   s.topic,
				ID:      e.ID,
			}, &channelEvent)
			if err != nil {
				s.info.Printf("increment send count: dequeue event: %v", err)
				continue
			}
			// Broadcast the update, but only if the transaction is successfully committed.
			err = txn.Commit()
			if err != nil {
				s.info.Printf("increment send count: dequeue event: commit: %v", err)
				continue
			}
			s.info.Printf("channel %q: requeue limit exceeded for topic: %q id: %q - dequeuing", s.name, s.topic, e.ID)
			s.broadcastEventUpdated(e.ID, data.EventStateFromProto(channelEvent.EventState))
		}

		sendCount.SendCount++

		err = data.SetSendCount(txn, &key, &sendCount)
		if err != nil {
			s.info.Printf("increment send count: set send count: %v", err)
			continue
		}

		err = txn.Commit()
		if err != nil {
			s.info.Printf("increment send count: commit: %v", err)
			continue
		}
	}
}

func (s *sharedChannel) IncrementSendCount(ctx context.Context, e *deq.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return nil
	case s.inc <- e:
		return nil
	}
}

// ScheduleEvent schedules an event to be sent no earlier than t.
func (s *sharedChannel) ScheduleEvent(ctx context.Context, e *deq.Event, t time.Time) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return nil
	case s.scheduled <- schedule{
		Event: e,
		Time:  t,
	}:
		return nil
	}
}

// ScheduleEvent attempts to schedule an event to be sent no earlier than t. If there is no
// available space in the schedule buffer, the sharedChannel is notified to look up the event from
// disk when it is ready to schedule it.
func (s *sharedChannel) TryScheduleEvent(e *deq.Event, t time.Time) {
	select {
	case s.scheduled <- schedule{
		Event: e,
		Time:  t,
	}:
	default:
		s.setMissed(true)
	}
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

		// Let's drain our events so we have room for some that might come in while we catch up.
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
				s.setIdle(true)
			}

			select {
			case <-s.done:
				return
			// The timer expired, we're idle
			// case <-timer.C:
			// We've got a new event, lets publish it
			case e := <-s.in:
				s.debug.Printf("channel %q: preparing scheduled event %q %q", s.name, e.Topic, e.ID)
				s.setIdle(false)
				select {
				case <-s.done:
					return
				case s.out <- e:
					s.debug.Printf("channel %q: prepared scheduled event %q %q", s.name, e.Topic, e.ID)
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
			s.info.Printf("catch up: parse event key %s: %v", lastKey, err)
			continue
		}

		var e data.EventPayload
		err = item.Value(func(val []byte) error {
			err = proto.Unmarshal(val, &e)
			if err != nil {
				return fmt.Errorf("unmarshal event: %v", err)
			}
			return nil
		})
		if err != nil {
			s.info.Printf("catch up: read %q %q: %v", key.Topic, key.ID, err)
			continue
		}

		channel := data.ChannelPayload{
			EventState: e.DefaultEventState,
		}
		err = data.GetChannelEvent(txn, &data.ChannelKey{
			Channel: s.name,
			Topic:   key.Topic,
			ID:      key.ID,
		}, &channel)
		if err != nil && err != deq.ErrNotFound {
			s.info.Printf("catch up: get channel event %q %q %q: %v", key.Topic, key.ID, s.name, err)
			continue
		}

		if channel.EventState != data.EventState_QUEUED {
			// Not queued, don't send
			continue
		}

		var count data.SendCount
		err = data.GetSendCount(txn, &data.SendCountKey{
			Channel: s.name,
			Topic:   key.Topic,
			ID:      key.ID,
		}, &count)
		if err != nil && err != deq.ErrNotFound {
			s.info.Printf("catch up: get send count for event %q %q on channel %q: %v", key.Topic, key.ID, s.name, err)
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
			SendCount:    int(count.SendCount),
			State:        data.EventStateFromProto(channel.EventState),
			DefaultState: data.EventStateFromProto(e.DefaultEventState),
			Indexes:      e.Indexes,
		}:
			s.debug.Printf("channel %q: read from disk: scheduled event %q %q", s.name, key.Topic, key.ID)
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
	// 		s.info.Printf("getCursor: parse event key %s: %v - skipping", current, err)
	// 		continue
	// 	}
	// 	_, err = txn.Get(current)
	// 	if err == badger.ErrKeyNotFound {
	//
	// 	}
	// }

	return current, nil
}

func (s *sharedChannel) broadcastEventUpdated(id string, state deq.State) {
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
