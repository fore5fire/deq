package eventstore

import (
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"gitlab.com/katcheCode/deqd/api/v1/deq"
)

// Channel allows multiple listeners to synchronize processing of events
type Channel struct {
	name string
	out  chan *deq.Event
	idle chan struct{}
	done chan error
	err  error
	db   *badger.DB
}

type sharedChannel struct {
	// Mutex protects idelChans and doneChans
	sync.Mutex
	name      string
	in        chan *deq.Event
	out       chan *deq.Event
	done      chan error
	idleChans []chan struct{}
	doneChans []chan error
	db        *badger.DB
}

// Channel returns the channel for a given name
func (s *Store) Channel(name string) Channel {
	s.sharedChannelsMu.RLock()
	shared, ok := s.sharedChannels[name]
	s.sharedChannelsMu.RUnlock()

	if !ok {
		shared = &sharedChannel{
			name: name,
			in:   make(chan *deq.Event, 20),
			out:  make(chan *deq.Event, 20),
			db:   s.db,
		}
		s.sharedChannelsMu.Lock()
		s.sharedChannels[name] = shared
		s.sharedChannelsMu.Unlock()

		go shared.start()
	}

	// DON'T FORGET TO ADD CHECK FOR FAILED CHANNEL

	idle := make(chan struct{})
	done := make(chan error, 1)
	shared.Lock()
	defer shared.Unlock()
	shared.idleChans = append(shared.idleChans, idle)
	shared.doneChans = append(shared.doneChans, done)

	return Channel{
		name: name,
		out:  shared.out,
		idle: idle,
		done: done,
		db:   s.db,
	}
}

// Follow returns
func (c Channel) Follow() (eventc chan *deq.Event, idle chan struct{}) {

	// go func() {
	// 	<-done
	// }()

	return c.out, c.idle
}

// Close cleans up resources for this Channel
func (c Channel) Close() {
	// TODO: clean up sharedChannel stuff
}

// Err returns the error that caused this channel to fail, or nil if the channel closed cleanly
func (c Channel) Err() error {
	return c.err
}

// SetEventState sets the state of an event for this channel
func (c Channel) SetEventState(topic, id string, state deq.EventState) error {
	txn := c.db.NewTransaction(true)
	defer txn.Discard()

	_, err := txn.Get([]byte(eventPrefix + "/" + url.QueryEscape(topic) + "/" + url.QueryEscape(id)))
	if err == badger.ErrKeyNotFound {
		return ErrNotFound
	}
	if err != nil {
		return err
	}

	// txn.Get()

	err = txn.Commit(nil)
	if err != nil {
		return err
	}
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
	c.out <- e
}

func (s *sharedChannel) start() {

	cursor, err := s.getCursor()
	if err != nil {
		s.broadcastErr(err)
		return
	}

	for {
		cursor, err = s.catchUp(cursor)
		if err != nil {
			s.broadcastErr(err)
		}

		// As long as s.in hasn't filled up...
		for len(s.in) < cap(s.in) {

			select {
			// Periodically poll idle so newly connected clients will know
			case <-time.After(time.Second / 2):
				s.Lock()
				for _, idle := range s.idleChans {
					select {
					case idle <- struct{}{}:
					default:
						// Don't block if idle isn't ready - we'll signal it next time around
					}
				}
				s.Unlock()
			// We've got a new event, lets publish it
			case e := <-s.in:
				s.out <- e
				cursor = eventPrefix + "/" + url.QueryEscape(e.Topic) + "/" + url.QueryEscape(e.Id)
			}
		}

		// We might have missed an event, lets go back to reading from disk.
		// First let's drain some events so we can tell if we've missed any more.
		// We'll read these off the disk, so it's ok to discard them
		s.Lock()
		for len(s.in) > 0 {
			<-s.in
		}
		s.Unlock()
	}
}

// catchUp returns nil instead of new prefix when time to quit
func (s *sharedChannel) catchUp(cursor string) (string, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 100
	it := txn.NewIterator(opts)
	defer it.Close()

	var lastKey []byte

	for it.Seek([]byte(cursor + "\uffff")); it.ValidForPrefix([]byte(eventPrefix + "/")); it.Next() {

		item := it.Item()
		lastKey = item.KeyCopy(lastKey)
		buffer, err := item.ValueCopy(nil)
		if err != nil {
			return "", err
		}

		parsed := strings.Split(string(lastKey), "/")
		if len(parsed) != 3 {
			log.Printf("invalid event key: %s", lastKey)
			continue
		}

		s.out <- &deq.Event{
			Id:      parsed[2],
			Topic:   parsed[1],
			Payload: buffer,
			// State:
		}
	}

	return string(lastKey), nil
}

func (s *sharedChannel) getCursor() (string, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get([]byte(cursorPrefix + "/" + s.name))
	if err == badger.ErrKeyNotFound {
		return eventPrefix + "/", nil
	}
	if err != nil {
		return "", err
	}

	cursor, err := item.ValueCopy(nil)
	if err != nil {
		return "", err
	}

	return string(cursor), nil
}

func (s *sharedChannel) broadcastErr(err error) {
	panic(err)
	// for _, donec := range s.doneChans {
	// 	donec <- err
	// }
}
