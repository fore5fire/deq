package eventstore

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"gitlab.com/katcheCode/deq/api/v1/deq"
	"gitlab.com/katcheCode/deq/pkg/eventstore/data"
)

// Store is an EventStore connected to a specific database
type Store struct {
	db               *badger.DB
	in               chan eventPromise
	out              chan *deq.Event
	sharedChannelsMu sync.Mutex
	sharedChannels   map[channelKey]*sharedChannel
	// done is used for signaling to our store's go routine
	done chan error
}

// Options are parameters for opening a store
type Options struct {
	Dir string
}

type eventPromise struct {
	event *deq.Event
	done  chan error
}

// Open opens a store from disk, or creates a new store if it does not already exist
func Open(opts Options) (*Store, error) {

	if opts.Dir == "" {
		return nil, errors.New("option Dir is required")
	}

	badgerOpts := badger.DefaultOptions
	badgerOpts.Dir = opts.Dir
	badgerOpts.ValueDir = opts.Dir
	badgerOpts.SyncWrites = true

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, err
	}
	s := &Store{
		db:             db,
		in:             make(chan eventPromise, 20),
		out:            make(chan *deq.Event, 20),
		sharedChannels: make(map[channelKey]*sharedChannel),
		done:           make(chan error, 1),
	}

	go s.listenOut()

	return s, nil
}

// Close closes the store
func (s *Store) Close() error {
	err := s.db.Close()
	if err != nil {
		return err
	}
	// TODO fix end signal
	close(s.done)

	return nil
}

// Pub publishes an event
func (s *Store) Pub(e deq.Event) (deq.Event, error) {

	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	err := writeEvent(txn, &e)
	if err == ErrAlreadyExists {
		// Supress the error if the new and existing events have matching payloads.
		existing, err := getEvent(txn, e.Topic, e.Id, "")
		if err != nil {
			return deq.Event{}, fmt.Errorf("get existing event: %v", err)
		}
		if !bytes.Equal(existing.Payload, e.Payload) {
			return deq.Event{}, ErrAlreadyExists
		}
		return *existing, nil
	}
	if err != nil {
		return deq.Event{}, err
	}

	err = txn.Commit(nil)
	if err == badger.ErrConflict {
		txn := s.db.NewTransaction(false)
		defer txn.Discard()
		existing, err := getEvent(txn, e.Topic, e.Id, "")
		if err != nil {
			return deq.Event{}, fmt.Errorf("get conflicting event: %v", err)
		}
		if !bytes.Equal(existing.Payload, e.Payload) {
			return deq.Event{}, ErrAlreadyExists
		}
		return *existing, nil
	}
	if err != nil {
		return deq.Event{}, err
	}

	e.State = e.DefaultState

	if e.DefaultState == deq.EventState_QUEUED {
		s.out <- &e
	}

	for _, channel := range s.sharedChannels {
		channel.broadcastEventUpdated(e.Id, e.State)
	}

	return e, nil
}

// // Get returns the event for an event ID, or ErrNotFound if none is found
// func (s *Store) Get(topic, eventID, channel string) (*deq.Event, error) {
// 	txn := s.db.NewTransaction(false)
// 	defer txn.Discard()
//
// 	return getEvent(txn, topic, eventID, channel)
// }

// Del deletes an event
func (s *Store) Del(topic, id string) error {

	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	// TODO: refactor this, we really don't need the whole event, just
	// the create time
	e, err := getEvent(txn, topic, id, "")
	if err != nil {
		return err
	}

	eventTimeKey, err := data.EventTimeKey{
		ID:    id,
		Topic: topic,
	}.Marshal()
	if err != nil {
		return fmt.Errorf("marshal event time key: %v", err)
	}

	eventKey, err := data.EventKey{
		ID:         id,
		Topic:      topic,
		CreateTime: time.Unix(0, e.CreateTime),
	}.Marshal()
	if err != nil {
		return fmt.Errorf("marshal event key: %v", err)
	}

	// TODO: cleanup channel keys

	err = txn.Delete(eventTimeKey)
	if err != nil {
		return fmt.Errorf("delete event time: %v", err)
	}
	err = txn.Delete(eventKey)
	if err != nil {
		return fmt.Errorf("delete event key: %v", err)
	}

	err = txn.Commit(nil)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) listenOut() {
	for e := range s.out {
		s.sharedChannelsMu.Lock()
		for _, shared := range s.sharedChannels {
			if shared.topic == e.Topic {
				select {
				case shared.in <- e:
					// TODO: Move db write code here
				default: // Skip if full, listeners can catch up from disk later
					shared.setMissed(true)
				}
			}
		}
		s.sharedChannelsMu.Unlock()
	}
}

// Channel returns a channel with the given name. If no channel exists with that name, a new channel is created
// func (s *Store) Follow(channelName string) *Channel {
//
// }

// fetch is used to get events starting at the iterator's afterKey until the most recent event.
// It returns a stream that will be sent events, or an error if a precondition is violated.
// If follow is false, eventc will be closed once all existing events after afterKey have been sent, or if done is closed.
// If follow is true, eventc will not be closed until done is closed. After all existing events have been sent, any new events will also be sent once they have been persisted to the disk.
// All data sent into eventc is in the canonical order (The order it was persisted to disk).
// If an error occurs fetching data, eventc will be closed, and the error can be accessed by calling Err on this Iterator
// If a fetch has already been called on this iterator, ErrIteratorAlreadyStarted will be returned.

var (
	// ErrNotFound is returned when a requested event doesn't exist in the store
	ErrNotFound = errors.New("event not found")
	// ErrInternal is returned when an interanl error occurs
	ErrInternal = errors.New("internal error")
	// ErrAlreadyExists is returned when creating an event with a key that is in use
	ErrAlreadyExists = errors.New("already exists")
)
