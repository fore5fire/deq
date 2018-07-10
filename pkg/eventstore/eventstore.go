package eventstore

import (
	"errors"
	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	// "github.com/satori/go.uuid"
	"encoding/binary"
	"gitlab.com/katcheCode/deqd/api/v1/deq"
	"sync"
	"time"
)

// Store is an EventStore connected to a specific database
type Store struct {
	db               *badger.DB
	in               chan eventPromise
	out              chan deq.Event
	sharedChannelsMu sync.RWMutex
	sharedChannels   map[string]*sharedChannel
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
		out:            make(chan deq.Event, 20),
		sharedChannels: make(map[string]*sharedChannel),
	}

	go s.startIn()
	go s.startOut()

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

// Create inserts an event into the store after assigning it an id
func (s *Store) Create(e deq.Event) (deq.Event, error) {
	e.Id = nil
	return s.insert(e)
}

// Insert inserts a new event into the store
func (s *Store) Insert(e deq.Event) (deq.Event, error) {
	return s.insert(e)
}

func (s *Store) insert(e deq.Event) (deq.Event, error) {

	done := make(chan error, 1)

	s.in <- eventPromise{
		event: &e,
		done:  done,
	}

	return e, <-done
}

func (s *Store) startIn() {

	var counter uint32

	for promise := range s.in {

		promise.event.Key = GenerateID(counter)
		counter++

		if promise.event.Id == nil {
			promise.event.Id = promise.event.Key
		}

		txn := s.db.NewTransaction(true)
		defer txn.Discard()

		data, err := proto.Marshal(promise.event)
		if err != nil {
			promise.done <- err
		}

		txn.Set(append(eventPrefix, promise.event.Key...), data)
		err = txn.Commit(nil)
		if err != nil {
			promise.done <- err
		}

		s.out <- *promise.event
		close(promise.done)
	}
}

func (s *Store) startOut() {
	for e := range s.out {
		s.sharedChannelsMu.RLock()
		for _, shared := range s.sharedChannels {
			select {
			case shared.in <- e:
				// TODO: Move db write code here
			default: // Skip if full, listeners can catch up from disk later
			}
		}
		s.sharedChannelsMu.RUnlock()
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
	// ErrChannelNotFound is returned when attempting to access a channel that has not been created
	ErrChannelNotFound = errors.New("Attempted to read from a channel that does not exist")
	// ErrKeyNotFound is returned when attempting to set event status for an event that does not exist
	ErrKeyNotFound = errors.New("Key not found")
	// ErrInternal is returned when an interanl error occurs
	ErrInternal = errors.New("Internal error")
)

var (
	sequenceKey   = []byte("__event-store-sequence-key__")
	channelPrefix = []byte("C")
	cursorPrefix  = []byte("c")
	eventPrefix   = []byte("E")
)

// GenerateID returns a new id using the current time
func GenerateID(count uint32) []byte {
	id := make([]byte, 10)
	timePart := uint64(time.Now().UnixNano()) / uint64(time.Millisecond)
	binary.BigEndian.PutUint64(id, timePart<<16)
	binary.BigEndian.PutUint32(id[6:], count)
	return id
}
