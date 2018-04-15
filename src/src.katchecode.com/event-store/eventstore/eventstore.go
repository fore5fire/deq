package eventstore

import (
	"errors"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	// "github.com/satori/go.uuid"
	"src.katchecode.com/event-store/api/v1/eventstore"
	"sync"
	"time"
)

// Store is an EventStore connected to a specific database
type Store struct {
	db               *badger.DB
	incoming         chan *eventstore.Event
	openChannels     map[string]*Channel
	openChannelsLock sync.Mutex
}

// Options are parameters for opening a store
type Options struct {
	Dir string
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
	store := &Store{
		db:           db,
		incoming:     make(chan *eventstore.Event, 20),
		openChannels: make(map[string]*Channel),
	}

	return store, nil
}

// Close closes the store
func (s *Store) Close() error {
	err := s.db.Close()
	if err != nil {
		return err
	}

	return nil
}

// Create inserts an event into the store after assigning it an id
func (s *Store) Create(e *eventstore.Event) error {
	return s.insert(e, true)
}

// Insert inserts a new event into the store
func (s *Store) Insert(e *eventstore.Event) error {
	return s.insert(e, false)
}

func (s *Store) insert(e *eventstore.Event, setID bool) error {

	const leaseSize = 1

	sequence, err := s.db.GetSequence(sequenceKey, leaseSize)
	if err != nil {
		return nil
	}

	// defer sequence.Release()

	err = s.db.Update(func(txn *badger.Txn) (err error) {

		data, err := proto.Marshal(e)
		if err != nil {
			return err
		}
		num, err := sequence.Next()
		if err != nil {
			return err
		}

		now := time.Now().UnixNano()
		key := string(eventPrefix) + string(now) + string(num)

		if setID {
			e.Id = key
		}

		txn.Set([]byte(key), data)
		return nil
	})

	s.incoming <- e

	if err != nil {
		return err
	}

	return nil
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
)

var (
	sequenceKey   = []byte("__event-store-sequence-key__")
	channelPrefix = []byte("C")
	eventPrefix   = []byte("E")
)
