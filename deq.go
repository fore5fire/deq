/*
Package deq provides an embedded key-value event queue.

To use deq as a standalone server, see package gitlab.com/katcheCode/deq/cmd/deqd in this
repository.

To connect to a standalone deq server, see package gitlab.com/katceCode/deq/deqc in this
repository.
*/
package deq

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"gitlab.com/katcheCode/deq/internal/data"
)

// Store is an event store connected to a specific database
type Store struct {
	db               *badger.DB
	in               chan eventPromise
	out              chan *Event
	sharedChannelsMu sync.Mutex
	sharedChannels   map[channelKey]*sharedChannel
	// done is used for signaling to our store's go routine
	done chan error
}

// Options are parameters for opening a store
type Options struct {
	// Dir specifies the directory where data will be written. Required.
	Dir string
	// LoadingMode defaults to LoadingModeBalanced
	LoadingMode LoadingMode
	// DangerousDeleteCorrupt allows DEQ to delete any corrupt data from an unclean shutdown. If this
	// option is false, attempting to call Open on a database with corrupt data will fail.
	DangerousDeleteCorrupt bool
}

// LoadingMode specifies how to load data into memory. Generally speaking, lower memory is slower
// and puts more load on the disk, while higher memory is much faster and requires fewer reads from
// disk. All data is still persisted to disk regardless of the LoadingMode. Always benchmark to see
// what best meets your needs.
type LoadingMode int

const (
	LoadingModeUnspecified LoadingMode = iota
	LoadingModeLowestMemory
	LoadingModeLowMemory
	LoadingModeBalanced
	LoadingModeHighMemory
	LoadingModeHighestMemory
)

func (m LoadingMode) badgerOptions() (options.FileLoadingMode, options.FileLoadingMode) {
	switch m {
	case LoadingModeLowestMemory:
		return options.FileIO, options.FileIO
	case LoadingModeLowMemory:
		return options.MemoryMap, options.FileIO
	case LoadingModeBalanced, LoadingModeUnspecified:
		return options.MemoryMap, options.MemoryMap
	case LoadingModeHighMemory:
		return options.LoadToRAM, options.MemoryMap
	case LoadingModeHighestMemory:
		return options.LoadToRAM, options.LoadToRAM
	default:
		panic("unrecognized LoadingMode")
	}
}

type eventPromise struct {
	event *Event
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
	badgerOpts.TableLoadingMode, badgerOpts.ValueLogLoadingMode = opts.LoadingMode.badgerOptions()
	badgerOpts.MaxTableSize = 1 << 24
	badgerOpts.Truncate = opts.DangerousDeleteCorrupt

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, err
	}
	s := &Store{
		db:             db,
		in:             make(chan eventPromise, 20),
		out:            make(chan *Event, 20),
		sharedChannels: make(map[channelKey]*sharedChannel),
		done:           make(chan error),
	}

	go s.garbageCollect(time.Minute * 5)
	go s.listenOut()

	return s, nil
}

// Close closes the store
func (s *Store) Close() error {

	close(s.done)

	err := s.db.Close()
	if err != nil {
		return err
	}

	return nil
}

// Pub publishes an event
func (s *Store) Pub(e Event) (Event, error) {

	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	err := writeEvent(txn, &e)
	if err == ErrAlreadyExists {
		// Supress the error if the new and existing events have matching payloads.
		existing, err := getEvent(txn, e.Topic, e.ID, "")
		if err != nil {
			return Event{}, fmt.Errorf("get existing event: %v", err)
		}
		if !bytes.Equal(existing.Payload, e.Payload) {
			return Event{}, ErrAlreadyExists
		}
		return *existing, nil
	}
	if err != nil {
		return Event{}, err
	}

	err = txn.Commit(nil)
	if err == badger.ErrConflict {
		txn := s.db.NewTransaction(false)
		defer txn.Discard()
		existing, err := getEvent(txn, e.Topic, e.ID, "")
		if err != nil {
			return Event{}, fmt.Errorf("get conflicting event: %v", err)
		}
		if !bytes.Equal(existing.Payload, e.Payload) {
			return Event{}, ErrAlreadyExists
		}
		return *existing, nil
	}
	if err != nil {
		return Event{}, err
	}

	e.State = e.DefaultState

	if e.DefaultState == EventStateQueued {
		s.out <- &e
	}

	for _, channel := range s.sharedChannels {
		channel.broadcastEventUpdated(e.ID, e.State)
	}

	return e, nil
}

// func (s *Store) SyncTo(ctx context.Context, client *deq.DEQClient) error {

// }

// func (s *Store) SyncFrom(ctx context.Context, client *deq.DEQClient) error {

// }

// func (s *Store) Sync(ctx context.Context, client *deq.DEQClient) error {

// 	ctx, cancel := context.WithCancel(ctx)
// 	defer cancel()

// 	errorc := make(chan error, 2)

// 	wg := sync.WaitGroup{}
// 	wg.Add(2)

// 	go func() {
// 		defer wg.Done()
// 		err := s.SyncTo(ctx, client)
// 		if err != nil {
// 			errorc <- err
// 		}
// 	}()
// 	go func() {
// 		defer wg.Done()
// 		err := s.SyncFrom(ctx, client)
// 		if err != nil {
// 			errorc <- err
// 		}
// 	}()
// 	wg.Wait()
// 	close(errorc)
// 	return <-errorc
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
		CreateTime: e.CreateTime,
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

func (s *Store) garbageCollect(interval time.Duration) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.db.RunValueLogGC(0.7)
		}
	}
}

var (
	// ErrNotFound is returned when a requested event doesn't exist in the database
	ErrNotFound = errors.New("event not found")
	// ErrInternal is returned when an interanl error occurs
	ErrInternal = errors.New("internal error")
	// ErrAlreadyExists is returned when creating an event with a key that is in use
	ErrAlreadyExists = errors.New("already exists")
)
