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
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

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

	defaultRequeueLimit int
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
	// DefaultRequeueLimit is the default RequeueLimit for new events. Defaults to 40. Set to -1 for
	// no default limit.
	DefaultRequeueLimit int
	// UpgradeIfNeeded causes the database to be upgraded if needed when it is opened. If
	// UpgradeIfNeeded is false and the version of the data on disk doesn't match the version of the
	// running code, Open returns an ErrVersionMismatch.
	UpgradeIfNeeded bool
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

	requeueLimit := opts.DefaultRequeueLimit
	if requeueLimit == 0 {
		requeueLimit = 40
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
		db:                  db,
		in:                  make(chan eventPromise, 20),
		out:                 make(chan *Event, 20),
		sharedChannels:      make(map[channelKey]*sharedChannel),
		done:                make(chan error),
		defaultRequeueLimit: requeueLimit,
	}

	txn := db.NewTransaction(opts.UpgradeIfNeeded)

	version, err := s.getDBVersion(txn)
	if err != nil {
		return nil, fmt.Errorf("read current database version: %v", err)
	}
	if version != dbCodeVersion {
		if !opts.UpgradeIfNeeded {
			return nil, ErrVersionMismatch
		}

		err = s.upgradeDB(version)
		if err != nil {
			return nil, fmt.Errorf("upgrade db: %v", err)
		}
	}

	go s.garbageCollect(time.Minute * 5)
	go s.listenOut()

	return s, nil
}

// Close closes the store. Close must be called once the store is no longer in use.
//
//   db, err := deq.Open(...)
//   if err != nil {
//     // handle error
//   }
//   defer db.Close()
func (s *Store) Close() error {

	s.sharedChannelsMu.Lock()
	defer s.sharedChannelsMu.Unlock()

	if len(s.sharedChannels) > 0 {
		log.Printf("[WARN] Store.Close called before closing all channels")
	}

	close(s.done)
	close(s.out)

	err := s.db.Close()
	if err != nil {
		return err
	}

	return nil
}

func isValidTopic(topic string) bool {
	if len(topic) == 0 {
		return false
	}

	first, size := utf8.DecodeRuneInString(topic)
	if (first < 'a' || first > 'z') && (first < 'A' || first > 'Z') || unicode.IsDigit(first) {
		return false
	}

	for _, r := range topic[size:] {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '-' && r != '.' && r != '_' {
			return false
		}
	}

	return true
}

// Pub publishes an event.
func (s *Store) Pub(ctx context.Context, e Event) (Event, error) {

	if !isValidTopic(e.Topic) {
		return Event{}, fmt.Errorf("e.Topic is not valid")
	}
	if e.CreateTime.IsZero() {
		e.CreateTime = time.Now()
	}
	if e.DefaultState == EventStateUnspecified {
		e.DefaultState = EventStateQueued
	}

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

	s.sharedChannelsMu.Lock()
	defer s.sharedChannelsMu.Unlock()

	for _, channel := range s.sharedChannels {
		if channel.topic == e.Topic {
			channel.broadcastEventUpdated(e.ID, e.State)
		}
	}

	return e, nil
}

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
	}.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal event time key: %v", err)
	}

	eventKey, err := data.EventKey{
		ID:         id,
		Topic:      topic,
		CreateTime: e.CreateTime,
	}.Marshal(nil)
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
	// ErrAlreadyExists is returned when creating an event with a key that is in use
	ErrAlreadyExists = errors.New("already exists")
	// ErrVersionMismatch is returned when opening a database with an incorrect format.
	ErrVersionMismatch = errors.New("version mismatch")
	// ErrInternal is returned when an interanl error occurs
	ErrInternal = errors.New("internal error")
)
