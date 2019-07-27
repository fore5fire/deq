/*
Package deq provides an embedded key-value event queue.

To use deq as a standalone server, see package gitlab.com/katcheCode/deq/cmd/deqd in this
repository.

To connect to a standalone deq server, see package gitlab.com/katceCode/deq/deqc in this
repository.
*/
package deqdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/deqdb/internal/data"
	"gitlab.com/katcheCode/deq/deqdb/internal/upgrade"
	"gitlab.com/katcheCode/deq/deqtype"
)

type storeClient struct {
	*Store
}

func AsClient(s *Store) deq.Client {
	return &storeClient{s}
}

func (c *storeClient) Channel(name, topic string) deq.Channel {
	return c.Store.Channel(name, topic)
}

// func (c *storeClient) Channel(name, topic string) deq.Channel {
// 	return c.Store.Channel(name, topic)
// }

// Store is an DEQ event store that saves to disk
type Store struct {
	db               data.DB
	in               chan eventPromise
	out              chan *deq.Event
	sharedChannelsMu sync.Mutex
	sharedChannels   map[channelKey]*sharedChannel
	// done is used for signaling to our store's go routine
	done chan error

	defaultRequeueLimit int

	info  Logger
	debug Logger
}

// Options are parameters for opening a store
type Options struct {
	// Dir specifies the directory where data will be written. Required.
	Dir string
	// LoadingMode defaults to LoadingModeBalanced
	LoadingMode LoadingMode
	// KeepCorrupt prevents DEQ from deleting any corrupt data after an unclean shutdown. If this
	// option is true, attempting to call Open on a database with corrupt data will fail.
	KeepCorrupt bool
	// DefaultRequeueLimit is the default RequeueLimit for new events. Defaults to 40. Set to -1 for
	// no default limit.
	DefaultRequeueLimit int
	// UpgradeIfNeeded causes the database to be upgraded if needed when it is opened. If
	// UpgradeIfNeeded is false and the version of the data on disk doesn't match the version of the
	// running code, Open returns an ErrVersionMismatch.
	UpgradeIfNeeded bool

	// Info is used to log information that is not directly returned from function calls, including
	// ignored errors in background goroutines. Info defaults to using a log.Logger from the standard
	// library printing to stderr.
	Info Logger

	// Debug is used to log debug information. It Defaults to disabled, and should usually only be set
	// to provide details for debugging this package.
	Debug Logger
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
	event *deq.Event
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

	info := opts.Info
	if info == nil {
		info = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile|log.LUTC)
	}
	debug := opts.Debug
	if debug == nil {
		debug = disabledLogger{}
	}

	tableLoadingMode, valueLogLoadingMode := opts.LoadingMode.badgerOptions()

	badgerOpts := badger.DefaultOptions(opts.Dir).
		WithValueDir(opts.Dir).
		WithSyncWrites(true).
		WithTableLoadingMode(tableLoadingMode).
		WithValueLogLoadingMode(valueLogLoadingMode).
		WithMaxTableSize(1 << 24).
		WithTruncate(!opts.KeepCorrupt).
		WithLogger(badgerLogger{
			info:  info,
			debug: debug,
		})

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, err
	}

	return open(data.DBFromBadger(db), requeueLimit, opts.UpgradeIfNeeded, info, debug)
}

func open(db data.DB, defaultRequeueLimit int, allowUpgrade bool, info, debug Logger) (*Store, error) {

	s := &Store{
		db:                  db,
		in:                  make(chan eventPromise, 20),
		out:                 make(chan *deq.Event, 20),
		sharedChannels:      make(map[channelKey]*sharedChannel),
		done:                make(chan error),
		defaultRequeueLimit: defaultRequeueLimit,
		info:                info,
		debug:               debug,
	}

	txn := db.NewTransaction(true)

	version, err := upgrade.StorageVersion(txn)
	if err != nil && err != upgrade.ErrVersionUnknown {
		return nil, fmt.Errorf("read current storage version: %v", err)
	}
	if err == upgrade.ErrVersionUnknown {
		// No version on disk, should be a new database.
		s.debug.Printf("no version found, assuming new database")
		err := txn.Set([]byte(upgrade.VersionKey), []byte(upgrade.CodeVersion))
		if err != nil {
			return nil, fmt.Errorf("write version for new db: %v", err)
		}
		version = upgrade.CodeVersion
	}

	s.debug.Printf("current storage version is %s", version)

	if version != upgrade.CodeVersion {
		if !allowUpgrade {
			return nil, deq.ErrVersionMismatch
		}

		err = upgrade.DB(context.TODO(), s.db, version)
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
		return errors.New("Store.Close called before closing all channels")
	}

	close(s.done)
	close(s.out)

	err := s.db.Close()
	if err != nil {
		return err
	}

	return nil
}

// validateTopic returns an error if topic is invalid, or nil otherwise. If allowInternal is true,
// topics that are reserved for system use will pass validation. Generally, allowInternal should be
// false only when the user is attempting to publish an event. When the system publishes an event or
// the user is reading events from a topic, access to internal topic names should be allowed.
func validateTopic(topic string, allowInternal bool) error {
	if len(topic) == 0 {
		return errors.New("topic must not be empty")
	}

	first, size := utf8.DecodeRuneInString(topic)
	if (first < 'a' || first > 'z') && (first < 'A' || first > 'Z') || unicode.IsDigit(first) {
		return errors.New("topic must begin with an alphanumeric character")
	}

	for _, r := range topic[size:] {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '-' && r != '.' && r != '_' {
			return errors.New("topic must only contain alphanumeric characters, as well as '-', '.', and '_'")
		}
	}

	if !allowInternal && strings.HasPrefix(topic, "deq.events.") {
		return errors.New("topics in package deq.events are reserved")
	}

	return nil
}

// Pub publishes an event.
func (s *Store) Pub(ctx context.Context, e deq.Event) (deq.Event, error) {

	err := validateTopic(e.Topic, false)
	if err != nil {
		return deq.Event{}, fmt.Errorf("validate e.Topic: %v", err)
	}
	if e.CreateTime.IsZero() {
		e.CreateTime = time.Now()
	}
	if e.DefaultState == deq.StateUnspecified {
		e.DefaultState = deq.StateQueued
	}
	e.State = e.DefaultState
	e.SendCount = 0

	for i := 0; i < 3; i++ {
		txn := s.db.NewTransaction(true)
		defer txn.Discard()

		// Publish a topic event if this is the first event on this topic.
		var topicEvent *deq.Event
		_, err = data.GetEvent(txn, "deq.events.Topic", e.Topic, "")
		if err != nil && err != deq.ErrNotFound {
			return deq.Event{}, fmt.Errorf("check existing topic: %v", err)
		}
		if err == deq.ErrNotFound {
			topic := &deqtype.Topic{
				Topic: e.Topic,
			}
			payload, err := topic.Marshal()
			if err != nil {
				return deq.Event{}, fmt.Errorf("new topic: marshal topic payload: %v", err)
			}
			topicEvent = &deq.Event{
				ID:         e.Topic,
				CreateTime: time.Now(),
				Topic:      "deq.events.Topic",
				Payload:    payload,
			}
			err = data.WriteEvent(txn, topicEvent)
			if err != nil {
				return deq.Event{}, fmt.Errorf("new topic: write event: %v", err)
			}
		}

		// Write the event to disk.
		err = data.WriteEvent(txn, &e)
		if err == deq.ErrAlreadyExists {
			// Suppress the error if the new and existing events have matching payloads.
			existing, err := data.GetEvent(txn, e.Topic, e.ID, "")
			if err != nil {
				return deq.Event{}, fmt.Errorf("get existing event: %v", err)
			}
			if !bytes.Equal(existing.Payload, e.Payload) {
				return deq.Event{}, deq.ErrAlreadyExists
			}
			return *existing, nil
		}
		if err != nil {
			return deq.Event{}, err
		}

		err = txn.Commit()
		if err == badger.ErrConflict {
			txn := s.db.NewTransaction(false)
			defer txn.Discard()
			existing, err := data.GetEvent(txn, e.Topic, e.ID, "")
			if err == deq.ErrNotFound {
				// The conflict was caused by another event on the topic thinking it was first and also
				// publishing a deq.events.Topic event. Just one retry should be enough.
				continue
			}
			if err != nil {
				return deq.Event{}, fmt.Errorf("get conflicting event: %v", err)
			}
			if !bytes.Equal(existing.Payload, e.Payload) {
				return deq.Event{}, deq.ErrAlreadyExists
			}
			return *existing, nil
		}
		if err != nil {
			return deq.Event{}, err
		}

		if e.DefaultState == deq.StateQueued {
			s.queueOut(&e)
		}

		if topicEvent != nil {
			s.queueOut(topicEvent)
		}

		return e, nil
	}

	return deq.Event{}, badger.ErrConflict
}

func (s *Store) queueOut(e *deq.Event) {
	s.out <- e

	s.sharedChannelsMu.Lock()
	defer s.sharedChannelsMu.Unlock()

	for _, shared := range s.sharedChannels {
		if shared.topic == e.Topic {
			shared.broadcastEventUpdated(e.ID, e.State)
		}
	}
}

// Del deletes an event
func (s *Store) Del(ctx context.Context, topic, id string) error {

	err := validateTopic(topic, false)
	if err != nil {
		return fmt.Errorf("validate topic: %v", err)
	}

	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	// TODO: refactor this, we really don't need the whole event, just
	// the create time
	e, err := data.GetEvent(txn, topic, id, "")
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

	err = txn.Delete(eventTimeKey)
	if err != nil {
		return fmt.Errorf("delete event time: %v", err)
	}
	err = txn.Delete(eventKey)
	if err != nil {
		return fmt.Errorf("delete event key: %v", err)
	}

	readTxn := s.db.NewTransaction(false)
	defer readTxn.Discard()

	// Delete any indexes that haven't been covered.
	for _, index := range e.Indexes {
		indexKey := data.IndexKey{
			Topic: topic,
			Value: index,
		}
		var indexPayload data.IndexPayload
		err = data.GetIndexPayload(readTxn, &indexKey, &indexPayload)
		if err != nil && err != deq.ErrNotFound {
			return fmt.Errorf("check index %q for newer event: %v", index, err)
		}
		if err == deq.ErrNotFound || indexPayload.EventId != id {
			// Index points to a newer event (which might have been deleted already). Nothing to do.
			continue
		}
		buf, err := indexKey.Marshal(nil)
		if err != nil {
			return fmt.Errorf("marshal key for index %q: %v", index, err)
		}
		err = txn.Delete(buf)
		if err != nil {
			return fmt.Errorf("delete index %q: %v", index, err)
		}
	}

	// Iterate over each channel and delete any with matching topic and id.
	err = func() error {
		prefix := []byte{data.ChannelTag, data.Sep}
		cursor := prefix

		it := readTxn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			Prefix:         prefix,
		})
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Seek(cursor) {
			var key data.ChannelKey
			err := data.UnmarshalChannelKey(it.Item().Key(), &key)
			if err != nil {
				return fmt.Errorf("unmarshal channel key: %v", err)
			}
			if key.Topic < topic || (key.Topic == topic && key.ID < id) {
				// Find the topic and id for this channel
				key.Topic = topic
				key.ID = id
				cursor, err = key.Marshal(cursor)
				if err != nil {
					return fmt.Errorf("marshal channel key: %v", err)
				}
				continue
			}
			if key.Topic == topic && key.ID == id {
				// We found a match - delete it.
				err = txn.Delete(it.Item().Key())
				if err != nil {
					return err
				}
			}

			// We've done with this channel, skip to the next one.
			cursor = append(cursor[:2], key.Channel...)
			// Append a 1 so the cursor is just after the current channel.
			cursor = append(cursor, 1, data.Sep)
		}

		// Iterate over send counts for each channel and delete any with matching topic and event.
		prefix[0] = data.SendCountTag

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Seek(cursor) {
			var key data.SendCountKey
			err := data.UnmarshalSendCountKey(it.Item().Key(), &key)
			if err != nil {
				return fmt.Errorf("unmarshal channel key: %v", err)
			}
			if key.Topic < topic || (key.Topic == topic && key.ID < id) {
				// Find the topic and id for this channel
				key.Topic = topic
				key.ID = id
				cursor, err = key.Marshal(cursor)
				if err != nil {
					return fmt.Errorf("marshal channel key: %v", err)
				}
				continue
			}
			if key.Topic == topic && key.ID == id {
				// We found a match - delete it.
				err = txn.Delete(it.Item().Key())
				if err != nil {
					return err
				}
			}

			// We've done with this channel, skip to the next one.
			cursor = append(cursor[:2], key.Channel...)
			// Append a 1 so the cursor is just after the current channel.
			cursor = append(cursor, 1, data.Sep)
		}
		return nil
	}()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) listenOut() {
	for e := range s.out {
		// TODO: Move db write code here to implement batch writes
		s.debug.Printf("store: received event %q %q", e.Topic, e.ID)
		s.sharedChannelsMu.Lock()
		for _, shared := range s.sharedChannels {
			if shared.topic == e.Topic {
				select {
				case shared.in <- e:
					s.debug.Printf("store: new event %q %q scheduled on channel %q", e.Topic, e.ID, shared.name)
				default: // Skip if full, listeners can catch up from disk later
					shared.setMissed(true)
					s.debug.Printf("store: new event %q %q missed on channel %q", e.Topic, e.ID, shared.name)
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
			s.debug.Printf("running storage garbage collector...")
			s.db.RunValueLogGC(0.7)
			s.debug.Printf("storage garbage colletion finished")
		}
	}
}

// VerifyEvents verifies all events in the database, optionally deleting any invalid data.
func (s *Store) VerifyEvents(ctx context.Context, deleteInvalid bool) error {
	txn := s.db.NewTransaction(deleteInvalid)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{data.IndexTag}
	err := func() error {
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			item := it.Item()

			delete := func() error {
				key := item.KeyCopy(nil)
				err := txn.Delete(key)
				if err == nil {
					return nil
				}
				if err != badger.ErrTxnTooBig {
					return fmt.Errorf("delete index key %q: %v", item.Key(), err)
				}
				// Transaction is too big - commit it and open a new one.
				it.Close()
				if err := txn.Commit(); err != nil {
					return fmt.Errorf("commit deleted events: %v", err)
				}
				txn = s.db.NewTransaction(true)
				it = txn.NewIterator(opts)
				it.Seek(key)
				if err := txn.Delete(item.Key()); err != nil {
					return fmt.Errorf("delete index key %q: %v", item.Key(), err)
				}
				return nil
			}

			var key data.IndexKey
			err := data.UnmarshalIndexKey(item.Key(), &key)
			if err != nil {
				s.info.Printf("verify events: unmarshal index key %q: %v", item.Key(), err)
				if !deleteInvalid {
					continue
				}
				if err := delete(); err != nil {
					return err
				}
				continue
			}

			var val data.IndexPayload
			err = data.GetIndexPayload(txn, &key, &val)
			if err != nil {
				s.info.Printf("verify events: get index payload for key %v: %v", key, err)
				if !deleteInvalid {
					continue
				}
				if err := delete(); err != nil {
					return err
				}
				continue
			}

			var event data.EventPayload
			eventKey := data.EventKey{
				Topic:      key.Topic,
				CreateTime: time.Unix(0, val.CreateTime),
				ID:         val.EventId,
			}
			err = data.GetEventPayload(txn, &eventKey, &event)
			if err != nil {
				s.info.Printf("verify events: get event for index %v: %v", key, err)
				if !deleteInvalid {
					continue
				}
				if err := delete(); err != nil {
					return err
				}
				continue
			}

			s.debug.Printf("verify events: index %v is valid", key)
		}

		return nil
	}()
	if err != nil {
		return err
	}

	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}
