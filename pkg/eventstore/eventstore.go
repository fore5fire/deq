package eventstore

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"

	// "github.com/satori/go.uuid"

	"sync"

	"gitlab.com/katcheCode/deqd/api/v1/deq"
)

// Store is an EventStore connected to a specific database
type Store struct {
	db               *badger.DB
	in               chan eventPromise
	out              chan *deq.Event
	sharedChannelsMu sync.RWMutex
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
	}

	go s.listenIn()
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
func (s *Store) Pub(e *deq.Event) error {

	if e == nil {
		return errors.New("event is nil")
	}

	done := make(chan error, 1)

	s.in <- eventPromise{
		event: e,
		done:  done,
	}

	return <-done
}

// listenIn listens to the store's in chan. It writes recieved events to disc,
// then sends them to the store's out chan.
// TODO: batch writes in a single transaction or spin off goroutines
func (s *Store) listenIn() {
	var buffer []byte
	var err error
	// var txn *badger.Txn
	for promise := range s.in {

		id := promise.event.Id
		topic := promise.event.Topic

		// It's a waste of space to include these in the value because they're
		// already in the key
		promise.event.Id = ""
		promise.event.Topic = ""

		size := promise.event.Size()
		if len(buffer) < size {
			buffer = make([]byte, size)
		}

		size, err = promise.event.MarshalTo(buffer)
		if err != nil {
			promise.done <- fmt.Errorf("marshal event: %v", err)
			continue
		}

		// Put the key-stored values back for in-memory processing of the event
		promise.event.Id = id
		promise.event.Topic = topic

		txn := s.db.NewTransaction(true)
		defer txn.Discard()

		key := eventKey(topic, id)

		_, err := txn.Get(key)
		if err == nil {
			promise.done <- ErrAlreadyExists
			continue
		}
		if err != badger.ErrKeyNotFound {
			promise.done <- fmt.Errorf("check event doesn't exist: %v", err)
			continue
		}

		err = txn.Set(key, buffer[:size])
		if err != nil {
			promise.done <- err
			continue
		}

		err = txn.Commit(nil)
		if err != nil {
			promise.done <- err
			continue
		}
		out := *promise.event
		s.out <- &out
		close(promise.done)
	}
}

func (s *Store) listenOut() {
	for e := range s.out {
		s.sharedChannelsMu.RLock()
		for _, shared := range s.sharedChannels {
			// TODO: organize sharedChannels so we can skip over other topics completely
			if shared.topic == url.QueryEscape(e.Topic) {
				select {
				case shared.in <- e:
					// TODO: Move db write code here
				default: // Skip if full, listeners can catch up from disk later
				}
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
	// ErrNotFound is returned when a requested event doesn't exist in the store
	ErrNotFound = errors.New("event not found")
	// ErrInternal is returned when an interanl error occurs
	ErrInternal = errors.New("internal error")
	// ErrAlreadyExists is returned when creating an event with a key that is in use
	ErrAlreadyExists = errors.New("already exists")
)

const (
	channelPrefix = "C"
	cursorPrefix  = "c"
	eventPrefix   = "E"
	dbVersionKey  = "___DEQ_DB_VERSION___"
	dbCodeVersion = "1.0.0"
)

// UpgradeDB upgrades the store's db to the current version.
// It is not safe to update the database concurrently with UpgradeDB.
// If any error is encountered, no changes will be made.
func (s *Store) UpgradeDB() error {

	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	currentVersion, err := s.getDBVersion(txn)
	if err != nil {
		return err
	}

	log.Printf("[INFO] current DB version is %s", currentVersion)

	if currentVersion == "0" {
		log.Printf("[INFO] upgrading db...")

		upgradeV0EventsToV1(txn)

		err := txn.Set([]byte(dbVersionKey), []byte(dbCodeVersion))
		if err != nil {
			return err
		}

		err = txn.Commit(nil)
		if err != nil {
			return fmt.Errorf("commit db upgrade: %v", err)
		}

		log.Printf("db upgraded to version %s", dbCodeVersion)
	}

	return nil
}

func (s *Store) getDBVersion(txn *badger.Txn) (string, error) {
	item, err := txn.Get([]byte(dbVersionKey))
	if err == badger.ErrKeyNotFound {
		return "0", nil
	}
	if err != nil {
		return "", err
	}

	version, err := item.ValueCopy(nil)
	if err != nil {
		return "", err
	}

	return string(version), nil
}

// upgradeV0EventsToV1 upgrades all events from v0 to v1 without commiting the txn.
func upgradeV0EventsToV1(txn *badger.Txn) error {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	prefix := []byte(eventPrefix + "/")

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		buffer, err := item.Value()
		if err != nil {
			return fmt.Errorf("get item value: %v", err)
		}

		event := new(deq.EventV0)
		err = proto.Unmarshal(buffer, event)
		if err != nil {
			return fmt.Errorf("unmarshal v0 formatted event: %v", err)
		}

		topic := url.QueryEscape(strings.TrimPrefix(event.Payload.GetTypeUrl(), "types.googleapis.com/"))
		id := url.QueryEscape(event.Key)

		err = txn.Delete(item.Key())
		if err != nil {
			return fmt.Errorf("delete v0 event: %v", err)
		}
		err = txn.Set(eventKey(topic, id), event.Payload.GetValue())
		if err != nil {
			return fmt.Errorf("add v1 event: %v", err)
		}
	}

	return nil
}

func eventKey(topic, id string) []byte {
	return []byte(eventPrefix + "/" + url.QueryEscape(topic) + "/" + url.QueryEscape(id))
}

func parseEventKey(key []byte) (topic, id string, err error) {
	parsed := strings.Split(string(key), "/")
	if len(parsed) != 3 {
		return "", "", fmt.Errorf("incorrect segment count")
	}
	id, err = url.QueryUnescape(parsed[2])
	if err != nil {
		return "", "", fmt.Errorf("url unescape: %v", err)
	}
	topic, err = url.QueryUnescape(parsed[1])
	if err != nil {
		return "", "", fmt.Errorf("url unescape: %v", err)
	}
	return topic, id, nil
}

func eventStateKey(channel, topic, id string) []byte {
	return []byte(channelPrefix + "/" + url.QueryEscape(channel) + "/" + url.QueryEscape(topic) + "/" + url.QueryEscape(id))
}
