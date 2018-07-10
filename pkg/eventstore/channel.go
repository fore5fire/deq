package eventstore

//go:generate stringer -type=EventStatus

import (
	"errors"
	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deqd/api/v1/deq"
	"gitlab.com/katcheCode/deqd/pkg/logger"
	"sync"
)

var log = logger.With().Str("pkg", "gitlab.com/katcheCode/deqd/eventstore").Logger()

// Channel allows multiple listeners to synchronize processing of events
type Channel struct {
	name string
	out  chan deq.Event
	done chan error
	err  error
	db   *badger.DB
}

type sharedChannel struct {
	sync.Mutex
	in        chan deq.Event
	out       chan deq.Event
	done      chan error
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
			in:  make(chan deq.Event, 20),
			out: make(chan deq.Event, 20),
			db:  s.db,
		}
		s.sharedChannelsMu.Lock()
		s.sharedChannels[name] = shared
		s.sharedChannelsMu.Unlock()

		go shared.start(name)
	}

	// DON'T FORGET TO ADD CHECK FOR FAILED CHANNEL

	done := make(chan error, 1)
	shared.Lock()
	defer shared.Unlock()
	shared.doneChans = append(shared.doneChans, done)

	return Channel{
		name: name,
		out:  shared.out,
		done: done,
		db:   s.db,
	}
}

// ChannelSettings is the settings for a channel
type ChannelSettings struct {
	// objectID     []byte
}

// ChannelSettingsDefaults is the default settings for a channel
var ChannelSettingsDefaults = ChannelSettings{}

// Follow returns
func (c Channel) Follow() (eventc chan deq.Event, done chan struct{}) {

	done = make(chan struct{})
	// go func() {
	// 	<-done
	// }()

	return c.out, done
}

// Err returns the error that caused this channel to fail, or nil if the channel closed cleanly
func (c Channel) Err() error {
	return c.err
}

// SetEventStatus sets the status of an event for this channel
func (c Channel) SetEventStatus(key []byte, status EventStatus) error {
	txn := c.db.NewTransaction(true)
	defer txn.Discard()

	_, err := txn.Get(append(eventPrefix, key...))
	if err == badger.ErrKeyNotFound {
		return ErrKeyNotFound
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

// EventStatus is the processing state of an event on a particular channel
type EventStatus int

// An event of type EventStatusPending will cause deqd to requeue the event after waiting the channel's event_timeout_miliseconds setting.
// EventStatusProcessed and EventStatusWillNotProcess have the same behavior for now.
const (
	EventStatusPending EventStatus = iota
	EventStatusProcessed
	EventStatusWillNotProcess
)

// Settings provides the current settings of a channel.
func (c Channel) Settings() (ChannelSettings, error) {
	settings := ChannelSettingsDefaults

	// txn := c.db.NewTransaction(false)
	// defer txn.Discard()
	//
	// item, err := txn.Get(append(channelPrefix, c.name...))
	// if err != nil {
	// 	return ChannelSettings{}, err
	// }
	//
	// data, err := item.Value()
	// if err != nil {
	// 	return ChannelSettings{}, err
	// }
	//
	// proto.Unmarshal(data, &settings)
	//
	// if err != nil {
	// 	return ChannelSettings{}, err
	// }

	return settings, nil
}

// SetSettings returns the current channel settings.
// An error is returned if a database error occured or the channel has not been created.
func (c Channel) SetSettings() error {

	// settings := ChannelSettingsDefaults
	//
	// data, err := proto.Marshal(&settings)
	// if err != nil {
	// 	return err
	// }
	//
	// txn := c.db.NewTransaction(true)
	// defer txn.Discard()
	// err = txn.Set(append(channelPrefix, c.name...), data)
	// if err != nil {
	// 	return err
	// }
	//
	// err = txn.Commit(nil)
	// if err != nil {
	// 	return err
	// }
	// return nil

	return errors.New("SetChannelSettings is not impelmented")
}

// RequeueEvent adds the event back into the event queue for this channel
func (c *Channel) RequeueEvent(e deq.Event) {
	log.Debug().Interface("event", e).Msg("Requeuing event")
	c.out <- e
}

func (s *sharedChannel) start(channelName string) {

	cursor, err := s.getCursor(channelName)
	if err != nil {
		s.broadcastErr(err)
		return
	}

	current := append(eventPrefix, cursor...)

	for {
		current, err = s.catchUp(append(current, "\uffff"...))
		if err != nil {
			s.broadcastErr(err)
		}

		for len(s.in) < cap(s.in) {
			// No event overflow since last read, we're all caught up
			e := <-s.in
			s.out <- e
			current = e.Key
		}

		// We might have missed an event, lets go back to reading from disk
		// First let's drain some events so we can tell if we've missed any more
		for i := 0; i < len(s.in); i++ {
			<-s.in
		}
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

	//
	// settings, err := s.ChannelSettings(channelName)
	// if err != nil {
	// 	c.err = err
	// 	return
	// }

	var event deq.Event
	var lastKey []byte

	for it.Seek(append(eventPrefix, cursor...)); it.ValidForPrefix(eventPrefix); it.Next() {

		item := it.Item()
		lastKey = item.Key()
		buffer, err := item.Value()

		if err != nil {
			return nil, err
		}

		err = proto.Unmarshal(buffer, &event)
		if err != nil {
			return nil, err
		}

		s.out <- event
	}

	return append(cursor[:0], lastKey...), nil
}

func (s *sharedChannel) getCursor(channelName string) ([]byte, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get(append(cursorPrefix, channelName...))
	if err == badger.ErrKeyNotFound {
		return eventPrefix, nil
	}
	if err != nil {
		return nil, err
	}

	cursor, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	return cursor, nil
}

func (s *sharedChannel) broadcastErr(err error) {
	panic(err)
	// for _, donec := range s.doneChans {
	// 	donec <- err
	// }
}
