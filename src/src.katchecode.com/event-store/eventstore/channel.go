package eventstore

import (
	"errors"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	"src.katchecode.com/event-store/api/v1/eventstore"
	"sync"
	"time"
)

// Channel allows multiple listeners to synchronize processing of events
type Channel struct {
	Eventc    chan *eventstore.Event
	waitGroup sync.WaitGroup
	err       error
	errLock   sync.Mutex
}

// ChannelSettings is the settings for a channel
type ChannelSettings struct {
	MaxID        string
	MinID        string
	RequeueDelay time.Duration
}

// ChannelSettingsDefaults is the default settings for a channel
var ChannelSettingsDefaults = ChannelSettings{
	MaxID:        "",
	MinID:        "",
	RequeueDelay: 8000 * time.Millisecond,
}

// Follow returns
func (s *Store) Follow(channelName string) (channel *Channel, done chan struct{}) {

	s.openChannelsLock.Lock()
	c, ok := s.openChannels[channelName]
	s.openChannelsLock.Unlock()
	if ok {
		done = c.addFollower()
		return c, done
	}

	c = &Channel{
		Eventc: make(chan *eventstore.Event, 20),
	}
	defer close(c.Eventc)

	allDone := make(chan struct{})

	go func() {
		txn := s.db.NewTransaction(false)
		defer txn.Discard()

		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		var buffer []byte

		settings, err := s.ChannelSettings(channelName)
		if err != nil {
			c.errLock.Lock()
			c.err = err
			c.errLock.Unlock()
			return
		}

		for it.Seek([]byte(string(eventPrefix))); it.ValidForPrefix(eventPrefix); it.Next() {

			item := it.Item()
			buffer, err := item.ValueCopy(buffer)

			if err != nil {
				c.errLock.Lock()
				c.err = err
				c.errLock.Unlock()
				return
			}

			event := &eventstore.Event{}
			err = proto.Unmarshal(buffer, event)
			if err != nil {
				c.errLock.Lock()
				c.err = err
				c.errLock.Unlock()
				return
			}

			select {
			case c.Eventc <- event:
			case <-allDone:
				return
			}
		}

	}()

	done = c.addFollower()
	go func() {
		c.waitGroup.Wait()
		close(allDone)
		s.openChannelsLock.Lock()
		delete(s.openChannels, channelName)
		s.openChannelsLock.Unlock()
	}()
	return c, done
}

func (c *Channel) addFollower() (donec chan struct{}) {
	donec = make(chan struct{})

	c.waitGroup.Add(1)
	go func() {
		for range donec {
		}
		c.waitGroup.Done()
	}()

	return donec
}

// Err returns the error that caused this channel to fail, or nil if the channel closed cleanly
func (c *Channel) Err() error {
	c.errLock.Lock()
	defer c.errLock.Unlock()
	return c.err
}

// ChannelSettings provides the current settings of a channel.
func (s *Store) ChannelSettings(channelName string) (ChannelSettings, error) {
	settings := ChannelSettingsDefaults

	// err = s.db.View(func(txn *badger.Txn) error {
	//
	// 	item, err := txn.Get([]byte(metaKey))
	// 	if err != nil {
	// 		return err
	// 	}
	//
	// 	data, err := item.Value()
	// 	if err != nil {
	// 		return err
	// 	}
	//
	// 	proto.Unmarshal(data, &meta)
	// 	return nil
	// })
	//
	// if err != nil {
	// 	return eventstore.StoreMeta{}, err
	// }

	return settings, nil
}

// SetChannelSettings returns the current channel settings.
// An error is returned if a database error occured or the channel has not been created.
func (s *Store) SetChannelSettings(channelName string) error {

	// meta := eventstore.StoreMeta{
	// 	StoreId: uuid.NewV4().String(),
	// }
	//
	// data, err := proto.Marshal(&meta)
	// if err != nil {
	// 	return err
	// }
	//
	// err = s.db.Update(func(txn *badger.Txn) error {
	// 	txn.Set(metaKey, data)
	// 	return nil
	// })
	// if err != nil {
	// 	return err
	// }
	//
	// return nil

	return errors.New("SetChannelSettings is not impelmented")
}
