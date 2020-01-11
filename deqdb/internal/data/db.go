package data

import (
	"fmt"
	"reflect"
	"time"

	"gitlab.com/katcheCode/deq"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
)

type DB interface {
	NewTransaction(update bool) Txn
	RunValueLogGC(float64) error
	Close() error
}

type Txn interface {
	Discard()
	Get(key []byte) (Item, error)
	Set(key, val []byte) error
	Delete(key []byte) error
	NewIterator(badger.IteratorOptions) Iter
	Commit() error
}

type Iter interface {
	Close()
	Item() Item
	Next()
	Rewind()
	Seek(key []byte)
	Valid() bool
	ValidForPrefix(prefix []byte) bool
}

type Item interface {
	Key() []byte
	KeyCopy(dst []byte) []byte
	Value(func([]byte) error) error
	ValueCopy(dst []byte) ([]byte, error)
}

var defaultChannelState = ChannelPayload{
	EventState: EventState_QUEUED,
}

// TODO: prevent writing empty indexes.

// GetEvent gets an event from the database by ID.
//
// If the event does not exist in the database, GetEvent returns deq.ErrNotFound
func GetEvent(txn Txn, topic, eventID, channel string) (*deq.Event, error) {
	var eventTime EventTimePayload
	err := GetEventTimePayload(txn, &EventTimeKey{
		ID:    eventID,
		Topic: topic,
	}, &eventTime)
	if err == deq.ErrNotFound {
		return nil, deq.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get event time: %v", err)
	}

	var event EventPayload
	err = GetEventPayload(txn, &EventKey{
		ID:         eventID,
		Topic:      topic,
		CreateTime: time.Unix(0, eventTime.CreateTime),
	}, &event)
	if err != nil {
		return nil, fmt.Errorf("get event payload: %v", err)
	}

	channelState := ChannelPayload{
		EventState: event.DefaultEventState,
	}
	var sendCount SendCount

	if channel != "" {
		err = GetChannelEvent(txn, &ChannelKey{
			ID:      eventID,
			Topic:   topic,
			Channel: channel,
		}, &channelState)
		if err != nil && err != deq.ErrNotFound {
			return nil, fmt.Errorf("get event state: %v", err)
		}

		err = GetSendCount(txn, &SendCountKey{
			ID:      eventID,
			Topic:   topic,
			Channel: channel,
		}, &sendCount)
		if err != nil && err != deq.ErrNotFound {
			return nil, fmt.Errorf("get send count: %v", err)
		}
	}

	return &deq.Event{
		ID:           eventID,
		Topic:        topic,
		Payload:      event.Payload,
		CreateTime:   time.Unix(0, eventTime.CreateTime),
		SendCount:    int(sendCount.SendCount),
		State:        EventStateFromProto(channelState.EventState),
		DefaultState: EventStateFromProto(event.DefaultEventState),
		Indexes:      event.Indexes,
	}, nil
}

// PrintKeys prints all keys found in the database to stdout. Intended of debugging only
func PrintKeys(txn Txn) {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key, err := Unmarshal(item.Key())
		if err != nil {
			fmt.Printf("%v %v\n", item.Key(), err)
			continue
		}
		fmt.Printf("%v: %+v\n", reflect.TypeOf(key), key)
	}
}

// WriteEvent writes an event to the database.
func WriteEvent(txn Txn, e *deq.Event) error {

	if e.DefaultState == deq.StateUnspecified {
		e.DefaultState = deq.StateQueued
	}

	key, err := EventTimeKey{
		Topic: e.Topic,
		ID:    e.ID,
	}.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal event time key: %v", err)
	}

	_, err = txn.Get(key)
	if err == nil {
		return deq.ErrAlreadyExists
	}
	if err != badger.ErrKeyNotFound {
		return fmt.Errorf("check event doesn't exist: %v", err)
	}

	val, err := proto.Marshal(&EventTimePayload{
		CreateTime: e.CreateTime.UnixNano(),
	})
	if err != nil {
		return fmt.Errorf("marshal event time payload: %v", err)
	}

	err = txn.Set(key, val)
	if err != nil {
		return err
	}

	key, err = EventKey{
		Topic:      e.Topic,
		CreateTime: e.CreateTime,
		ID:         e.ID,
	}.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal event key: %v", err)
	}

	val, err = proto.Marshal(&EventPayload{
		Payload:           e.Payload,
		DefaultEventState: EventStateToProto(e.DefaultState),
		Indexes:           e.Indexes,
	})
	if err != nil {
		return fmt.Errorf("marshal event time payload: %v", err)
	}

	err = txn.Set(key, val)
	if err != nil {
		return err
	}

	for _, index := range e.Indexes {
		indexKey := IndexKey{
			Topic:      e.Topic,
			Value:      index,
			CreateTime: e.CreateTime,
			ID:         e.ID,
		}
		// log.Printf("[DEBUG] WriteEvent %d: write index %d: using key %+v", i, indexKey)

		index := IndexPayload{
			// EventId:    e.ID,
			// CreateTime: e.CreateTime.UnixNano(),
		}

		err = WriteIndex(txn, &indexKey, &index)
		if err != nil {
			return err
		}
	}

	// if e.DefaultState != deq.StateQueued {

	// 	it := txn.NewIterator(badger.DefaultIteratorOptions)
	// 	defer it.Close()

	// 	prefix := []byte{ChannelTag, Sep}
	// 	cursor := prefix

	// 	for it.Seek(cursor); it.ValidForPrefix(prefix); it.Seek(cursor) {

	// 		var key ChannelKey
	// 		err := UnmarshalChannelKey(it.Item().Key(), &key)
	// 		if err != nil {
	// 			return fmt.Errorf("unmarshal channel key: %v", err)
	// 		}

	// 		// Skip to next channel
	// 		cursor, err = ChannelPrefix(key.Channel + "\u0001")
	// 		if err != nil {
	// 			return fmt.Errorf("marshal channel prefix: %v", err)
	// 		}

	// 		newKey := ChannelKey{
	// 			Topic:   e.Topic,
	// 			Channel: key.Channel,
	// 			ID:      e.ID,
	// 		}

	// 		err = setChannelEvent(txn, newKey, ChannelPayload{
	// 			EventState:   eventStateToProto(e.DefaultState),
	// 			RequeueCount: int32(e.RequeueCount),
	// 		})
	// 		if err != nil {
	// 			return fmt.Errorf("set event state on channel %s: %v", key.Channel, err)
	// 		}
	// 	}
	// }

	return nil
}
