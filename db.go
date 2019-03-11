package deq

import (
	"fmt"
	"log"
	"time"

	"github.com/dgraph-io/badger"
	"gitlab.com/katcheCode/deq/internal/eventdb"
)

type eventDB struct {
	eventdb.DB
}

type eventTxn struct {
	eventdb.Txn
}

func (db *eventDB) NewTransaction(update bool) _Txn {
	return &eventTxn{db.DB.NewTransaction(update)}
}

func (t *eventTxn) Commit() error {
	return t.Txn.Commit()
}

type eventIter struct {
	*eventdb.EventIter
}

func (t *eventTxn) NewEventIter(opts badger.IteratorOptions) _EventIter {
	return &eventIter{t.Txn.NewEventIter(opts)}
}

type eventTimeIter struct {
	*eventdb.EventTimeIter
}

func (t *eventTxn) NewEventTimeIter(opts badger.IteratorOptions) _EventTimeIter {
	return &eventTimeIter{t.Txn.NewEventTimeIter(opts)}
}

type channelIter struct {
	*eventdb.ChannelIter
}

func (t *eventTxn) NewChannelIter(opts badger.IteratorOptions) _ChannelIter {
	return &channelIter{t.Txn.NewChannelIter(opts)}
}

type indexIter struct {
	*eventdb.IndexIter
}

func (t *eventTxn) NewIndexIter(opts badger.IteratorOptions) _IndexIter {
	return &indexIter{t.Txn.NewIndexIter(opts)}
}

type _DB interface {
	NewTransaction(bool) _Txn
}

type _Txn interface {
	SetChannelEvent(key eventdb.ChannelKey, payload eventdb.ChannelPayload) error
	GetEventTimePayload(key eventdb.EventTimeKey) (payload eventdb.EventTimePayload, err error)
	GetEventPayload(key eventdb.EventKey) (payload eventdb.EventPayload, err error)
	GetChannelEvent(key eventdb.ChannelKey) (eventdb.ChannelPayload, error)
	NewEventIter(badger.IteratorOptions) _EventIter
	NewEventTimeIter(badger.IteratorOptions) _EventTimeIter
	NewChannelIter(badger.IteratorOptions) _ChannelIter
	NewIndexIter(badger.IteratorOptions) _IndexIter
	Discard()
	Commit() error
}

type _Iter interface {
	Rewind()
	Next()
	Valid() bool
	Close()
}

type _IndexIter interface {
	_Iter
	Seek(key eventdb.IndexKey) error
	ValidForTopic(topic string) bool
	Key(dst *eventdb.IndexKey) error
}

type _ChannelIter interface {
	_Iter
	Seek(key eventdb.ChannelKey) error
	SeekChannel(channel string) error
	ValidForChannel(channel string) bool
	ChannelPayload(dst *eventdb.ChannelPayload) error
	Key(dst *eventdb.ChannelKey) error
}

type _EventTimeIter interface {
	_Iter
	Seek(key eventdb.EventTimeKey) error
	ValidForTopic(topic string) bool
	EventTimePayload(dst *eventdb.EventTimePayload) error
	Key(dst *eventdb.EventTimeKey) error
}

type _EventIter interface {
	_Iter
	Seek(key eventdb.EventKey) error
	ValidForTopic(topic string) bool
	EventPayload(dst *eventdb.EventPayload) error
	Key(dst *eventdb.EventKey) error
}

var defaultChannelState = eventdb.ChannelPayload{
	EventState: eventdb.EventState_QUEUED,
}

func getEvent(txn _Txn, topic, eventID, channel string) (*Event, error) {
	eventTime, err := txn.GetEventTimePayload(eventdb.EventTimeKey{
		ID:    eventID,
		Topic: topic,
	})
	if err == eventdb.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get event time: %v", err)
	}

	event, err := txn.GetEventPayload(eventdb.EventKey{
		ID:         eventID,
		Topic:      topic,
		CreateTime: time.Unix(0, eventTime.CreateTime),
	})
	if err != nil {
		return nil, fmt.Errorf("get event payload: %v", err)
	}

	channelState := defaultChannelState

	if channel != "" {
		channelState, err = txn.GetChannelEvent(eventdb.ChannelKey{
			ID:      eventID,
			Topic:   topic,
			Channel: channel,
		})
		if err != nil {
			return nil, fmt.Errorf("get event state: %v", err)
		}
	}

	return &Event{
		ID:           eventID,
		Topic:        topic,
		Payload:      event.Payload,
		CreateTime:   time.Unix(0, eventTime.CreateTime),
		RequeueCount: int(channelState.RequeueCount),
		State:        protoToEventState(channelState.EventState),
		DefaultState: protoToEventState(event.DefaultEventState),
		Indexes:      event.Indexes,
	}, nil
}

func writeEvent(txn eventdb.Txn, e *Event) error {
	key := eventdb.EventTimeKey{
		Topic: e.Topic,
		ID:    e.ID,
	}

	_, err := txn.GetEventTimePayload(key)
	if err == nil {
		return ErrAlreadyExists
	}
	if err != badger.ErrKeyNotFound {
		return fmt.Errorf("check event doesn't exist: %v", err)
	}

	err = txn.SetEventTimePayload(key, &eventdb.EventTimePayload{
		CreateTime: e.CreateTime.UnixNano(),
	})
	if err != nil {
		return fmt.Errorf("set event time payload: %v", err)
	}

	err = txn.SetEventPayload(eventdb.EventKey{
		Topic:      e.Topic,
		CreateTime: e.CreateTime,
		ID:         e.ID,
	}, &eventdb.EventPayload{
		Payload:           e.Payload,
		DefaultEventState: e.DefaultState.toProto(),
		Indexes:           e.Indexes,
	})
	if err != nil {
		return fmt.Errorf("marshal event time payload: %v", err)
	}

	for _, index := range e.Indexes {
		err := txn.SetIndex(&eventdb.IndexKey{
			Topic: e.Topic,
			Value: index,
			ID:    e.ID,
		})
		if err != nil {
			return fmt.Errorf("write index: %v", err)
		}
	}

	if e.DefaultState != EventStateUnspecified && e.DefaultState != EventStateQueued {

		it := txn.NewChannelIter(badger.DefaultIteratorOptions)
		defer it.Close()

		var cursor string

		for it.Rewind(); it.Valid(); err = it.SeekChannel(cursor) {
			if err != nil {
				log.Printf("seek channel: %v", err)
				continue
			}

			var key eventdb.ChannelKey
			err := it.Key(&key)
			if err != nil {
				return fmt.Errorf("unmarshal channel key: %v", err)
			}

			// Skip to next channel
			cursor = key.Channel + "\u0001"

			err = txn.SetChannelEvent(eventdb.ChannelKey{
				Topic:   e.Topic,
				Channel: key.Channel,
				ID:      e.ID,
			}, eventdb.ChannelPayload{
				EventState:   e.DefaultState.toProto(),
				RequeueCount: int32(e.RequeueCount),
			})
			if err != nil {
				return fmt.Errorf("set event state on channel %s: %v", key.Channel, err)
			}
		}
	}

	return nil
}

func (e EventState) toProto() eventdb.EventState {
	switch e {
	case EventStateUnspecified:
		return eventdb.EventState_UNSPECIFIED_STATE
	case EventStateQueued:
		return eventdb.EventState_QUEUED
	case EventStateDequeuedOK:
		return eventdb.EventState_DEQUEUED_OK
	case EventStateDequeuedError:
		return eventdb.EventState_DEQUEUED_ERROR
	default:
		panic("unrecognized EventState")
	}
}

func protoToEventState(e eventdb.EventState) EventState {
	switch e {
	case eventdb.EventState_UNSPECIFIED_STATE:
		return EventStateUnspecified
	case eventdb.EventState_QUEUED:
		return EventStateQueued
	case eventdb.EventState_DEQUEUED_OK:
		return EventStateDequeuedOK
	case eventdb.EventState_DEQUEUED_ERROR:
		return EventStateDequeuedError
	default:
		panic("unrecognized EventState")
	}
}
