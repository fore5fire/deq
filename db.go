package deq

import (
	"fmt"
	"hash/crc32"
	"log"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq/internal/data"
)

var defaultChannelState = data.ChannelPayload{
	EventState: data.EventState_QUEUED,
}

func getEvent(txn *badger.Txn, topic, eventID, channel string) (*Event, error) {
	eventTime, err := getEventTimePayload(txn, data.EventTimeKey{
		ID:    eventID,
		Topic: topic,
	})
	if err == ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get event time: %v", err)
	}

	event, err := getEventPayload(txn, data.EventKey{
		ID:         eventID,
		Topic:      topic,
		CreateTime: time.Unix(0, eventTime.CreateTime),
	})
	if err != nil {
		return nil, fmt.Errorf("get event payload: %v", err)
	}

	channelState := defaultChannelState

	if channel != "" {
		channelState, err = getChannelEvent(txn, data.ChannelKey{
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

func printKeys(txn *badger.Txn) {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		if item.IsDeletedOrExpired() {
			continue
		}

		key, err := data.Unmarshal(item.Key())
		if err != nil {
			log.Printf("%v %v", item.Key(), err)
			continue
		}
		log.Printf("%+v", key)
	}
}

func writeEvent(txn *badger.Txn, e *Event) error {
	key, err := data.EventTimeKey{
		Topic: e.Topic,
		ID:    e.ID,
	}.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal event time key: %v", err)
	}

	_, err = txn.Get(key)
	if err == nil {
		return ErrAlreadyExists
	}
	if err != badger.ErrKeyNotFound {
		return fmt.Errorf("check event doesn't exist: %v", err)
	}

	val, err := proto.Marshal(&data.EventTimePayload{
		CreateTime: e.CreateTime.UnixNano(),
	})
	if err != nil {
		return fmt.Errorf("marshal event time payload: %v", err)
	}

	err = txn.Set(key, val)
	if err != nil {
		return err
	}

	key, err = data.EventKey{
		Topic:      e.Topic,
		CreateTime: e.CreateTime,
		ID:         e.ID,
	}.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal event time key: %v", err)
	}

	val, err = proto.Marshal(&data.EventPayload{
		Payload:           e.Payload,
		DefaultEventState: e.DefaultState.toProto(),
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
		indexKey := data.IndexKey{
			Topic: e.Topic,
			Value: index,
		}

		// Check if index is in use. Only overwrite if newer, or if create time is the same if the event
		// id hash is greater.
		var existing data.IndexPayload
		err := getIndexPayload(txn, indexKey, &existing)
		if err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("lookup existing index: %v", err)
		}
		if err == nil && !shouldUpdateIndex(&existing, e) {
			continue
		}

		err = writeIndex(txn, indexKey, &data.IndexPayload{
			EventId:    e.ID,
			CreateTime: e.CreateTime.UnixNano(),
		})
		if err != nil {
			return err
		}
	}

	if e.DefaultState != EventStateUnspecified && e.DefaultState != EventStateQueued {

		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte{data.ChannelTag, data.Sep}
		cursor := prefix

		for it.Seek(cursor); it.ValidForPrefix(prefix); it.Seek(cursor) {

			var key data.ChannelKey
			err := data.UnmarshalChannelKey(it.Item().Key(), &key)
			if err != nil {
				return fmt.Errorf("unmarshal channel key: %v", err)
			}

			// Skip to next channel
			cursor, err = data.ChannelPrefix(key.Channel + "\u0001")
			if err != nil {
				return fmt.Errorf("marshal channel prefix: %v", err)
			}

			newKey := data.ChannelKey{
				Topic:   e.Topic,
				Channel: key.Channel,
				ID:      e.ID,
			}

			err = setChannelEvent(txn, newKey, data.ChannelPayload{
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

func shouldUpdateIndex(existing *data.IndexPayload, candidate *Event) bool {
	createTime := candidate.CreateTime.UnixNano()

	// Use the event with the later create time.
	if existing.CreateTime != createTime {
		return existing.CreateTime < createTime
	}

	// If the create times are equal, use the one with the higher crc32 hash.
	hashExisting := crc32.ChecksumIEEE([]byte(existing.EventId))
	hashCandidate := crc32.ChecksumIEEE([]byte(candidate.ID))
	if hashExisting != hashCandidate {
		return hashExisting < hashCandidate
	}

	// If there's a hash collision, just use the event with the highest ID.
	return existing.EventId < candidate.ID
}

func getIndexPayload(txn *badger.Txn, key data.IndexKey, dst *data.IndexPayload) error {
	rawkey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal index: %v", err)
	}
	item, err := txn.Get(rawkey)
	if err != nil {
		return err
	}
	buf, err := item.Value()
	if err != nil {
		return err
	}
	err = proto.Unmarshal(buf, dst)
	if err != nil {
		return fmt.Errorf("unmarshal payload: %v", err)
	}
	return nil
}

func writeIndex(txn *badger.Txn, key data.IndexKey, payload *data.IndexPayload) error {
	rawkey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal index: %v", err)
	}
	buf, err := proto.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %v", err)
	}
	err = txn.Set(rawkey, buf)
	if err != nil {
		return err
	}

	return nil
}

func setChannelEvent(txn *badger.Txn, key data.ChannelKey, payload data.ChannelPayload) error {

	rawkey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal key: %v", err)
	}
	buf, err := proto.Marshal(&payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %v", err)
	}

	err = txn.Set(rawkey, buf)
	if err != nil {
		return err
	}

	return nil
}

func getEventTimePayload(txn *badger.Txn, key data.EventTimeKey) (payload data.EventTimePayload, err error) {
	rawKey, err := key.Marshal(nil)
	if err != nil {
		return payload, fmt.Errorf("marshal event time key: %v", err)
	}
	item, err := txn.Get(rawKey)
	if err == badger.ErrKeyNotFound {
		return payload, ErrNotFound
	}
	if err != nil {
		return payload, err
	}
	val, err := item.Value()
	if err != nil {
		return payload, err
	}

	err = proto.Unmarshal(val, &payload)
	if err != nil {
		return data.EventTimePayload{}, fmt.Errorf("unmarshal event time payload: %v", err)
	}

	return payload, nil
}

func getEventPayload(txn *badger.Txn, key data.EventKey) (payload data.EventPayload, err error) {
	rawKey, err := key.Marshal(nil)
	if err != nil {
		return payload, fmt.Errorf("marshal event key: %v", err)
	}
	item, err := txn.Get(rawKey)
	if err != nil {
		return payload, err
	}
	val, err := item.Value()
	if err != nil {
		return payload, err
	}

	err = proto.Unmarshal(val, &payload)
	if err != nil {
		return data.EventPayload{}, fmt.Errorf("unmarshal event payload: %v", err)
	}

	return payload, nil
}

var defaultChannelPayload = data.ChannelPayload{
	EventState: EventStateQueued.toProto(),
}

func getChannelEvent(txn *badger.Txn, key data.ChannelKey) (data.ChannelPayload, error) {

	rawKey, err := key.Marshal(nil)
	if err != nil {
		return data.ChannelPayload{}, fmt.Errorf("marshal event key: %v", err)
	}

	item, err := txn.Get(rawKey)
	if err == badger.ErrKeyNotFound {
		return defaultChannelPayload, nil
	}
	if err != nil {
		return data.ChannelPayload{}, err
	}
	// Not found isn't an error - it just means we need to use the default state
	val, err := item.Value()
	if err != nil {
		return data.ChannelPayload{}, err
	}
	var channelState data.ChannelPayload
	err = proto.Unmarshal(val, &channelState)
	if err != nil {
		return data.ChannelPayload{}, fmt.Errorf("unmarshal channel payload: %v", err)
	}

	return channelState, nil
}

func (e EventState) toProto() data.EventState {
	switch e {
	case EventStateUnspecified:
		return data.EventState_UNSPECIFIED_STATE
	case EventStateQueued:
		return data.EventState_QUEUED
	case EventStateDequeuedOK:
		return data.EventState_DEQUEUED_OK
	case EventStateDequeuedError:
		return data.EventState_DEQUEUED_ERROR
	default:
		panic("unrecognized EventState")
	}
}

func protoToEventState(e data.EventState) EventState {
	switch e {
	case data.EventState_UNSPECIFIED_STATE:
		return EventStateUnspecified
	case data.EventState_QUEUED:
		return EventStateQueued
	case data.EventState_DEQUEUED_OK:
		return EventStateDequeuedOK
	case data.EventState_DEQUEUED_ERROR:
		return EventStateDequeuedError
	default:
		panic("unrecognized EventState")
	}
}

// TODO: just use requeue limit on event itself once implemented?
func incrementSavedRequeueCount(txn *badger.Txn, channel, topic string, defaultRequeueLimit int, e *Event) (*data.ChannelPayload, error) {

	key := data.ChannelKey{
		Channel: channel,
		Topic:   topic,
		ID:      e.ID,
	}

	channelEvent, err := getChannelEvent(txn, key)
	if err != nil {
		return nil, err
	}

	if defaultRequeueLimit == -1 || int(channelEvent.RequeueCount) < 40 {
		channelEvent.RequeueCount++
	} else {
		channelEvent.EventState = data.EventState_DEQUEUED_ERROR
	}

	err = setChannelEvent(txn, key, channelEvent)
	if err != nil {
		return nil, err
	}

	return &channelEvent, nil
}
