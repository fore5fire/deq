package eventstore

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq/api/v1/deq"
	"gitlab.com/katcheCode/deq/pkg/eventstore/data"
)

func getEvent(txn *badger.Txn, topic, eventID, channel string) (*deq.Event, error) {
	key, err := data.EventTimeKey{
		ID:    eventID,
		Topic: topic,
	}.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal event time key: %v", err)
	}
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		log.Printf("get %s", key)
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	val, err := item.Value()
	if err != nil {
		return nil, err
	}

	var eventTime data.EventTimePayload
	err = proto.Unmarshal(val, &eventTime)
	if err != nil {
		return nil, fmt.Errorf("unmarshal event time payload: %v", err)
	}

	eventKey, err := data.EventKey{
		ID:         eventID,
		Topic:      topic,
		CreateTime: time.Unix(0, eventTime.CreateTime),
	}.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal event key: %v", err)
	}
	item, err = txn.Get(eventKey)
	if err != nil {
		return nil, err
	}
	val, err = item.Value()
	if err != nil {
		return nil, err
	}

	var event data.EventPayload
	err = proto.Unmarshal(val, &event)
	if err != nil {
		return nil, fmt.Errorf("unmarshal event payload: %v", err)
	}

	eventState := deq.EventState_QUEUED

	if channel != "" {
		channelKey, err := data.ChannelKey{
			ID:      eventID,
			Topic:   topic,
			Channel: channel,
		}.Marshal()
		if err != nil {
			return nil, fmt.Errorf("marshal channel key: %v", err)
		}
		item, err = txn.Get(channelKey)
		if err != nil && err != badger.ErrKeyNotFound {
			return nil, err
		}
		// not found isn't an error - it just means we need to use the default state
		if err == nil {
			val, err = item.Value()
			if err != nil {
				return nil, err
			}
			var channelState data.ChannelPayload
			err = proto.Unmarshal(val, &channelState)
			if err != nil {
				return nil, fmt.Errorf("unmarshal channel payload: %v", err)
			}

			eventState = channelState.EventState
		}
	}

	return &deq.Event{
		Id:           eventID,
		Topic:        topic,
		CreateTime:   eventTime.CreateTime,
		State:        eventState,
		DefaultState: event.DefaultEventState,
		Payload:      event.Payload,
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

func writeEvent(txn *badger.Txn, e *deq.Event) error {
	key, err := data.EventTimeKey{
		Topic: e.Topic,
		ID:    e.Id,
	}.Marshal()
	if err != nil {
		return fmt.Errorf("marshal event time key: %v", err)
	}

	_, err = txn.Get(key)
	if err == nil {
		existing, err := getEvent(txn, e.Topic, e.Id, "")
		if err != nil {
			return fmt.Errorf("get existing event: %v", err)
		}
		if !bytes.Equal(existing.Payload, e.Payload) {
			return ErrAlreadyExists
		}
		return nil
	}
	if err != badger.ErrKeyNotFound {
		return fmt.Errorf("check event doesn't exist: %v", err)
	}

	val, err := proto.Marshal(&data.EventTimePayload{
		CreateTime: e.CreateTime,
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
		CreateTime: time.Unix(0, e.CreateTime),
		ID:         e.Id,
	}.Marshal()
	if err != nil {
		return fmt.Errorf("marshal event time key: %v", err)
	}

	val, err = proto.Marshal(&data.EventPayload{
		Payload:           e.Payload,
		DefaultEventState: e.DefaultState,
	})
	if err != nil {
		return fmt.Errorf("marshal event time payload: %v", err)
	}

	err = txn.Set(key, val)
	if err != nil {
		return err
	}

	if e.DefaultState != deq.EventState_UNSPECIFIED_STATE && e.DefaultState != deq.EventState_QUEUED {

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
				ID:      e.Id,
			}

			err = setEventState(txn, newKey, e.DefaultState)
			if err != nil {
				return fmt.Errorf("set event state on channel %s: %v", key.Channel, err)
			}
		}
	}

	return nil
}

func setEventState(txn *badger.Txn, key data.ChannelKey, state deq.EventState) error {
	rawkey, err := key.Marshal()
	if err != nil {
		return fmt.Errorf("marshal key: %v", err)
	}
	payload, err := proto.Marshal(&data.ChannelPayload{
		EventState: state,
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %v", err)
	}

	err = txn.Set(rawkey, payload)
	if err != nil {
		return err
	}

	return nil
}
