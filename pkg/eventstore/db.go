package eventstore

import (
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

	eventState := event.DefaultEventState

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
		return ErrAlreadyExists
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

	return nil
}
