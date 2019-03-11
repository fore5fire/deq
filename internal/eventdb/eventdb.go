// Package eventdb provides a database for getting and setting DEQ's internal storage types.
package eventdb

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq/internal/eventdb/kvstore"
)

var ErrNotFound = errors.New("not found")

type DB struct {
	db kvstore.DB
}

func New(store *badger.DB) *DB {
	return &DB{kvstore.WrapBadger(store)}
}

type Txn struct {
	txn kvstore.Txn
}

func (db *DB) NewTransaction(update bool) Txn {
	return Txn{db.db.NewTransaction(update)}
}

func (txn *Txn) PrintKeys() {
	it := txn.txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()

		key, err := Unmarshal(item.Key())
		if err != nil {
			fmt.Printf("%v %v\n", item.Key(), err)
			continue
		}
		fmt.Printf("%+v\n", key)
	}
}

func (txn *Txn) GetEventTimePayload(key EventTimeKey) (payload EventTimePayload, err error) {
	rawKey, err := key.Marshal(nil)
	if err != nil {
		return payload, fmt.Errorf("marshal event time key: %v", err)
	}
	item, err := txn.txn.Get(rawKey)
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
		return EventTimePayload{}, fmt.Errorf("unmarshal event time payload: %v", err)
	}

	return payload, nil
}

func (txn *Txn) SetEventTimePayload(key EventTimeKey, payload *EventTimePayload) error {
	eventKey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal event key: %v", err)
	}
	val, err := proto.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal event time payload: %v", err)
	}
	err = txn.txn.Set(eventKey, val)
	if err != nil {
		return err
	}
	return nil
}

func (txn *Txn) DeleteEventTimePayload(key EventTimeKey) error {
	eventKey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal event key: %v", err)
	}
	err = txn.txn.Delete(eventKey)
	if err != nil {
		return err
	}
	return nil
}

func (txn *Txn) GetEventPayload(key EventKey) (payload EventPayload, err error) {
	rawKey, err := key.Marshal(nil)
	if err != nil {
		return payload, fmt.Errorf("marshal event key: %v", err)
	}
	item, err := txn.txn.Get(rawKey)
	if err != nil {
		return payload, err
	}
	val, err := item.Value()
	if err != nil {
		return payload, err
	}

	err = proto.Unmarshal(val, &payload)
	if err != nil {
		return EventPayload{}, fmt.Errorf("unmarshal event payload: %v", err)
	}

	return payload, nil
}

func (txn *Txn) SetEventPayload(key EventKey, payload *EventPayload) error {
	eventKey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal event key: %v", err)
	}
	val, err := proto.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal event time payload: %v", err)
	}
	err = txn.txn.Set(eventKey, val)
	if err != nil {
		return err
	}
	return nil
}

func (txn *Txn) DeleteEventPayload(key EventKey) error {
	eventKey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal event key: %v", err)
	}
	err = txn.txn.Delete(eventKey)
	if err != nil {
		return err
	}
	return nil
}

var defaultChannelPayload = ChannelPayload{
	EventState: EventState_QUEUED,
}

func (txn *Txn) GetChannelEvent(key ChannelKey) (ChannelPayload, error) {

	rawKey, err := key.Marshal(nil)
	if err != nil {
		return ChannelPayload{}, fmt.Errorf("marshal event key: %v", err)
	}

	item, err := txn.txn.Get(rawKey)
	if err == badger.ErrKeyNotFound {
		return defaultChannelPayload, nil
	}
	if err != nil {
		return ChannelPayload{}, err
	}
	// Not found isn't an error - it just means we need to use the default state
	val, err := item.Value()
	if err != nil {
		return ChannelPayload{}, err
	}
	var channelState ChannelPayload
	err = proto.Unmarshal(val, &channelState)
	if err != nil {
		return ChannelPayload{}, fmt.Errorf("unmarshal channel payload: %v", err)
	}

	return channelState, nil
}

func (txn *Txn) SetChannelEvent(key ChannelKey, payload ChannelPayload) error {

	rawkey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal key: %v", err)
	}
	buf, err := proto.Marshal(&payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %v", err)
	}

	err = txn.txn.Set(rawkey, buf)
	if err != nil {
		return err
	}

	return nil
}

func (txn *Txn) DeleteChannelEvent(key ChannelKey) error {
	indexKey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal index key: %v", err)
	}

	err = txn.txn.Delete(indexKey)
	if err != nil {
		return err
	}

	return nil
}

func (txn *Txn) SetIndex(key *IndexKey) error {
	indexKey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal index key: %v", err)
	}

	err = txn.txn.Set(indexKey, nil)
	if err != nil {
		return err
	}

	return nil
}

func (txn *Txn) DeleteIndex(key *IndexKey) error {
	indexKey, err := key.Marshal(nil)
	if err != nil {
		return fmt.Errorf("marshal index key: %v", err)
	}

	err = txn.txn.Delete(indexKey)
	if err != nil {
		return err
	}

	return nil
}

func (txn *Txn) Commit() error {
	return txn.txn.Commit()
}

func (txn *Txn) Discard() {
	txn.txn.Discard()
}
