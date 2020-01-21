package v1_1_0

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v2"
	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/deqdb/internal/data"
	"gitlab.com/katcheCode/deq/deqdb/internal/upgrade/batch"
	"gitlab.com/katcheCode/deq/deqtype"
)

// UpgradeToV1_2_0 upgrades a database from version 1.1.0 to 1.2.0
func UpgradeToV1_2_0(ctx context.Context, db data.DB) error {

	err := pubTopicEvents(ctx, db)
	if err != nil {
		return fmt.Errorf("publish topic events: %v", err)
	}

	err = splitChannelKeys(ctx, db)
	if err != nil {
		return fmt.Errorf("split channel keys: %v", err)
	}

	return nil
}

func pubTopicEvents(ctx context.Context, db data.DB) error {
	txn := db.NewTransaction(false)
	defer txn.Discard()

	batchSize := 500

	it := NewTopicIter(txn)
	defer it.Close()

	var batchErr error
	for batchErr != batch.ErrDone {
		writeTxn := db.NewTransaction(true)
		defer writeTxn.Discard()

		batchErr = batch.Run(ctx, batchSize, func(ctx context.Context) error {
			if !it.Next(ctx) {
				return batch.ErrDone
			}
			topic, createTime := it.Topic()
			payload, err := proto.Marshal(&deqtype.Topic{
				Topic: topic,
			})
			if err != nil {
				return fmt.Errorf("marshal topic %q payload: %v", topic, err)
			}
			err = data.WriteEvent(writeTxn, &deq.Event{
				ID:         topic,
				Topic:      "deq.events.Topic",
				CreateTime: createTime,
				Payload:    payload,
			})
			if err != nil {
				return fmt.Errorf("write event topic %q: %v", topic, err)
			}
			return nil
		})
		if batchErr != nil && batchErr != batch.ErrDone {
			return batchErr
		}

		err := writeTxn.Commit()
		if err != nil {
			return fmt.Errorf("commit txn: %v", err)
		}
	}

	return nil
}

func splitChannelKeys(ctx context.Context, db data.DB) error {

	txn := db.NewTransaction(false)
	defer txn.Discard()

	batchSize := 500

	tagArray := [...]byte{data.ChannelTag}
	tag := tagArray[:]

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	it.Seek(tag)

	var batchErr error
	for batchErr != batch.ErrDone {

		writeTxn := db.NewTransaction(true)
		defer writeTxn.Discard()

		batchErr = batch.Run(ctx, batchSize, func(ctx context.Context) error {
			it.Next()
			if !it.ValidForPrefix(tag) {
				return batch.ErrDone
			}
			item := it.Item()

			var key data.ChannelKey
			err := data.UnmarshalChannelKey(item.Key(), &key)
			if err != nil {
				return fmt.Errorf("unmarshal channel key %s: %v", item.Key(), err)
			}

			var channel data.ChannelPayload
			err = data.GetChannelEvent(writeTxn, &key, &channel)
			if err != nil {
				return fmt.Errorf("get channel event: %v", err)
			}

			if channel.DeprecatedSendCount != 0 {
				countKey := data.SendCountKey{
					Topic:   key.Topic,
					Channel: key.Channel,
					ID:      key.ID,
				}
				count := data.SendCount{
					SendCount: channel.DeprecatedSendCount,
				}
				err = data.SetSendCount(writeTxn, &countKey, &count)
				if err != nil {
					return fmt.Errorf("set v1.2.0 send count: %v", err)
				}

				channel.DeprecatedSendCount = 0
				err = data.SetChannelEvent(writeTxn, &key, &channel)
				if err != nil {
					return fmt.Errorf("remove send_count from channel event: %v", err)
				}
			}

			return nil
		})
		if batchErr != nil && batchErr != batch.ErrDone {
			return batchErr
		}

		err := writeTxn.Commit()
		if err != nil {
			return fmt.Errorf("commit txn: %v", err)
		}
	}

	return nil
}
