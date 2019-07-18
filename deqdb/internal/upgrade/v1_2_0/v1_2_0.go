package v1_2_0

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/badger"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/deqdb/internal/data"
	"gitlab.com/katcheCode/deq/deqdb/internal/upgrade/batch"
)

const indexTag = 'I'

// UpgradeToV1_2_1 upgrades a database from version 1.2.0 to 1.2.1
func UpgradeToV1_2_1(ctx context.Context, db data.DB) error {
	return deleteOrphanedIndexes(ctx, db)
}

func deleteOrphanedIndexes(ctx context.Context, db data.DB) error {
	txn := db.NewTransaction(false)
	defer txn.Discard()

	batchSize := 500

	tag := []byte{indexTag}

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

			var key data.IndexKey
			err := data.UnmarshalIndexKey(item.Key(), &key)
			if err != nil {
				return fmt.Errorf("unmarshal index key %s: %v", item.Key(), err)
			}

			var indexPayload data.IndexPayload
			err = data.GetIndexPayload(writeTxn, &key, &indexPayload)
			if err != nil {
				return fmt.Errorf("get index payload: %v", err)
			}

			// Lookup event for index
			var eventPayload data.EventPayload
			err = data.GetEventPayload(writeTxn, &data.EventKey{
				Topic:      key.Topic,
				ID:         indexPayload.EventId,
				CreateTime: time.Unix(0, indexPayload.CreateTime),
			}, &eventPayload)
			if err != nil && err != deq.ErrNotFound {
				return fmt.Errorf("get event for index: %v", err)
			}
			if err == nil {
				// event still exists, nothing to do.
				return nil
			}

			// Event was deleted, delete the index.
			err = writeTxn.Delete(item.Key())
			if err != nil {
				return fmt.Errorf("delete orphaned index: %v", err)
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
