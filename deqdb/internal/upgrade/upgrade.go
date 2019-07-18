package upgrade

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/dgraph-io/badger"
	"gitlab.com/katcheCode/deq/deqdb/internal/data"
	"gitlab.com/katcheCode/deq/deqdb/internal/upgrade/v1_1_0"
	"gitlab.com/katcheCode/deq/deqdb/internal/upgrade/v1_2_0"
)

// DB upgrades a store's db to the current version.
// It is not safe to use the database concurrently with upgradeDB.
func DB(ctx context.Context, db data.DB, currentVersion string) error {

	if currentVersion == CodeVersion {
		return nil
	}

	// Upgrade 1.0.0 to 1.1.0
	if currentVersion == "1.0.0" {
		log.Printf("[INFO] upgrading db from 1.0.0 to 1.1.0")
		batchSize := 500
		u := &upgradeV1_0_0{}
		more := true
		for more {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			err := func() error {
				txn := db.NewTransaction(true)
				defer txn.Discard()

				more = u.NextBatch(txn, batchSize)

				err := txn.Commit()
				if err != nil {
					return fmt.Errorf("commit batch: %v", err)
				}

				log.Printf("[INFO] %d indexes upgraded, %d indexes failed", u.updated, u.failed)
				return nil
			}()
			if err != nil {
				return err
			}
		}
		currentVersion = "1.1.0"
	}

	// Upgrade 1.1.0 to 1.2.0
	if currentVersion == "1.1.0" {
		log.Printf("[INFO] upgrading db from 1.1.0 to 1.2.0")
		err := v1_1_0.UpgradeToV1_2_0(ctx, db)
		if err != nil {
			return fmt.Errorf("upgrade from 1.1.0 to 1.2.0: %v", err)
		}

		currentVersion = "1.2.0"
	}

	if currentVersion == "1.2.0" {
		log.Printf("[INFO] upgrading db from 1.2.0 to 1.2.1")
		err := v1_2_0.UpgradeToV1_2_1(ctx, db)
		if err != nil {
			return fmt.Errorf("upgrade from 1.2.0 to 1.2.1: %v", err)
		}
		currentVersion = "1.2.1"
	}

	if currentVersion != CodeVersion {
		return fmt.Errorf("unsupported on-disk version: %s", currentVersion)
	}

	txn := db.NewTransaction(true)
	defer txn.Discard()

	err := txn.Set([]byte(VersionKey), []byte(CodeVersion))
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return fmt.Errorf("commit db upgrade: %v", err)
	}

	log.Printf("[INFO] db upgraded to version %s", CodeVersion)

	return nil
}

// ErrVersionUnknown is returned when the storage version can not be determined.
var ErrVersionUnknown = errors.New("version unknown")

// StorageVersion returns the current version of the database as saved in the storage engine.
//
// badger.ErrKeyNotFound is returned if the database version could not be detected.
func StorageVersion(txn data.Txn) (string, error) {
	item, err := txn.Get([]byte(VersionKey))
	if err == badger.ErrKeyNotFound {
		return "", ErrVersionUnknown
	}
	if err != nil {
		return "", err
	}

	version, err := item.ValueCopy(nil)
	if err != nil {
		return "", err
	}

	return string(version), nil
}

type upgradeV1_0_0 struct {
	updated, failed int
	cursor          []byte
}

// NextBatch upgrades the database from v1.0.0 to the current version. It is the caller's
// responsibility to commit the Txn.
func (u *upgradeV1_0_0) NextBatch(txn data.Txn, batchSize int) bool {

	prefix := []byte{data.IndexTagV1_0_0, data.Sep}
	if len(u.cursor) == 0 {
		u.cursor = prefix
	}

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	var i int
	for it.Seek(append(u.cursor, 0)); it.ValidForPrefix(prefix); it.Next() {
		i++
		if i >= batchSize {
			return true
		}

		item := it.Item()

		u.cursor = item.KeyCopy(u.cursor)

		var oldIndex data.IndexKeyV1_0_0
		err := data.UnmarshalIndexKeyV1_0_0(item.Key(), &oldIndex)
		if err != nil {
			log.Printf("unmarshal v1.0.0 index key: %v", err)
			continue
		}

		var eTime data.EventTimePayload
		err = data.GetEventTimePayload(txn, &data.EventTimeKey{
			Topic: oldIndex.Topic,
			ID:    oldIndex.ID,
		}, &eTime)
		if err != nil {
			log.Printf("get event time payload: %v", err)
			continue
		}

		err = data.WriteIndex(txn, &data.IndexKey{
			Topic: oldIndex.Topic,
			Value: oldIndex.Value,
		}, &data.IndexPayload{
			EventId:    oldIndex.ID,
			CreateTime: eTime.CreateTime,
		})
		if err != nil {
			log.Printf("set new index: %v", err)
			continue
		}

		err = txn.Delete(item.Key())
		if err != nil {
			log.Printf("delete old index %v: %v", oldIndex, err)
			continue
		}

		u.updated++
	}
	u.failed = i - u.updated
	return false
}

const (
	// VersionKey is the key the current database version is stored under.
	VersionKey = "___DEQ_DB_VERSION___"
	// CodeVersion is the database version the running code expects.
	CodeVersion = "1.2.1"
)
