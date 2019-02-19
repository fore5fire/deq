package deq

import (
	"log"

	"github.com/dgraph-io/badger"
)

// UpgradeDB upgrades the store's db to the current version.
// It is not safe to update the database concurrently with UpgradeDB.
func (s *Store) UpgradeDB() error {

	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	currentVersion, err := s.getDBVersion(txn)
	if err != nil {
		return err
	}

	log.Printf("[INFO] current DB version is %s", currentVersion)

	if currentVersion == "0" {
		panic("DB version 0 is no longer supported.")
		// log.Printf("[INFO] upgrading db...")

		// // err = upgradeV0EventsToV1(s.db)
		// if err != nil {
		// 	return err
		// }

		// err := txn.Set([]byte(dbVersionKey), []byte(dbCodeVersion))
		// if err != nil {
		// 	return err
		// }

		// err = txn.Commit(nil)
		// if err != nil {
		// 	return fmt.Errorf("commit db upgrade: %v", err)
		// }

		// log.Printf("db upgraded to version %s", dbCodeVersion)
	}

	return nil
}

func (s *Store) getDBVersion(txn *badger.Txn) (string, error) {
	item, err := txn.Get([]byte(dbVersionKey))
	if err == badger.ErrKeyNotFound {
		return "1.0.0", nil
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

// upgradeV0EventsToV1 upgrades all events from v0 to v1 without commiting the txn.
// func upgradeV0EventsToV1(db *badger.DB) error {
// 	chunkSize := 500
// 	updated := 0
// 	skipped := 0
// 	prefix := []byte{data.EventV0Tag}
// 	cursor := prefix
// 	for {
// 		i := 0

// 		err := db.Update(func(txn *badger.Txn) error {
// 			it := txn.NewIterator(badger.DefaultIteratorOptions)
// 			defer it.Close()

// 			for it.Seek(append(cursor, 0)); it.ValidForPrefix(prefix) && i < chunkSize; it.Next() {
// 				i++
// 				item := it.Item()
// 				buf, err := item.Value()
// 				if err != nil {
// 					return fmt.Errorf("get item value: %v", err)
// 				}

// 				cursor = item.KeyCopy(cursor)

// 				var event deq.EventV0
// 				err = proto.Unmarshal(buf, &event)
// 				if err != nil {
// 					return fmt.Errorf("unmarshal v0 formatted event: %v", err)
// 				}

// 				e := deq.Event{
// 					Topic:   strings.TrimPrefix(strings.TrimPrefix(event.Payload.GetTypeUrl(), "types.googleapis.com/"), "type.googleapis.com/"),
// 					Id:      "v0-" + base64.RawURLEncoding.EncodeToString(event.Id),
// 					Payload: event.Payload.GetValue(),
// 					// We didn't keep track of the state before, so let's assume they're all
// 					// dequeued to prevent a mass send-out
// 					DefaultState: EventStateQueued,
// 					CreateTime:   deq.DeprecatedTimeFromID(event.Id).UnixNano(),
// 				}

// 				err = writeEvent(txn, &e)
// 				if err != nil {
// 					log.Printf("write v1 event %s: %v", e.Id, err)
// 					skipped++
// 					continue
// 				}

// 				err = txn.Delete(item.KeyCopy(nil))
// 				if err != nil {
// 					log.Printf("delete v0 event %v: %v", event.Id, err)
// 					skipped++
// 					continue
// 				}

// 				updated++
// 			}
// 			return nil
// 		})
// 		if err != nil {
// 			return err
// 		}

// 		log.Printf("%d upgraded, %d skipped", updated, skipped)
// 		if i < chunkSize {
// 			break
// 		}
// 	}

// 	return nil
// }

const (
	dbVersionKey  = "___DEQ_DB_VERSION___"
	dbCodeVersion = "1.0.0"
)
