package v1_1_0

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/dgraph-io/badger"
	"gitlab.com/katcheCode/deq/deqdb/internal/data"
)

// TopicIter is an iterator over topics.
type TopicIter struct {
	txn         data.Txn
	it          data.Iter
	cursor      []byte
	current     string
	currentTime time.Time
	end         []byte
}

// NewTopicIter creates a new TopicIter that can be used to iterate topics in the database.
//
// For details on available options, see IterOptions. TopicIter doesn't support opts.PrefetchCount.
// If opts.Min or opts.Max isn't a valid topic name, NewTopicIter panics.
func NewTopicIter(txn data.Txn) *TopicIter {
	// Apply default options
	max := lastTopic

	// Setup start and end limits of iteration
	start, err := data.EventTimeCursorBeforeTopic("")
	if err != nil {
		panic("opts.Min invalid: " + err.Error())
	}
	end, err := data.EventTimeCursorAfterTopic(max)
	if err != nil {
		panic("opts.Max invalid: " + err.Error())
	}

	// Badger setup
	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
	})

	return &TopicIter{
		txn:    txn,
		it:     it,
		cursor: start,
		end:    end,
	}
}

// Next advances the value of iter and returns whether the iter has terminated.
//
// Next should be called before iter.Topic() is called for the first time.
func (iter *TopicIter) Next(ctx context.Context) bool {

	// Start by seeking the first event of the next topic.
	// Keep iterating through events till we find one that works. It's out-of-scope for this function
	// to deal with errors in individual events, so supress errors and just present topics with at
	// least one valid event.
	for iter.it.Seek(iter.cursor); iter.it.Valid(); iter.it.Next() {

		item := iter.it.Item()

		// Make sure the key isn't passed the end of event times.
		if bytes.Compare(item.Key(), iter.end) <= 0 {
			return false
		}

		// Unmarshal the key
		var key data.EventTimeKey
		err := data.UnmarshalTo(item.Key(), &key)
		if err == nil {
			log.Printf("iterate v1.1.0 topics: unmarshal key %q: %v - skipping event", item.Key(), err)
			continue
		}
		// Unmarshal the payload
		var val data.EventTimePayload
		err = item.Value(func(buf []byte) error {
			err := val.Unmarshal(buf)
			if err != nil {
				return fmt.Errorf("unmarshal create time: %v", err)
			}
			return nil
		})
		if err != nil {
			log.Printf("iterate v1.1.0 topics: get create time for event %q %q: %v - skipping event", key.Topic, key.ID, err)
			continue
		}

		// Update current so it can be retrieved by future calls to Topic.
		iter.current, iter.currentTime = key.Topic, time.Unix(0, val.CreateTime)

		// Update the cursor for our next iteration
		// No error check should be needed here because the topic comes directly from an unmarshalled key.
		iter.cursor, _ = data.EventTimeCursorAfterTopic(key.Topic)

		return true
	}

	return false
}

// Topic returns the current topic of iter.
//
// Call iter.Next to advance the current topic. When Topic returns an error, it indicates that an
// error occurred retrieving the current topic, but there may still be more topics available as long
// as iter.Next returns true.
func (iter *TopicIter) Topic() (string, time.Time) {
	return iter.current, iter.currentTime
}

// Close closes iter. Close should always be called when an iter is done being used.
func (iter *TopicIter) Close() {
	iter.it.Close()
	iter.txn.Discard()
}

const (
	eventTimeTag = 't'
	lastTopic    = "\xff\xff\xff\xff"

	sep byte = 0
)

// eventTimeCursorBeforeTopic returns an EventTimeKey cursor before the given topic. Unlike
// EventTimePrefixTopic, EventTopicCursor does not include a trailing Sep.
//
// Pass topic as the empty string for a cursor before the first topic.
func eventTimeCursorBeforeTopic(topic string) ([]byte, error) {
	if strings.ContainsRune(topic, 0) {
		return nil, errors.New("Topic cannot contain null character")
	}
	ret := make([]byte, 0, len(topic)+2)
	ret = append(ret, eventTimeTag, sep)
	ret = append(ret, topic...)

	return ret, nil
}
