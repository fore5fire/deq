package deq

import (
	"bytes"
	"fmt"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq/internal/data"
	"gitlab.com/katcheCode/deq/internal/storage"
)

// IterOpts is the available options for
type IterOpts struct {
	// Min and Max specify inclusive bounds for the returned results.
	Min, Max string
	// Reversed specifies if the listed results are sorted in reverse order.
	Reversed bool
	// PrefetchCount specifies how many values to prefetch.
	PrefetchCount int
}

/*
DefaultIterOpts are default options for iterators, intended to be used as a starting point for
custom options. For example:

	opts := deq.DefaultIterOpts
	opts.Reversed = true

	iter := channel.NewEventIter(opts)
	defer iter.Close()
*/
var DefaultIterOpts = IterOpts{
	PrefetchCount: 20,
}

/*
EventIter iterates events in the database lexicographically by ID. It is created with
Channel.NewEventIter, and should always be closed after it is done being used.

Example usage:

	opts := deq.DefaultIterOpts
	// customize options as needed

	iter := channel.NewEventIter(opts)
	defer iter.Close()

	for iter.Next() {
		fmt.Println(iter.Event().ID)
	}
	if iter.Err() != nil {
		// handle error
	}
*/
type EventIter struct {
	txn     storage.Txn
	it      storage.Iter
	opts    IterOpts
	current Event
	err     error
	end     []byte
	channel string
}

// NewEventIter creates a new EventIter that iterates events on the topic and channel of c.
//
// opts.Min and opts.Max specify the range of event IDs to read from c's topic. EventIter only has
// partial support for opts.PrefetchCount.
func (c *Channel) NewEventIter(opts IterOpts) *EventIter {

	// Topic should be valid, no need to check error
	prefix, _ := data.EventTimePrefixTopic(c.topic)
	max := opts.Max + "\xff\xff\xff\xff"

	var start, end []byte
	if opts.Reversed {
		start = append(prefix, max...)
		end = append(append(end, prefix...), opts.Min...)
	} else {
		start = append(append(start, prefix...), opts.Min...)
		end = append(prefix, max...)
	}

	txn := c.db.NewTransaction(false)
	it := txn.NewIterator(badger.IteratorOptions{
		Reverse:        opts.Reversed,
		PrefetchValues: opts.PrefetchCount > 0,
		// TODO: prefetch other event data too
		PrefetchSize: opts.PrefetchCount,
	})

	it.Seek(start)

	return &EventIter{
		opts:    opts,
		txn:     txn,
		it:      it,
		end:     end,
		channel: c.name,
	}
}

// Next advances the current event of iter and returns whether the iter has terminated.
//
// Next should be called before iter.Event() is called for the first time.
func (iter *EventIter) Next() bool {
	// Clear any error from the previous iteration.
	iter.err = nil

	// Check if there are any values left
	target := 1
	if iter.opts.Reversed {
		target = -1
	}
	if !iter.it.Valid() || bytes.Compare(iter.it.Item().Key(), iter.end) == target {
		return false
	}

	// Advance the iterator after we cache the current value.
	defer iter.it.Next()

	item := iter.it.Item()

	var key data.EventTimeKey
	err := data.UnmarshalTo(item.Key(), &key)
	if err != nil {
		iter.err = fmt.Errorf("parse event key %s: %v", item.Key(), err)
		return true
	}

	val, err := item.Value()
	if err != nil {
		iter.err = fmt.Errorf("get item value: %v", err)
		return true
	}

	var eTime data.EventTimePayload
	err = proto.Unmarshal(val, &eTime)
	if err != nil {
		iter.err = fmt.Errorf("unmarshal event time: %v", err)
		return true
	}

	createTime := time.Unix(0, eTime.CreateTime)

	e, err := getEventPayload(iter.txn, data.EventKey{
		Topic:      key.Topic,
		CreateTime: createTime,
		ID:         key.ID,
	})
	if err != nil {
		iter.err = fmt.Errorf("get event: %v", err)
	}

	channel, err := getChannelEvent(iter.txn, data.ChannelKey{
		Channel: iter.channel,
		Topic:   key.Topic,
		ID:      key.ID,
	})
	if err != nil {
		iter.err = fmt.Errorf("get channel event: %v", err)
		return true
	}

	iter.current = Event{
		ID:           key.ID,
		Topic:        key.Topic,
		CreateTime:   createTime,
		Payload:      e.Payload,
		RequeueCount: int(channel.RequeueCount),
		State:        protoToEventState(channel.EventState),
		DefaultState: protoToEventState(e.DefaultEventState),
		Indexes:      e.Indexes,
	}

	return true
}

// Event returns the current topic of iter.
//
// Call iter.Next() to advance the current event. When Event returns an error, it indicates that an
// error occurred retrieving the current event, but there may still be more events available as long
// as iter.Next() returns true.
func (iter *EventIter) Event() Event {
	return iter.current
}

// Err returns an error that occurred during a call to Next.
//
// Err should be checked after a call to Next returns false. If Err returns nil, then iteration
// completed successfully. Otherwise, after handling the error it is safe to try to continue
// iteration. For example:
//
//   for {
//     for iter.Next() {
//       // do something
//     }
//     if iter.Err() == nil {
//       break
//     }
//     // handle error
//   }
func (iter *EventIter) Err() error {
	return iter.err
}

// Close closes iter. Close should always be called when an iter is done being used.
func (iter *EventIter) Close() {
	iter.it.Close()
	iter.txn.Discard()
}

/*
TopicIter iterates topics in the database.

Always call Close() on a TopicIter when it is no longer needed. Each TopicIter returns values
from a snapshot of the database at the time the TopicIter was created. Each individual TopicIter is
not safe for concurrent use, but mutliple TopicIters can be used concurrently.

Example usage:
	opts := deq.DefaultIterOpts
	opts.Min = "example-min"

  iter := db.NewTopicIter(opts)
	defer iter.Close()

  for iter.Next() {
    topic := iter.Topic()
		// do something with topic
	}

*/
type TopicIter struct {
	txn     storage.Txn
	it      storage.Iter
	cursor  []byte
	current string
	opts    IterOpts
	end     []byte
}

// NewTopicIter creates a new TopicIter that can be used to iterate topics in the database.
//
// For details on available options, see IterOpts. TopicIter doesn't support opts.PrefetchCount.
// If opts.Min or opts.Max isn't a valid topic name, NewTopicIter panics.
func (s *Store) NewTopicIter(opts IterOpts) *TopicIter {

	txn := s.db.NewTransaction(false)
	return newTopicIter(txn, opts)
}

func newTopicIter(txn storage.Txn, opts IterOpts) *TopicIter {
	// Apply default options
	max := opts.Max
	if max == "" {
		max = data.LastTopic
	}

	// Setup start and end limits of iteration
	start, err := data.EventTimeCursorBeforeTopic(opts.Min)
	if err != nil {
		panic("opts.Min invalid: " + err.Error())
	}
	end, err := data.EventTimeCursorAfterTopic(max)
	if err != nil {
		panic("opts.Max invalid: " + err.Error())
	}
	// Switch start and end if we're reverse sorted.
	if opts.Reversed {
		tmp := start
		start = end
		end = tmp
	}

	// Badger setup
	it := txn.NewIterator(badger.IteratorOptions{
		Reverse:        opts.Reversed,
		PrefetchValues: false,
	})

	return &TopicIter{
		opts:   opts,
		txn:    txn,
		it:     it,
		cursor: start,
		end:    end,
	}
}

// Next advances the value of iter and returns whether the iter has terminated.
//
// Next should be called before iter.Topic() is called for the first time.
func (iter *TopicIter) Next() bool {

	// Seek the first event of the next topic and make sure one exists
	iter.it.Seek(iter.cursor)
	if !iter.it.Valid() {
		return false
	}
	item := iter.it.Item()

	// Check if we've passed our end value
	target := 1
	if iter.opts.Reversed {
		target = -1
	}
	if bytes.Compare(item.Key(), iter.end) == target {
		return false
	}

	// Unmarshal the key
	var key data.EventTimeKey
	// Keep iterating through events till we find one that works. It's out-of-scope for this function
	// to deal with errors in individual events, so supress errors and just present topics with at
	// least one valid event.
	for {
		err := data.UnmarshalTo(item.Key(), &key)
		if err == nil {
			break
		}
		iter.it.Next()
		if bytes.Compare(item.Key(), iter.end) == target || !iter.it.Valid() {
			return false
		}
	}

	// Update current so it can be retrieved by future calls to Topic.
	iter.current = key.Topic

	// Update the cursor for our next iteration
	if iter.opts.Reversed {
		// No error check should be needed here because the topic comes directly from an unmarshalled key.
		iter.cursor, _ = data.EventTimeCursorBeforeTopic(key.Topic)
	} else {
		// No error check should be needed here because the topic comes directly from an unmarshalled key.
		iter.cursor, _ = data.EventTimeCursorAfterTopic(key.Topic)
	}

	return true
}

// Topic returns the current topic of iter.
//
// Call iter.Next() to advance the current topic. When Topic returns an error, it indicates that an
// error occurred retrieving the current topic, but there may still be more topics available as long
// as iter.Next() returns true.
func (iter *TopicIter) Topic() string {
	return iter.current
}

// Close closes iter. Close should always be called when an iter is done being used.
func (iter *TopicIter) Close() {
	iter.it.Close()
	iter.txn.Discard()
}

/*
IndexIter iterates events in the database lexicographically by indexes. It is created with
Channel.NewIndexIter, and should always be closed after it is done being used.

Example usage:

	opts := deq.DefaultIterOpts
	// customize options as needed

	iter := channel.NewIndexIter(opts)
	defer iter.Close()

	for iter.Next() {
		fmt.Println(iter.Event())
	}
	if iter.Err() != nil {
		// handle error
	}
*/
type IndexIter struct {
	txn     storage.Txn
	it      storage.Iter
	opts    IterOpts
	current Event
	err     error
	end     []byte
	channel string
}

// NewIndexIter creates a new IndexIter that iterates events on the topic and channel of c.
//
// opts.Min and opts.Max specify the range of event IDs to read from c's topic. EventIter only has
// partial support for opts.PrefetchCount.
func (c *Channel) NewIndexIter(opts IterOpts) *IndexIter {

	// Topic should be valid, no need to check error
	prefix, _ := data.IndexPrefixTopic(c.topic)
	max := opts.Max + "\xff\xff\xff\xff"

	var start, end []byte
	if opts.Reversed {
		start = append(prefix, max...)
		end = append(append(end, prefix...), opts.Min...)
	} else {
		start = append(append(start, prefix...), opts.Min...)
		end = append(prefix, max...)
	}

	txn := c.db.NewTransaction(false)
	it := txn.NewIterator(badger.IteratorOptions{
		Reverse:        opts.Reversed,
		PrefetchValues: opts.PrefetchCount > 0,
		// TODO: prefetch other event data too
		PrefetchSize: opts.PrefetchCount,
	})

	it.Seek(start)

	return &IndexIter{
		opts:    opts,
		txn:     txn,
		it:      it,
		end:     end,
		channel: c.name,
	}
}

// Next advances the current event of iter and returns whether the iter has terminated.
//
// Next should be called before iter.Event() is called for the first time.
func (iter *IndexIter) Next() bool {
	// Clear any error from the previous iteration.
	iter.err = nil

	// Check if there are any values left
	target := 1
	if iter.opts.Reversed {
		target = -1
	}
	if !iter.it.Valid() || bytes.Compare(iter.it.Item().Key(), iter.end) == target {
		return false
	}

	// Advance the iterator after we cache the current value.
	defer iter.it.Next()

	item := iter.it.Item()

	var key data.IndexKey
	err := data.UnmarshalTo(item.Key(), &key)
	if err != nil {
		iter.err = fmt.Errorf("parse event key %s: %v", item.Key(), err)
		return false
	}

	e, err := getEvent(iter.txn, key.Topic, key.ID, iter.channel)
	if err != nil {
		iter.err = err
		return false
	}

	iter.current = *e

	return true
}

// Event returns the current topic of iter.
//
// Call Next to advance the current event. Next should be called at least once before Event.
func (iter *IndexIter) Event() Event {
	return iter.current
}

// Err returns an error that occurred during a call to Next.
//
// Err should be checked after a call to Next returns false. If Err returns nil, then iteration
// completed successfully. Otherwise, after handling the error it is safe to try to continue
// iteration. For example:
//
//   for {
//     for iter.Next() {
//       // do something
//     }
//     if iter.Err() == nil {
//       break
//     }
//     // handle error
//   }
func (iter *IndexIter) Err() error {
	return iter.err
}

// Close closes iter. Close should always be called when an iter is done being used.
func (iter *IndexIter) Close() {
	iter.it.Close()
	iter.txn.Discard()
}
