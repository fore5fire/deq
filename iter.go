package deq

import (
	"bytes"
	"fmt"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq/internal/data"
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
EventIter iterates events in the database. It is created with Channel.NewEventIter, and should
always be closed after it is done being used.

Example usage:

	opts := deq.DefaultIterOpts
	// customize options as needed

	iter := channel.NewEventIter(opts)
	defer iter.Close()

	for iter.Next() {
		event, err := iter.Event()
		if err != nil {
			// handle error
			continue
		}
		log.Println(event.ID)
	}
*/
type EventIter struct {
	txn         *badger.Txn
	it          *badger.Iterator
	opts        IterOpts
	current     Event
	err         error
	end         []byte
	channelName string
	topic       string
}

// NewEventIter creates a new EventIter that can be used to iterate events in the database.
//
// See IterOpts for details on specific options. EventIter only has partial support for
// opts.PrefetchCount.
func (c *Channel) NewEventIter(opts IterOpts) *EventIter {

	// Topic should be valid, no need to check error
	prefix, _ := data.EventCursorBeforeTopic(c.topic)

	if opts.Max == "" {
		opts.Max = "\xFF\xFF\xFF\xFF"
	}

	var start, end []byte
	if opts.Reversed {
		start = append(append(start, prefix...), opts.Max...)
		end = append(append(end, prefix...), opts.Min...)
	} else {
		start = append(append(start, prefix...), opts.Min...)
		end = append(append(end, prefix...), opts.Max...)
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
		opts:        opts,
		txn:         txn,
		it:          it,
		end:         end,
		channelName: c.name,
		topic:       c.topic,
	}
}

// Next advances the current event of iter and returns whether the iter has terminated.
//
// Next should be called before iter.Event() is called for the first time.
func (iter *EventIter) Next() bool {
	// Check if there are any values left
	target := 1
	if iter.opts.Reversed {
		target = -1
	}
	if bytes.Compare(iter.it.Item().Key(), iter.end) == target || !iter.it.Valid() {
		return false
	}

	// Advance the iterator after we cache the current value.
	defer iter.it.Next()

	// Clear any error from the previous iteration.
	iter.err = nil

	item := iter.it.Item()

	var key data.EventKey
	err := data.UnmarshalTo(item.Key(), &key)
	if err != nil {
		iter.err = fmt.Errorf("parse event key %s: %v", item.Key(), err)
		return true
	}

	channel, err := getChannelEvent(iter.txn, data.ChannelKey{
		Channel: iter.channelName,
		Topic:   iter.topic,
		ID:      key.ID,
	})
	if err != nil {
		iter.err = fmt.Errorf("get channel event: %v", err)
		return true
	}

	val, err := item.Value()
	if err != nil {
		iter.err = fmt.Errorf("get item value: %v", err)
		return true
	}

	var e data.EventPayload
	err = proto.Unmarshal(val, &e)
	if err != nil {
		iter.err = fmt.Errorf("unmarshal event: %v", err)
		return true
	}

	iter.current = Event{
		ID:           key.ID,
		Topic:        key.Topic,
		CreateTime:   key.CreateTime,
		Payload:      e.Payload,
		RequeueCount: int(channel.RequeueCount),
		State:        protoToEventState(channel.EventState),
		DefaultState: protoToEventState(e.DefaultEventState),
	}

	return true
}

// Event returns the current topic of iter.
//
// Call iter.Next() to advance the current event. When Event returns an error, it indicates that an
// error occurred retrieving the current event, but there may still be more events available as long
// as iter.Next() returns true.
func (iter *EventIter) Event() (Event, error) {
	return iter.current, iter.err
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
	txn     *badger.Txn
	it      *badger.Iterator
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

	start, err := data.EventCursorBeforeTopic(opts.Min)
	if err != nil {
		panic("opts.Min invalid: " + err.Error())
	}
	if opts.Max == "" {
		opts.Max = "\xff\xff\xff\xff"
	}
	end, err := data.EventCursorAfterTopic(opts.Max)
	if err != nil {
		panic("opts.Max invalid: " + err.Error())
	}
	// Switch start and end if we're reverse sorted.
	if opts.Reversed {
		tmp := start
		start = end
		end = tmp
	}

	txn := s.db.NewTransaction(false)

	return &TopicIter{
		opts: opts,
		txn:  txn,
		it: txn.NewIterator(badger.IteratorOptions{
			Reverse:        opts.Reversed,
			PrefetchValues: false,
		}),
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
	var key data.EventKey
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
	// No error check should be needed here because the topic comes directly from an unmarshalled key.
	iter.cursor, _ = data.EventCursorBeforeTopic(key.Topic)
	if !iter.opts.Reversed {
		// If we're going forward, we need to seek just after this topic, or we'll just get the first
		// event in this topic over and over.
		iter.cursor = append(iter.cursor, 1)
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
