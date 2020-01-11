package data

import (
	"bytes"
	"context"
	"errors"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/deqerr"
)

/*
IndexIterV1_2_0 iterates events in the database lexicographically by indexes. It is created with
Channel.NewIndexIter, and should always be closed after it is done being used.

Example usage:

	iter := channel.NewIndexIter(nil)
	defer iter.Close()

	for iter.Next(ctx) {
		fmt.Println(iter.Event())
	}
	if iter.Err() != nil {
		// handle error
	}
*/
type IndexIterV1_2_0 struct {
	txn      Txn
	it       Iter
	current  deq.Event
	err      error
	end      []byte
	channel  string
	reversed bool
	selector string
}

// NewIndexIterV1_2_0 creates a new IndexIter that iterates events on the topic and channel of c.
//
// opts.Min and opts.Max specify the range of event IDs to read from c's topic. EventIter only has
// partial support for opts.PrefetchCount.
//
// Channel is optional - passing the empty string for channel causes results to have default values
// as if they were read on a new channel. This can speed up the iterators performance in cases
// where the channel specific details are not needed.
func NewIndexIterV1_2_0(txn Txn, topic, channel string, opts *deq.IterOptions) (*IndexIterV1_2_0, error) {

	if opts == nil {
		opts = &deq.IterOptions{}
	}

	var prefetchCount int
	if opts.PrefetchCount == -1 {
		prefetchCount = 0
	} else if opts.PrefetchCount == 0 {
		prefetchCount = 20
	} else {
		prefetchCount = opts.PrefetchCount
	}

	prefix, err := IndexPrefixTopic(topic)
	if err != nil {
		return nil, deqerr.Errorf(deqerr.Invalid, "build event time topic prefix: %v", err)
	}
	max := "\xff\xff\xff\xff"
	if opts.Max != "" {
		max = opts.Max
	}

	start := append(append([]byte(nil), prefix...), opts.Min...)
	end := append(prefix, max...)
	if opts.Reversed {
		start, end = end, start
	}

	it := txn.NewIterator(badger.IteratorOptions{
		Reverse:        opts.Reversed,
		PrefetchValues: prefetchCount > 0,
		// TODO: prefetch other event data too
		PrefetchSize: prefetchCount,
	})

	it.Seek(start)

	return &IndexIterV1_2_0{
		reversed: opts.Reversed,
		txn:      txn,
		it:       it,
		end:      end,
		channel:  channel,
		err:      errors.New("iteration not started"),
	}, nil
}

// Next advances the current event of iter and returns whether the iter has terminated.
//
// Next should be called before iter.Event() is called for the first time.
func (iter *IndexIterV1_2_0) Next(ctx context.Context) bool {
	// Clear any error from the previous iteration.
	iter.err = nil

	// Check if there are any values left
	target := 1
	if iter.reversed {
		target = -1
	}
	if !iter.it.Valid() || bytes.Compare(iter.it.Item().Key(), iter.end) == target {
		return false
	}

	// Advance the iterator after we cache the current value.
	defer iter.it.Next()

	item := iter.it.Item()

	var key IndexKey
	err := UnmarshalTo(item.Key(), &key)
	if err != nil {
		iter.err = deqerr.Errorf(deqerr.Internal, "parse event key %s: %v", item.Key(), err)
		return false
	}

	// TODO: support index-only iteration to only lookup when needed.
	var payload IndexPayloadV1_2_0
	err = item.Value(func(val []byte) error {
		err = proto.Unmarshal(val, &payload)
		if err != nil {
			return deqerr.Errorf(deqerr.Internal, "unmarshal index payload: %v", err)
		}
		return nil
	})
	if err != nil {
		iter.err = deqerr.Errorf(deqerr.Unavailable, "read index payload: %v", err)
		return false
	}

	e, err := GetEvent(iter.txn, key.Topic, payload.EventId, iter.channel)
	if err != nil {
		iter.err = deqerr.Errorf(deqerr.Unavailable, "get event %q %q: %v", key.Topic, payload.EventId, err)
		return false
	}

	e.Selector = key.Value
	// e.SelectorVersion = payload.Version
	iter.current = *e

	return true
}

// Event returns the current topic of iter.
//
// Call Next to advance the current event. Next should be called at least once before Event.
func (iter *IndexIterV1_2_0) Event() deq.Event {
	if iter.err != nil {
		panic("Event() is only valid when Err() returns nil")
	}
	return iter.current
}

// Err returns an error that occurred during a call to Next.
//
// Err should be checked after a call to Next returns false. If Err returns nil, then iteration
// completed successfully. Otherwise, after handling the error it is safe to try to continue
// iteration. For example:
//
//   for {
//     for iter.Next(ctx) {
//       // do something
//     }
//     if iter.Err() == nil {
//       break
//     }
//     // handle error
//   }
func (iter *IndexIterV1_2_0) Err() error {
	return iter.err
}

// Close closes iter. Close should always be called when an iter is done being used.
func (iter *IndexIterV1_2_0) Close() {
	iter.it.Close()
}

var _ deq.EventIter = &EventIter{}
var _ deq.EventIter = &IndexIter{}
