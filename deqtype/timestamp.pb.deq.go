///
//  Generated code. Do not modify.
//  source: timestamp.proto
///
package deqtype

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/katcheCode/deq"
	
	types "github.com/gogo/protobuf/types"
)

type TimestampEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	Selector        string
	SelectorVersion int64

	Timestamp *types.Timestamp
}

type _TimestampTopicConfig interface {
	EventToTimestampEvent(deq.Event) (*TimestampEvent, error)
}

// TimestampEventIter is an iterator for TimestampEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a TimestampEvent.
type TimestampEventIter interface {
	Next(ctx context.Context) (*TimestampEvent, error)
	Close()
}

type XXX_TimestampEventIter struct {
	Iter   deq.EventIter
	Config _TimestampTopicConfig
}

// Next returns the next TimestampEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_TimestampEventIter) Next(ctx context.Context) (*TimestampEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToTimestampEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to TimestampEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_TimestampEventIter) Close() {
	it.Iter.Close()
}


