///
//  Generated code. Do not modify.
//  source: duration/duration.proto
///
package types

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/katcheCode/deq"
	types "github.com/gogo/protobuf/types"
)

type DurationEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	Duration *types.Duration
}

type _DurationTopicConfig interface {
	EventToDurationEvent(deq.Event) (*DurationEvent, error)
}

// DurationEventIter is an iterator for DurationEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a DurationEvent.
type DurationEventIter interface {
	Next(ctx context.Context) (*DurationEvent, error)
	Close()
}

type XXX_DurationEventIter struct {
	Iter   deq.EventIter
	Config _DurationTopicConfig
}

// Next returns the next DurationEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_DurationEventIter) Next(ctx context.Context) (*DurationEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToDurationEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to DurationEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_DurationEventIter) Close() {
	it.Iter.Close()
}


