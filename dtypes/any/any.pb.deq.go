///
//  Generated code. Do not modify.
//  source: any/any.proto
///
package types

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/katcheCode/deq"
	types "github.com/gogo/protobuf/types"
)

type AnyEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	Any *types.Any
}

type _AnyTopicConfig interface {
	EventToAnyEvent(deq.Event) (*AnyEvent, error)
}

// AnyEventIter is an iterator for AnyEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a AnyEvent.
type AnyEventIter interface {
	Next(ctx context.Context) (*AnyEvent, error)
	Close()
}

type XXX_AnyEventIter struct {
	Iter   deq.EventIter
	Config _AnyTopicConfig
}

// Next returns the next AnyEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_AnyEventIter) Next(ctx context.Context) (*AnyEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToAnyEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to AnyEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_AnyEventIter) Close() {
	it.Iter.Close()
}


