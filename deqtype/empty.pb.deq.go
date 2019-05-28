///
//  Generated code. Do not modify.
//  source: empty.proto
///
package deqtype

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/katcheCode/deq"
	types "github.com/gogo/protobuf/types"
)

type EmptyEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	Empty *types.Empty
}

type _EmptyTopicConfig interface {
	EventToEmptyEvent(deq.Event) (*EmptyEvent, error)
}

// EmptyEventIter is an iterator for EmptyEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a EmptyEvent.
type EmptyEventIter interface {
	Next(ctx context.Context) (*EmptyEvent, error)
	Close()
}

type XXX_EmptyEventIter struct {
	Iter   deq.EventIter
	Config _EmptyTopicConfig
}

// Next returns the next EmptyEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_EmptyEventIter) Next(ctx context.Context) (*EmptyEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToEmptyEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to EmptyEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_EmptyEventIter) Close() {
	it.Iter.Close()
}


