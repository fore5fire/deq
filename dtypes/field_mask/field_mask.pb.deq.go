///
//  Generated code. Do not modify.
//  source: field_mask/field_mask.proto
///
package field_mask

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/katcheCode/deq"
	
	
	
	field_mask "github.com/gogo/protobuf/types"
)

type FieldMaskEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.EventState
	State        deq.EventState
	Indexes      []string

	FieldMask *field_mask.FieldMask
}

type _FieldMaskTopicConfig interface {
	EventToFieldMaskEvent(deq.Event) (*FieldMaskEvent, error)
}

// FieldMaskEventIter is an iterator for FieldMaskEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a FieldMaskEvent.
type FieldMaskEventIter interface {
	Next(ctx context.Context) (*FieldMaskEvent, error)
	Close()
}

type XXX_FieldMaskEventIter struct {
	Iter   deq.EventIter
	Config _FieldMaskTopicConfig
}

// Next returns the next FieldMaskEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_FieldMaskEventIter) Next(ctx context.Context) (*FieldMaskEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToFieldMaskEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to FieldMaskEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_FieldMaskEventIter) Close() {
	it.Iter.Close()
}


