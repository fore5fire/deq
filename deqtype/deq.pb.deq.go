///
//  Generated code. Do not modify.
//  source: deq.proto
///
package deqtype

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/katcheCode/deq"
	
)

type TopicEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	Selector        string
	SelectorVersion int64

	Topic *Topic
}

type _TopicTopicConfig interface {
	EventToTopicEvent(deq.Event) (*TopicEvent, error)
}

// TopicEventIter is an iterator for TopicEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a TopicEvent.
type TopicEventIter interface {
	Next(ctx context.Context) (*TopicEvent, error)
	Close()
}

type XXX_TopicEventIter struct {
	Iter   deq.EventIter
	Config _TopicTopicConfig
}

// Next returns the next TopicEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_TopicEventIter) Next(ctx context.Context) (*TopicEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToTopicEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to TopicEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_TopicEventIter) Close() {
	it.Iter.Close()
}


