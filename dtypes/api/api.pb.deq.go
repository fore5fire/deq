///
//  Generated code. Do not modify.
//  source: api/api.proto
///
package api

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/katcheCode/deq"
	
	
	
	api "github.com/gogo/protobuf/types"
	ptype "google/protobuf"
	source_context "google/protobuf"
)

type ApiEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.EventState
	State        deq.EventState
	Indexes      []string

	Api *api.Api
}

type _ApiTopicConfig interface {
	EventToApiEvent(deq.Event) (*ApiEvent, error)
}

// ApiEventIter is an iterator for ApiEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a ApiEvent.
type ApiEventIter interface {
	Next(ctx context.Context) (*ApiEvent, error)
	Close()
}

type XXX_ApiEventIter struct {
	Iter   deq.EventIter
	Config _ApiTopicConfig
}

// Next returns the next ApiEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_ApiEventIter) Next(ctx context.Context) (*ApiEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToApiEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to ApiEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_ApiEventIter) Close() {
	it.Iter.Close()
}


type MethodEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.EventState
	State        deq.EventState
	Indexes      []string

	Method *api.Method
}

type _MethodTopicConfig interface {
	EventToMethodEvent(deq.Event) (*MethodEvent, error)
}

// MethodEventIter is an iterator for MethodEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a MethodEvent.
type MethodEventIter interface {
	Next(ctx context.Context) (*MethodEvent, error)
	Close()
}

type XXX_MethodEventIter struct {
	Iter   deq.EventIter
	Config _MethodTopicConfig
}

// Next returns the next MethodEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_MethodEventIter) Next(ctx context.Context) (*MethodEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToMethodEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to MethodEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_MethodEventIter) Close() {
	it.Iter.Close()
}


type MixinEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.EventState
	State        deq.EventState
	Indexes      []string

	Mixin *api.Mixin
}

type _MixinTopicConfig interface {
	EventToMixinEvent(deq.Event) (*MixinEvent, error)
}

// MixinEventIter is an iterator for MixinEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a MixinEvent.
type MixinEventIter interface {
	Next(ctx context.Context) (*MixinEvent, error)
	Close()
}

type XXX_MixinEventIter struct {
	Iter   deq.EventIter
	Config _MixinTopicConfig
}

// Next returns the next MixinEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_MixinEventIter) Next(ctx context.Context) (*MixinEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToMixinEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to MixinEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_MixinEventIter) Close() {
	it.Iter.Close()
}


