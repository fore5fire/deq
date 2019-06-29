///
//  Generated code. Do not modify.
//  source: type.proto
///
package deqtype

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/katcheCode/deq"
	
	types "github.com/gogo/protobuf/types"
)

type TypeEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	Type *types.Type
}

type _TypeTopicConfig interface {
	EventToTypeEvent(deq.Event) (*TypeEvent, error)
}

// TypeEventIter is an iterator for TypeEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a TypeEvent.
type TypeEventIter interface {
	Next(ctx context.Context) (*TypeEvent, error)
	Close()
}

type XXX_TypeEventIter struct {
	Iter   deq.EventIter
	Config _TypeTopicConfig
}

// Next returns the next TypeEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_TypeEventIter) Next(ctx context.Context) (*TypeEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToTypeEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to TypeEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_TypeEventIter) Close() {
	it.Iter.Close()
}


type FieldEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	Field *types.Field
}

type _FieldTopicConfig interface {
	EventToFieldEvent(deq.Event) (*FieldEvent, error)
}

// FieldEventIter is an iterator for FieldEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a FieldEvent.
type FieldEventIter interface {
	Next(ctx context.Context) (*FieldEvent, error)
	Close()
}

type XXX_FieldEventIter struct {
	Iter   deq.EventIter
	Config _FieldTopicConfig
}

// Next returns the next FieldEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_FieldEventIter) Next(ctx context.Context) (*FieldEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToFieldEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to FieldEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_FieldEventIter) Close() {
	it.Iter.Close()
}


type EnumEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	Enum *types.Enum
}

type _EnumTopicConfig interface {
	EventToEnumEvent(deq.Event) (*EnumEvent, error)
}

// EnumEventIter is an iterator for EnumEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a EnumEvent.
type EnumEventIter interface {
	Next(ctx context.Context) (*EnumEvent, error)
	Close()
}

type XXX_EnumEventIter struct {
	Iter   deq.EventIter
	Config _EnumTopicConfig
}

// Next returns the next EnumEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_EnumEventIter) Next(ctx context.Context) (*EnumEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToEnumEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to EnumEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_EnumEventIter) Close() {
	it.Iter.Close()
}


type EnumValueEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	EnumValue *types.EnumValue
}

type _EnumValueTopicConfig interface {
	EventToEnumValueEvent(deq.Event) (*EnumValueEvent, error)
}

// EnumValueEventIter is an iterator for EnumValueEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a EnumValueEvent.
type EnumValueEventIter interface {
	Next(ctx context.Context) (*EnumValueEvent, error)
	Close()
}

type XXX_EnumValueEventIter struct {
	Iter   deq.EventIter
	Config _EnumValueTopicConfig
}

// Next returns the next EnumValueEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_EnumValueEventIter) Next(ctx context.Context) (*EnumValueEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToEnumValueEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to EnumValueEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_EnumValueEventIter) Close() {
	it.Iter.Close()
}


type OptionEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	Option *types.Option
}

type _OptionTopicConfig interface {
	EventToOptionEvent(deq.Event) (*OptionEvent, error)
}

// OptionEventIter is an iterator for OptionEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a OptionEvent.
type OptionEventIter interface {
	Next(ctx context.Context) (*OptionEvent, error)
	Close()
}

type XXX_OptionEventIter struct {
	Iter   deq.EventIter
	Config _OptionTopicConfig
}

// Next returns the next OptionEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_OptionEventIter) Next(ctx context.Context) (*OptionEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToOptionEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to OptionEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_OptionEventIter) Close() {
	it.Iter.Close()
}


