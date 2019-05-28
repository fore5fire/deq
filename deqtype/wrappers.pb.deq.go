///
//  Generated code. Do not modify.
//  source: wrappers.proto
///
package deqtype

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/katcheCode/deq"
	types "github.com/gogo/protobuf/types"
)

type DoubleValueEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	DoubleValue *types.DoubleValue
}

type _DoubleValueTopicConfig interface {
	EventToDoubleValueEvent(deq.Event) (*DoubleValueEvent, error)
}

// DoubleValueEventIter is an iterator for DoubleValueEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a DoubleValueEvent.
type DoubleValueEventIter interface {
	Next(ctx context.Context) (*DoubleValueEvent, error)
	Close()
}

type XXX_DoubleValueEventIter struct {
	Iter   deq.EventIter
	Config _DoubleValueTopicConfig
}

// Next returns the next DoubleValueEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_DoubleValueEventIter) Next(ctx context.Context) (*DoubleValueEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToDoubleValueEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to DoubleValueEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_DoubleValueEventIter) Close() {
	it.Iter.Close()
}


type FloatValueEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	FloatValue *types.FloatValue
}

type _FloatValueTopicConfig interface {
	EventToFloatValueEvent(deq.Event) (*FloatValueEvent, error)
}

// FloatValueEventIter is an iterator for FloatValueEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a FloatValueEvent.
type FloatValueEventIter interface {
	Next(ctx context.Context) (*FloatValueEvent, error)
	Close()
}

type XXX_FloatValueEventIter struct {
	Iter   deq.EventIter
	Config _FloatValueTopicConfig
}

// Next returns the next FloatValueEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_FloatValueEventIter) Next(ctx context.Context) (*FloatValueEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToFloatValueEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to FloatValueEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_FloatValueEventIter) Close() {
	it.Iter.Close()
}


type Int64ValueEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	Int64Value *types.Int64Value
}

type _Int64ValueTopicConfig interface {
	EventToInt64ValueEvent(deq.Event) (*Int64ValueEvent, error)
}

// Int64ValueEventIter is an iterator for Int64ValueEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a Int64ValueEvent.
type Int64ValueEventIter interface {
	Next(ctx context.Context) (*Int64ValueEvent, error)
	Close()
}

type XXX_Int64ValueEventIter struct {
	Iter   deq.EventIter
	Config _Int64ValueTopicConfig
}

// Next returns the next Int64ValueEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_Int64ValueEventIter) Next(ctx context.Context) (*Int64ValueEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToInt64ValueEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to Int64ValueEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_Int64ValueEventIter) Close() {
	it.Iter.Close()
}


type UInt64ValueEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	UInt64Value *types.UInt64Value
}

type _UInt64ValueTopicConfig interface {
	EventToUInt64ValueEvent(deq.Event) (*UInt64ValueEvent, error)
}

// UInt64ValueEventIter is an iterator for UInt64ValueEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a UInt64ValueEvent.
type UInt64ValueEventIter interface {
	Next(ctx context.Context) (*UInt64ValueEvent, error)
	Close()
}

type XXX_UInt64ValueEventIter struct {
	Iter   deq.EventIter
	Config _UInt64ValueTopicConfig
}

// Next returns the next UInt64ValueEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_UInt64ValueEventIter) Next(ctx context.Context) (*UInt64ValueEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToUInt64ValueEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to UInt64ValueEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_UInt64ValueEventIter) Close() {
	it.Iter.Close()
}


type Int32ValueEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	Int32Value *types.Int32Value
}

type _Int32ValueTopicConfig interface {
	EventToInt32ValueEvent(deq.Event) (*Int32ValueEvent, error)
}

// Int32ValueEventIter is an iterator for Int32ValueEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a Int32ValueEvent.
type Int32ValueEventIter interface {
	Next(ctx context.Context) (*Int32ValueEvent, error)
	Close()
}

type XXX_Int32ValueEventIter struct {
	Iter   deq.EventIter
	Config _Int32ValueTopicConfig
}

// Next returns the next Int32ValueEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_Int32ValueEventIter) Next(ctx context.Context) (*Int32ValueEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToInt32ValueEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to Int32ValueEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_Int32ValueEventIter) Close() {
	it.Iter.Close()
}


type UInt32ValueEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	UInt32Value *types.UInt32Value
}

type _UInt32ValueTopicConfig interface {
	EventToUInt32ValueEvent(deq.Event) (*UInt32ValueEvent, error)
}

// UInt32ValueEventIter is an iterator for UInt32ValueEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a UInt32ValueEvent.
type UInt32ValueEventIter interface {
	Next(ctx context.Context) (*UInt32ValueEvent, error)
	Close()
}

type XXX_UInt32ValueEventIter struct {
	Iter   deq.EventIter
	Config _UInt32ValueTopicConfig
}

// Next returns the next UInt32ValueEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_UInt32ValueEventIter) Next(ctx context.Context) (*UInt32ValueEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToUInt32ValueEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to UInt32ValueEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_UInt32ValueEventIter) Close() {
	it.Iter.Close()
}


type BoolValueEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	BoolValue *types.BoolValue
}

type _BoolValueTopicConfig interface {
	EventToBoolValueEvent(deq.Event) (*BoolValueEvent, error)
}

// BoolValueEventIter is an iterator for BoolValueEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a BoolValueEvent.
type BoolValueEventIter interface {
	Next(ctx context.Context) (*BoolValueEvent, error)
	Close()
}

type XXX_BoolValueEventIter struct {
	Iter   deq.EventIter
	Config _BoolValueTopicConfig
}

// Next returns the next BoolValueEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_BoolValueEventIter) Next(ctx context.Context) (*BoolValueEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToBoolValueEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to BoolValueEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_BoolValueEventIter) Close() {
	it.Iter.Close()
}


type StringValueEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	StringValue *types.StringValue
}

type _StringValueTopicConfig interface {
	EventToStringValueEvent(deq.Event) (*StringValueEvent, error)
}

// StringValueEventIter is an iterator for StringValueEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a StringValueEvent.
type StringValueEventIter interface {
	Next(ctx context.Context) (*StringValueEvent, error)
	Close()
}

type XXX_StringValueEventIter struct {
	Iter   deq.EventIter
	Config _StringValueTopicConfig
}

// Next returns the next StringValueEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_StringValueEventIter) Next(ctx context.Context) (*StringValueEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToStringValueEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to StringValueEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_StringValueEventIter) Close() {
	it.Iter.Close()
}


type BytesValueEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	BytesValue *types.BytesValue
}

type _BytesValueTopicConfig interface {
	EventToBytesValueEvent(deq.Event) (*BytesValueEvent, error)
}

// BytesValueEventIter is an iterator for BytesValueEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a BytesValueEvent.
type BytesValueEventIter interface {
	Next(ctx context.Context) (*BytesValueEvent, error)
	Close()
}

type XXX_BytesValueEventIter struct {
	Iter   deq.EventIter
	Config _BytesValueTopicConfig
}

// Next returns the next BytesValueEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_BytesValueEventIter) Next(ctx context.Context) (*BytesValueEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToBytesValueEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to BytesValueEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_BytesValueEventIter) Close() {
	it.Iter.Close()
}


