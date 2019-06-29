///
//  Generated code. Do not modify.
//  source: descriptor.proto
///
package deqtype

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/katcheCode/deq"
	
	descriptor "github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
)

type FileDescriptorSetEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	FileDescriptorSet *descriptor.FileDescriptorSet
}

type _FileDescriptorSetTopicConfig interface {
	EventToFileDescriptorSetEvent(deq.Event) (*FileDescriptorSetEvent, error)
}

// FileDescriptorSetEventIter is an iterator for FileDescriptorSetEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a FileDescriptorSetEvent.
type FileDescriptorSetEventIter interface {
	Next(ctx context.Context) (*FileDescriptorSetEvent, error)
	Close()
}

type XXX_FileDescriptorSetEventIter struct {
	Iter   deq.EventIter
	Config _FileDescriptorSetTopicConfig
}

// Next returns the next FileDescriptorSetEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_FileDescriptorSetEventIter) Next(ctx context.Context) (*FileDescriptorSetEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToFileDescriptorSetEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to FileDescriptorSetEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_FileDescriptorSetEventIter) Close() {
	it.Iter.Close()
}


type FileDescriptorProtoEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	FileDescriptorProto *descriptor.FileDescriptorProto
}

type _FileDescriptorProtoTopicConfig interface {
	EventToFileDescriptorProtoEvent(deq.Event) (*FileDescriptorProtoEvent, error)
}

// FileDescriptorProtoEventIter is an iterator for FileDescriptorProtoEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a FileDescriptorProtoEvent.
type FileDescriptorProtoEventIter interface {
	Next(ctx context.Context) (*FileDescriptorProtoEvent, error)
	Close()
}

type XXX_FileDescriptorProtoEventIter struct {
	Iter   deq.EventIter
	Config _FileDescriptorProtoTopicConfig
}

// Next returns the next FileDescriptorProtoEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_FileDescriptorProtoEventIter) Next(ctx context.Context) (*FileDescriptorProtoEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToFileDescriptorProtoEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to FileDescriptorProtoEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_FileDescriptorProtoEventIter) Close() {
	it.Iter.Close()
}


type DescriptorProtoEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	DescriptorProto *descriptor.DescriptorProto
}

type _DescriptorProtoTopicConfig interface {
	EventToDescriptorProtoEvent(deq.Event) (*DescriptorProtoEvent, error)
}

// DescriptorProtoEventIter is an iterator for DescriptorProtoEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a DescriptorProtoEvent.
type DescriptorProtoEventIter interface {
	Next(ctx context.Context) (*DescriptorProtoEvent, error)
	Close()
}

type XXX_DescriptorProtoEventIter struct {
	Iter   deq.EventIter
	Config _DescriptorProtoTopicConfig
}

// Next returns the next DescriptorProtoEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_DescriptorProtoEventIter) Next(ctx context.Context) (*DescriptorProtoEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToDescriptorProtoEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to DescriptorProtoEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_DescriptorProtoEventIter) Close() {
	it.Iter.Close()
}


type ExtensionRangeOptionsEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	ExtensionRangeOptions *descriptor.ExtensionRangeOptions
}

type _ExtensionRangeOptionsTopicConfig interface {
	EventToExtensionRangeOptionsEvent(deq.Event) (*ExtensionRangeOptionsEvent, error)
}

// ExtensionRangeOptionsEventIter is an iterator for ExtensionRangeOptionsEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a ExtensionRangeOptionsEvent.
type ExtensionRangeOptionsEventIter interface {
	Next(ctx context.Context) (*ExtensionRangeOptionsEvent, error)
	Close()
}

type XXX_ExtensionRangeOptionsEventIter struct {
	Iter   deq.EventIter
	Config _ExtensionRangeOptionsTopicConfig
}

// Next returns the next ExtensionRangeOptionsEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_ExtensionRangeOptionsEventIter) Next(ctx context.Context) (*ExtensionRangeOptionsEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToExtensionRangeOptionsEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to ExtensionRangeOptionsEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_ExtensionRangeOptionsEventIter) Close() {
	it.Iter.Close()
}


type FieldDescriptorProtoEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	FieldDescriptorProto *descriptor.FieldDescriptorProto
}

type _FieldDescriptorProtoTopicConfig interface {
	EventToFieldDescriptorProtoEvent(deq.Event) (*FieldDescriptorProtoEvent, error)
}

// FieldDescriptorProtoEventIter is an iterator for FieldDescriptorProtoEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a FieldDescriptorProtoEvent.
type FieldDescriptorProtoEventIter interface {
	Next(ctx context.Context) (*FieldDescriptorProtoEvent, error)
	Close()
}

type XXX_FieldDescriptorProtoEventIter struct {
	Iter   deq.EventIter
	Config _FieldDescriptorProtoTopicConfig
}

// Next returns the next FieldDescriptorProtoEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_FieldDescriptorProtoEventIter) Next(ctx context.Context) (*FieldDescriptorProtoEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToFieldDescriptorProtoEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to FieldDescriptorProtoEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_FieldDescriptorProtoEventIter) Close() {
	it.Iter.Close()
}


type OneofDescriptorProtoEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	OneofDescriptorProto *descriptor.OneofDescriptorProto
}

type _OneofDescriptorProtoTopicConfig interface {
	EventToOneofDescriptorProtoEvent(deq.Event) (*OneofDescriptorProtoEvent, error)
}

// OneofDescriptorProtoEventIter is an iterator for OneofDescriptorProtoEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a OneofDescriptorProtoEvent.
type OneofDescriptorProtoEventIter interface {
	Next(ctx context.Context) (*OneofDescriptorProtoEvent, error)
	Close()
}

type XXX_OneofDescriptorProtoEventIter struct {
	Iter   deq.EventIter
	Config _OneofDescriptorProtoTopicConfig
}

// Next returns the next OneofDescriptorProtoEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_OneofDescriptorProtoEventIter) Next(ctx context.Context) (*OneofDescriptorProtoEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToOneofDescriptorProtoEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to OneofDescriptorProtoEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_OneofDescriptorProtoEventIter) Close() {
	it.Iter.Close()
}


type EnumDescriptorProtoEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	EnumDescriptorProto *descriptor.EnumDescriptorProto
}

type _EnumDescriptorProtoTopicConfig interface {
	EventToEnumDescriptorProtoEvent(deq.Event) (*EnumDescriptorProtoEvent, error)
}

// EnumDescriptorProtoEventIter is an iterator for EnumDescriptorProtoEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a EnumDescriptorProtoEvent.
type EnumDescriptorProtoEventIter interface {
	Next(ctx context.Context) (*EnumDescriptorProtoEvent, error)
	Close()
}

type XXX_EnumDescriptorProtoEventIter struct {
	Iter   deq.EventIter
	Config _EnumDescriptorProtoTopicConfig
}

// Next returns the next EnumDescriptorProtoEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_EnumDescriptorProtoEventIter) Next(ctx context.Context) (*EnumDescriptorProtoEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToEnumDescriptorProtoEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to EnumDescriptorProtoEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_EnumDescriptorProtoEventIter) Close() {
	it.Iter.Close()
}


type EnumValueDescriptorProtoEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	EnumValueDescriptorProto *descriptor.EnumValueDescriptorProto
}

type _EnumValueDescriptorProtoTopicConfig interface {
	EventToEnumValueDescriptorProtoEvent(deq.Event) (*EnumValueDescriptorProtoEvent, error)
}

// EnumValueDescriptorProtoEventIter is an iterator for EnumValueDescriptorProtoEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a EnumValueDescriptorProtoEvent.
type EnumValueDescriptorProtoEventIter interface {
	Next(ctx context.Context) (*EnumValueDescriptorProtoEvent, error)
	Close()
}

type XXX_EnumValueDescriptorProtoEventIter struct {
	Iter   deq.EventIter
	Config _EnumValueDescriptorProtoTopicConfig
}

// Next returns the next EnumValueDescriptorProtoEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_EnumValueDescriptorProtoEventIter) Next(ctx context.Context) (*EnumValueDescriptorProtoEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToEnumValueDescriptorProtoEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to EnumValueDescriptorProtoEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_EnumValueDescriptorProtoEventIter) Close() {
	it.Iter.Close()
}


type ServiceDescriptorProtoEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	ServiceDescriptorProto *descriptor.ServiceDescriptorProto
}

type _ServiceDescriptorProtoTopicConfig interface {
	EventToServiceDescriptorProtoEvent(deq.Event) (*ServiceDescriptorProtoEvent, error)
}

// ServiceDescriptorProtoEventIter is an iterator for ServiceDescriptorProtoEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a ServiceDescriptorProtoEvent.
type ServiceDescriptorProtoEventIter interface {
	Next(ctx context.Context) (*ServiceDescriptorProtoEvent, error)
	Close()
}

type XXX_ServiceDescriptorProtoEventIter struct {
	Iter   deq.EventIter
	Config _ServiceDescriptorProtoTopicConfig
}

// Next returns the next ServiceDescriptorProtoEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_ServiceDescriptorProtoEventIter) Next(ctx context.Context) (*ServiceDescriptorProtoEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToServiceDescriptorProtoEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to ServiceDescriptorProtoEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_ServiceDescriptorProtoEventIter) Close() {
	it.Iter.Close()
}


type MethodDescriptorProtoEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	MethodDescriptorProto *descriptor.MethodDescriptorProto
}

type _MethodDescriptorProtoTopicConfig interface {
	EventToMethodDescriptorProtoEvent(deq.Event) (*MethodDescriptorProtoEvent, error)
}

// MethodDescriptorProtoEventIter is an iterator for MethodDescriptorProtoEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a MethodDescriptorProtoEvent.
type MethodDescriptorProtoEventIter interface {
	Next(ctx context.Context) (*MethodDescriptorProtoEvent, error)
	Close()
}

type XXX_MethodDescriptorProtoEventIter struct {
	Iter   deq.EventIter
	Config _MethodDescriptorProtoTopicConfig
}

// Next returns the next MethodDescriptorProtoEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_MethodDescriptorProtoEventIter) Next(ctx context.Context) (*MethodDescriptorProtoEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToMethodDescriptorProtoEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to MethodDescriptorProtoEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_MethodDescriptorProtoEventIter) Close() {
	it.Iter.Close()
}


type FileOptionsEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	FileOptions *descriptor.FileOptions
}

type _FileOptionsTopicConfig interface {
	EventToFileOptionsEvent(deq.Event) (*FileOptionsEvent, error)
}

// FileOptionsEventIter is an iterator for FileOptionsEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a FileOptionsEvent.
type FileOptionsEventIter interface {
	Next(ctx context.Context) (*FileOptionsEvent, error)
	Close()
}

type XXX_FileOptionsEventIter struct {
	Iter   deq.EventIter
	Config _FileOptionsTopicConfig
}

// Next returns the next FileOptionsEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_FileOptionsEventIter) Next(ctx context.Context) (*FileOptionsEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToFileOptionsEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to FileOptionsEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_FileOptionsEventIter) Close() {
	it.Iter.Close()
}


type MessageOptionsEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	MessageOptions *descriptor.MessageOptions
}

type _MessageOptionsTopicConfig interface {
	EventToMessageOptionsEvent(deq.Event) (*MessageOptionsEvent, error)
}

// MessageOptionsEventIter is an iterator for MessageOptionsEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a MessageOptionsEvent.
type MessageOptionsEventIter interface {
	Next(ctx context.Context) (*MessageOptionsEvent, error)
	Close()
}

type XXX_MessageOptionsEventIter struct {
	Iter   deq.EventIter
	Config _MessageOptionsTopicConfig
}

// Next returns the next MessageOptionsEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_MessageOptionsEventIter) Next(ctx context.Context) (*MessageOptionsEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToMessageOptionsEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to MessageOptionsEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_MessageOptionsEventIter) Close() {
	it.Iter.Close()
}


type FieldOptionsEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	FieldOptions *descriptor.FieldOptions
}

type _FieldOptionsTopicConfig interface {
	EventToFieldOptionsEvent(deq.Event) (*FieldOptionsEvent, error)
}

// FieldOptionsEventIter is an iterator for FieldOptionsEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a FieldOptionsEvent.
type FieldOptionsEventIter interface {
	Next(ctx context.Context) (*FieldOptionsEvent, error)
	Close()
}

type XXX_FieldOptionsEventIter struct {
	Iter   deq.EventIter
	Config _FieldOptionsTopicConfig
}

// Next returns the next FieldOptionsEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_FieldOptionsEventIter) Next(ctx context.Context) (*FieldOptionsEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToFieldOptionsEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to FieldOptionsEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_FieldOptionsEventIter) Close() {
	it.Iter.Close()
}


type OneofOptionsEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	OneofOptions *descriptor.OneofOptions
}

type _OneofOptionsTopicConfig interface {
	EventToOneofOptionsEvent(deq.Event) (*OneofOptionsEvent, error)
}

// OneofOptionsEventIter is an iterator for OneofOptionsEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a OneofOptionsEvent.
type OneofOptionsEventIter interface {
	Next(ctx context.Context) (*OneofOptionsEvent, error)
	Close()
}

type XXX_OneofOptionsEventIter struct {
	Iter   deq.EventIter
	Config _OneofOptionsTopicConfig
}

// Next returns the next OneofOptionsEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_OneofOptionsEventIter) Next(ctx context.Context) (*OneofOptionsEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToOneofOptionsEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to OneofOptionsEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_OneofOptionsEventIter) Close() {
	it.Iter.Close()
}


type EnumOptionsEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	EnumOptions *descriptor.EnumOptions
}

type _EnumOptionsTopicConfig interface {
	EventToEnumOptionsEvent(deq.Event) (*EnumOptionsEvent, error)
}

// EnumOptionsEventIter is an iterator for EnumOptionsEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a EnumOptionsEvent.
type EnumOptionsEventIter interface {
	Next(ctx context.Context) (*EnumOptionsEvent, error)
	Close()
}

type XXX_EnumOptionsEventIter struct {
	Iter   deq.EventIter
	Config _EnumOptionsTopicConfig
}

// Next returns the next EnumOptionsEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_EnumOptionsEventIter) Next(ctx context.Context) (*EnumOptionsEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToEnumOptionsEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to EnumOptionsEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_EnumOptionsEventIter) Close() {
	it.Iter.Close()
}


type EnumValueOptionsEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	EnumValueOptions *descriptor.EnumValueOptions
}

type _EnumValueOptionsTopicConfig interface {
	EventToEnumValueOptionsEvent(deq.Event) (*EnumValueOptionsEvent, error)
}

// EnumValueOptionsEventIter is an iterator for EnumValueOptionsEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a EnumValueOptionsEvent.
type EnumValueOptionsEventIter interface {
	Next(ctx context.Context) (*EnumValueOptionsEvent, error)
	Close()
}

type XXX_EnumValueOptionsEventIter struct {
	Iter   deq.EventIter
	Config _EnumValueOptionsTopicConfig
}

// Next returns the next EnumValueOptionsEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_EnumValueOptionsEventIter) Next(ctx context.Context) (*EnumValueOptionsEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToEnumValueOptionsEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to EnumValueOptionsEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_EnumValueOptionsEventIter) Close() {
	it.Iter.Close()
}


type ServiceOptionsEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	ServiceOptions *descriptor.ServiceOptions
}

type _ServiceOptionsTopicConfig interface {
	EventToServiceOptionsEvent(deq.Event) (*ServiceOptionsEvent, error)
}

// ServiceOptionsEventIter is an iterator for ServiceOptionsEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a ServiceOptionsEvent.
type ServiceOptionsEventIter interface {
	Next(ctx context.Context) (*ServiceOptionsEvent, error)
	Close()
}

type XXX_ServiceOptionsEventIter struct {
	Iter   deq.EventIter
	Config _ServiceOptionsTopicConfig
}

// Next returns the next ServiceOptionsEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_ServiceOptionsEventIter) Next(ctx context.Context) (*ServiceOptionsEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToServiceOptionsEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to ServiceOptionsEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_ServiceOptionsEventIter) Close() {
	it.Iter.Close()
}


type MethodOptionsEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	MethodOptions *descriptor.MethodOptions
}

type _MethodOptionsTopicConfig interface {
	EventToMethodOptionsEvent(deq.Event) (*MethodOptionsEvent, error)
}

// MethodOptionsEventIter is an iterator for MethodOptionsEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a MethodOptionsEvent.
type MethodOptionsEventIter interface {
	Next(ctx context.Context) (*MethodOptionsEvent, error)
	Close()
}

type XXX_MethodOptionsEventIter struct {
	Iter   deq.EventIter
	Config _MethodOptionsTopicConfig
}

// Next returns the next MethodOptionsEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_MethodOptionsEventIter) Next(ctx context.Context) (*MethodOptionsEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToMethodOptionsEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to MethodOptionsEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_MethodOptionsEventIter) Close() {
	it.Iter.Close()
}


type UninterpretedOptionEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	UninterpretedOption *descriptor.UninterpretedOption
}

type _UninterpretedOptionTopicConfig interface {
	EventToUninterpretedOptionEvent(deq.Event) (*UninterpretedOptionEvent, error)
}

// UninterpretedOptionEventIter is an iterator for UninterpretedOptionEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a UninterpretedOptionEvent.
type UninterpretedOptionEventIter interface {
	Next(ctx context.Context) (*UninterpretedOptionEvent, error)
	Close()
}

type XXX_UninterpretedOptionEventIter struct {
	Iter   deq.EventIter
	Config _UninterpretedOptionTopicConfig
}

// Next returns the next UninterpretedOptionEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_UninterpretedOptionEventIter) Next(ctx context.Context) (*UninterpretedOptionEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToUninterpretedOptionEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to UninterpretedOptionEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_UninterpretedOptionEventIter) Close() {
	it.Iter.Close()
}


type SourceCodeInfoEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	SourceCodeInfo *descriptor.SourceCodeInfo
}

type _SourceCodeInfoTopicConfig interface {
	EventToSourceCodeInfoEvent(deq.Event) (*SourceCodeInfoEvent, error)
}

// SourceCodeInfoEventIter is an iterator for SourceCodeInfoEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a SourceCodeInfoEvent.
type SourceCodeInfoEventIter interface {
	Next(ctx context.Context) (*SourceCodeInfoEvent, error)
	Close()
}

type XXX_SourceCodeInfoEventIter struct {
	Iter   deq.EventIter
	Config _SourceCodeInfoTopicConfig
}

// Next returns the next SourceCodeInfoEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_SourceCodeInfoEventIter) Next(ctx context.Context) (*SourceCodeInfoEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToSourceCodeInfoEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to SourceCodeInfoEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_SourceCodeInfoEventIter) Close() {
	it.Iter.Close()
}


type GeneratedCodeInfoEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	GeneratedCodeInfo *descriptor.GeneratedCodeInfo
}

type _GeneratedCodeInfoTopicConfig interface {
	EventToGeneratedCodeInfoEvent(deq.Event) (*GeneratedCodeInfoEvent, error)
}

// GeneratedCodeInfoEventIter is an iterator for GeneratedCodeInfoEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a GeneratedCodeInfoEvent.
type GeneratedCodeInfoEventIter interface {
	Next(ctx context.Context) (*GeneratedCodeInfoEvent, error)
	Close()
}

type XXX_GeneratedCodeInfoEventIter struct {
	Iter   deq.EventIter
	Config _GeneratedCodeInfoTopicConfig
}

// Next returns the next GeneratedCodeInfoEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_GeneratedCodeInfoEventIter) Next(ctx context.Context) (*GeneratedCodeInfoEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToGeneratedCodeInfoEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to GeneratedCodeInfoEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_GeneratedCodeInfoEventIter) Close() {
	it.Iter.Close()
}


