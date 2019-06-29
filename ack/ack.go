/*
Package ack provides ack codes for sending acknowledgements to the DEQ server. These are returned by
handlers
*/
package ack

import (
	"fmt"

	api "gitlab.com/katcheCode/deq/api/v1/deq"
)

// Code is a code used when acknowledging an event.
type Code api.AckCode

const (
	// Unspecified is the default value for a Code
	Unspecified Code = Code(api.AckCode_UNSPECIFIED)
	// OK indicates the event was processed successfully.
	//
	// Events marked OK are dequeued.
	OK = Code(api.AckCode_OK)
	// Invalid indicates the event was not processed successfully because the event is invalid.
	//
	// Events marked OK are dequeued.
	Invalid = Code(api.AckCode_INVALID)
	// Internal indicates the event was not processed successfully because an internal error occurred.
	//
	// Events marked internal are dequeued.
	Internal = Code(api.AckCode_INTERNAL)

	// Requeue requeues the event with exponential backoff
	Requeue = Code(api.AckCode_REQUEUE)
	// RequeueLinear requires an event with a linear backoff
	RequeueLinear = Code(api.AckCode_REQUEUE_LINEAR)
	// RequeueConstant requeues an event with no backoff
	RequeueConstant = Code(api.AckCode_REQUEUE_CONSTANT)

	// NoOp explicitly has no effect.
	NoOp = Code(-1)

	// DequeueOK is a legacy code. Use OK instead.
	//
	// DequeueOK dequeues an event, indicating it was processed successfully.
	DequeueOK = Code(api.AckCode_OK)
	// DequeueError is a legacy code. Use Invalid or Internal insead.
	//
	// DequeueError dequeues an event, indicating it was not processed successfully due to an error.
	DequeueError = Code(api.AckCode_DEQUEUE_ERROR)
	// RequeueExponential is a legacy code. Use Requeue instead.
	//
	// RequeueExponential requeues an event with an exponential backoff
	RequeueExponential = Code(api.AckCode_REQUEUE)
)

// Error returns an error containing the specified code, which can be returned from a handler to
// indicate the desired response action. The argument a is used exactly as in fmt.Sprint to define
// the response message.
func Error(code Code, a ...interface{}) error {
	return ackError{
		message: fmt.Sprint(a...),
		code:    code,
	}
}

// Errorf returns an error containing the specified code, which can be returned from a handler to
// indicate the desired response action. The argument a is used exactly as in fmt.Sprintf to define
// the response message.
func Errorf(code Code, format string, a ...interface{}) error {
	return ackError{
		message: fmt.Sprintf(format, a...),
		code:    code,
	}
}

// ErrorCode returns the code for an error.
//
// If the error is nil, DequeueOK is returned. If the error was created by Error or Errorf in this
// package, the code used to create the error will be returned. Otherwise, RequeueExponential will
// be returned.
func ErrorCode(err error) Code {
	switch err := err.(type) {
	case ackError:
		return err.code
	case nil:
		return DequeueOK
	default:
		return RequeueExponential
	}
}

type ackError struct {
	message string
	code    Code
}

func (e ackError) Error() string {
	return e.message
}
