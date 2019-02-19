/*
Package ack provides ack codes for sending acknowledgements to the DEQ server. These are returned by
handlers
*/
package ack

import (
	api "gitlab.com/katcheCode/deq/api/v1/deq"
)

// Code is a code used when acknowledging an event
type Code api.AckCode

const (
	// DequeueOK dequeues an event, indicating it was processed successfully
	DequeueOK = Code(api.AckCode_DEQUEUE_OK)
	// DequeueError dequeues an event, indicating it was not processed successfully
	DequeueError = Code(api.AckCode_DEQUEUE_ERROR)
	// RequeueConstant requeues an event with no backoff
	RequeueConstant = Code(api.AckCode_REQUEUE_CONSTANT)
	// RequeueLinear requires an event with a linear backoff
	RequeueLinear = Code(api.AckCode_REQUEUE_LINEAR)
	// RequeueExponential requeues an event with an exponential backoff
	RequeueExponential = Code(api.AckCode_REQUEUE_EXPONENTIAL)
)
