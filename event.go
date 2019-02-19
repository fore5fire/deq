package deq

import "time"

// Event is the fundamental data storage unit for DEQ.
type Event struct {
	// ID is the unique identifier for the event. Use a deterministic ID for request idempotency.
	// Required.
	ID string
	// Topic is the topic to which the event will be sent. Cannot contain the null character.
	// Required.
	Topic string
	// Payload is the arbitrary data this event holds. The structure of payload is generally specified
	// by its topic.
	Payload []byte
	// CreateTime is the time the event was created.
	// Output only.
	CreateTime time.Time
	// DefaultState is the initial state of this event for existing channels. If not EventStateQueued,
	// the event will be created but not sent to subscribers of topic.
	DefaultState EventState
	// EventState is the state of the event in the channel it is recieved on.
	// Output only.
	State EventState
	// RequeueCount is the number of attempts to send the event to the channel it is recieved on.
	// Output only.
	RequeueCount int
}

type EventState int

const (
	EventStateUnspecified EventState = iota
	EventStateQueued
	EventStateDequeuedOK
	EventStateDequeuedError
)
