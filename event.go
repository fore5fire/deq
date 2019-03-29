package deq

import "time"

// Event is the fundamental data storage unit for DEQ.
type Event struct {
	// ID is the unique identifier for the event. Use a deterministic ID for request idempotency.
	// Events can be iterated lexicographically by ID using an EventIter
	// Required.
	ID string
	// Topic is the topic to which the event will be sent. Cannot contain the null character.
	// Required.
	Topic string
	// Payload is the arbitrary data this event holds. The structure of payload is generally specified
	// by its topic.
	Payload []byte
	// Indexes specify additional indexes for this event. Indexes are scoped by the topic of the
	// event. Events can be iterated lexicographically by index using an IndexIter. Identical indexes
	// are sorted by event ID. Indexes cannot contain the null character.
	Indexes []string
	// CreateTime is the time the event was created.
	// Defaults to time.Now()
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

// EventState is the state of an event on a specific channel.
type EventState int

const (
	// EventStateUnspecified is the default value of an EventState.
	EventStateUnspecified EventState = iota
	// EventStateQueued indicates that the event is queued on the channel.
	EventStateQueued
	// EventStateDequeuedOK indicates that the event was processed successfully and is not queued on
	// the channel.
	EventStateDequeuedOK
	// EventStateDequeuedError indicates that the event was not processed successfully and is not
	// queued on the channel.
	EventStateDequeuedError
	// DefaultEventState is the default event state for new events if none is specified.
	DefaultEventState = EventStateQueued
)

func (s EventState) String() string {
	switch s {
	case EventStateUnspecified:
		return ""
	case EventStateQueued:
		return "Queued"
	case EventStateDequeuedOK:
		return "DequeuedOK"
	case EventStateDequeuedError:
		return "DequeuedError"
	default:
		return ""
	}
}
