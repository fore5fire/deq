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
	DefaultState State
	// EventState is the state of the event in the channel it is received on.
	// Output only.
	State State
	// SendCount is the number of attempts to send the event to the channel it is received on.
	// Output only.
	SendCount int
	// Selector is the ID or index used to retrieve the event.
	// Output only.
	Selector string
}

// State is the state of an event on a specific channel.
type State int

const (
	// StateUnspecified is the default value of an EventState.
	StateUnspecified State = 0

	// StateQueued indicates that the event is queued on the channel with exponential backoff. The
	// send delay for the event is calculated as 2^send_count * initial once send_count is greater
	// than zero.
	StateQueued State = 1
	// StateQueuedLinear indicates that the event is queued on the channel with linear backoff. The
	// send delay for the event is calculated as send_count * initial.
	StateQueuedLinear State = 7
	// StateQueuedConstant indicates that the event is queued on the channel with no backoff. The
	// send delay for the event is equal to the initial rate once send_count is greater than zero.
	StateQueuedConstant State = 8

	// StateOK indicates that the event was processed successfully and is not queued on
	// the channel.
	StateOK State = 2
	// StateInvalid indicates that the event has one or more fields has an invalid value. The
	// event's creator should create a new event with the invalid fields corrected.
	StateInvalid State = 4
	// StateInternal indicates that the event encountered an internal error durring processing.
	StateInternal State = 5
	// StateSendLimitReached indicates that the event was dequeued automatically after reaching its
	// send limit.
	StateSendLimitReached State = 6

	// StateDequeuedError is a legacy option. Use StateInvalid or StateInternal instead.
	StateDequeuedError State = 3

	// DefaultState is the default event state for new events if none is specified.
	DefaultState = StateQueued
)

func (s State) String() string {
	switch s {
	case StateUnspecified:
		return ""
	case StateQueued:
		return "Queued"
	case StateQueuedLinear:
		return "QueuedLinear"
	case StateQueuedConstant:
		return "QueuedConstant"
	case StateOK:
		return "OK"
	case StateInvalid:
		return "Invalid"
	case StateInternal:
		return "Internal"
	case StateSendLimitReached:
		return "SendLimitReached"
	case StateDequeuedError:
		return "DequeuedError"
	default:
		return "UnrecognizedState"
	}
}

// MarshalYAML returns the string value of the event for use in marshalling YAML documents.
func (s State) MarshalYAML() (interface{}, error) {
	return s.String(), nil
}
