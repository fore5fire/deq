package deq

import (
	"context"
	"errors"
	"time"

	"gitlab.com/katcheCode/deq/deqopt"
)

// Client is a client for accessing the database.
//
// Client is used to publish or delete events and create channels.
type Client interface {
	Pub(ctx context.Context, e Event) (Event, error)
	Del(ctx context.Context, topic, id string) error
	Channel(name, topic string) Channel
}

// Channel is an event channel in the database.
//
// Each channel has an independant queue and recieves events independantly of other channels.
type Channel interface {
	Sub(ctx context.Context, handler SubHandler) error
	Get(ctx context.Context, id string, options ...deqopt.GetOption) (Event, error)
	BatchGet(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]Event, error)
	// NewEventIter creates a new iterator over events by ID. Passing nil for opts is the same as
	// passing a pointer to the zero value.
	NewEventIter(*IterOptions) EventIter
	// NewEventIter creates a new iterator over events by the index. Passing nil for opts is the same
	// as passing a pointer to the zero value.
	NewIndexIter(*IterOptions) EventIter
	SetEventState(ctx context.Context, id string, state State) error
	RequeueEvent(ctx context.Context, e Event, delay time.Duration) error
	Close()
}

// SubHandler is a handler for events recieved through a subscription.
type SubHandler func(context.Context, Event) (*Event, error)

// EventIter is an iterator over events of a given topic in the database.
type EventIter interface {
	Next(context.Context) bool
	Event() Event
	Err() error
	Close()
}

// TopicIter is an iterator over topics with one or more events in the database.
type TopicIter interface {
	Next(ctx context.Context) bool
	Topic() string
	Close()
}

// IterOptions defines options for iterators.
type IterOptions struct {
	// Min and Max specify inclusive bounds for the returned results. Defaults to the entire range.
	Min, Max string
	// Reversed specifies if the listed results are sorted in reverse order.
	Reversed bool
	// PrefetchCount specifies how many values to prefetch. Defaults to a value specified by the
	// the implementation. Set to -1 to disable prefetching. Cannot be less than -1.
	PrefetchCount int
}

var (
	// ErrNotFound is returned when a requested event doesn't exist in the database
	ErrNotFound = errors.New("event not found")
	// ErrAlreadyExists is returned when creating an event with a key that is in use
	ErrAlreadyExists = errors.New("already exists")
	// ErrVersionMismatch is returned when opening a database with an incorrect format.
	ErrVersionMismatch = errors.New("version mismatch")
	// ErrInternal is returned when an internal error occurs
	ErrInternal = errors.New("internal error")
	// ErrIterationComplete is returned after an iterator has returned it's last element.
	ErrIterationComplete = errors.New("iteration complete")
)
