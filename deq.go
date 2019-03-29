package deq

import (
	"context"
	"errors"
	"time"

	"gitlab.com/katcheCode/deq/ack"
)

type Client interface {
	Pub(context.Context, Event) (Event, error)
	Channel(name, topic string) Channel
}

type Channel interface {
	Sub(ctx context.Context, handler SubHandler) error
	Get(ctx context.Context, id string) (Event, error)
	NewEventIter(IterOpts) EventIter
	NewIndexIter(IterOpts) EventIter
	Await(ctx context.Context, eventID string) (Event, error)
	SetEventState(ctx context.Context, id string, state EventState) error
	RequeueEvent(ctx context.Context, e Event, delay time.Duration) error
	Close()
}

type SubHandler func(context.Context, Event) (*Event, ack.Code)

type EventIter interface {
	Next(context.Context) bool
	Event() Event
	Err() error
	Close()
}

type TopicIter interface {
	Next(ctx context.Context) bool
	Topic() string
	Close()
}

// IterOpts defines options for iterators.
type IterOpts struct {
	// Min and Max specify inclusive bounds for the returned results.
	Min, Max string
	// Reversed specifies if the listed results are sorted in reverse order.
	Reversed bool
	// PrefetchCount specifies how many values to prefetch.
	PrefetchCount int
}

/*
DefaultIterOpts are default options for iterators, intended to be used as a starting point for
custom options. For example:

	opts := deq.DefaultIterOpts
	opts.Reversed = true

	iter := channel.NewEventIter(opts)
	defer iter.Close()
*/
var DefaultIterOpts = IterOpts{
	PrefetchCount: 20,
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
