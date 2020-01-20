/*
Package deq provides a generic interface for all DEQ go implementations. The two existing
implementations are gitlab.com/katcheCode/deq/deqdb and gitlab.com/katcheCode/deq/deqclient.
*/
package deq

import (
	"context"
	"time"

	"gitlab.com/katcheCode/deq/deqerr"
	"gitlab.com/katcheCode/deq/deqopt"
)

// Client is a client for accessing the database.
//
// Client is used to publish or delete events and create channels.
type Client interface {
	// Pub publishes an event to the database. If the event's default state is EventStateQueued (the
	// default value), the event is queued on all channels.
	//
	// Events are immutable, so attempting to publish different events with the same topic and ID
	// returns an error. If multiple events with the same payload are published with the same topic
	// and ID, the later events are not published and the previously published event is returned.
	Pub(ctx context.Context, e Event) (Event, error)

	// Channel returns a channel of a certain topic.
	//
	// A channel is active if Close has not been called on any channels returned by this method,
	// including channels from other clients of the same database.
	Channel(name, topic string) Channel

	// Del deletes an event in the database. When an event is deleted, it is also removed from the
	// queue on all channels. Note that this operation breaks the immutability of the database, and
	// is intended as a tool for debugging and emergency fixes only - it is not covered by any
	// stability guarantees and is intended to be replaced with a safe alternative in the future.
	// If an application needs to represent deletion of an event, it is reccomended to add a `deleted`
	// field or some other application-level data to store the object's state.
	Del(ctx context.Context, topic, id string) error
}

// Channel is an event channel in the database.
//
// Each channel has an independent queue and receives events independently of other channels.
type Channel interface {
	// Sub subscribes to the channel's events. As long as the subscription is active, events are read
	// from the channel's queue and handler is called once for each event read. handler's response to
	// each event is used to update the event's state and optionally publish an event in response.
	//
	// See the documentation of SubHandler for details on how handler's return values are used.
	//
	// Sub provides the same delivery guarantees as Next.
	//
	// Sub blocks until the context is canceled or an error occurs. handler is always called on the
	// goroutine that Sub was called on, but handler's response may be processed concurrently with
	// later calls to handler. It is safe to create multiple subscriptions on the same channel
	// concurrently.
	Sub(ctx context.Context, handler SubHandler) error

	// Next returns the next event in the channel's queue after scheduling the event to be resent
	// according to its backoff rate.
	//
	// Next provides no guarantees about the order that events are received or processed.
	// Events queued on a channel are guaranteed to be sent eventually, but may be sent more than once
	// in some circumstances. Events are resent repeatedly with a delay according to their backoff
	// rate until their state is no longer EventStateQueued on the channel. If an event reaches its
	// resend limit, it is dequeued with a state of EventStateResendLimitReached on that channel.
	Next(ctx context.Context) (Event, error)

	// Get gets an event on the channel without affecting its state or position in the queue. See
	// package deqopt for available options.
	Get(ctx context.Context, id string, options ...deqopt.GetOption) (Event, error)

	// BatchGet gets multiple events on the channel without affecting their state or position in the
	// queue. See option deqopt for available options.
	BatchGet(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]Event, error)

	// NewEventIter creates a new iterator over events by ID. Passing nil for opts is the same as
	// passing a pointer to the zero value.
	NewEventIter(*IterOptions) EventIter

	// NewEventIter creates a new iterator over events by the index. Passing nil for opts is the same
	// as passing a pointer to the zero value.
	NewIndexIter(*IterOptions) EventIter

	// SetEventState sets the state of an event on the channel.
	SetEventState(ctx context.Context, id string, state State) error

	// SetIdleTimeout sets the duration of consecutive idling needed for subscriptions on the channel
	// to be cancelled automatically. Defaults to no timeout.
	SetIdleTimeout(time.Duration)

	// SetInitialResendDelay sets the initial send delay of an event's first resend on the channel.
	// This is used as a base value from which any backoff is applied.
	SetInitialResendDelay(time.Duration)

	// Close closes the channel, freeing its resources. Always call Close when done using a channel.
	Close()
}

// SubHandler is a handler for events received through a subscription.
//
// If SubHandler returns a non-nil Event, the event is published as a response. The event
// passed to handler is also updated according to the returned error. Errors created by the the
// gitlab.com/katcheCode/deq/ack package set the state and/or backoff delay of the event according
// to the ack.Code used to create the error. A nil error causes the event's state to be set to OK,
// and any other non-nil error causes the event's state to QUEUED without modifying its backoff
// delay.
type SubHandler func(context.Context, Event) (*Event, error)

// EventIter is an iterator over events of a given topic in the database.
type EventIter interface {
	Next(context.Context) bool
	Event() Event
	Err() error
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
	ErrNotFound = deqerr.New(deqerr.NotFound, "not found")
	// ErrAlreadyExists is returned when creating an event with a key that is in use
	ErrAlreadyExists = deqerr.New(deqerr.Dup, "already exists")
	// ErrVersionMismatch is returned when opening a database with an incorrect format.
	ErrVersionMismatch = deqerr.New(deqerr.Internal, "version mismatch")
	// ErrIterationComplete is returned after an iterator has returned it's last element.
	ErrIterationComplete = deqerr.New(deqerr.NotFound, "iteration complete")
)

const (
	// TopicsName is the topic containing all topic events. This topic holds the
	// names of all other topics published in the database.
	TopicsName = "deq.events.Topic"
)
