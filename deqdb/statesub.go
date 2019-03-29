package deqdb

import (
	"context"
	"errors"

	"gitlab.com/katcheCode/deq"
)

// EventStateSubscription allows you to get updates when a particular event's state is updated.
type EventStateSubscription struct {
	C <-chan deq.EventState
	c chan deq.EventState

	eventID string
	channel *Channel
	missed  bool
	latest  deq.EventState
}

var (
	// ErrSubscriptionClosed indicates that the operation was interrupted because Close() was called
	// on the EventStateSubscription.
	ErrSubscriptionClosed = errors.New("subscription closed")
)

// NewEventStateSubscription returns a new EventStateSubscription.
//
// EventStateSubscription begins caching updates as soon as it's created, even before
// Next is called. Close should always be called when the EventStateSubscription
// is no longer needed.
// Example usage:
//
//   sub := channel.NewEventStateSubscription(id)
// 	 defer sub.Close()
// 	 for {
// 	   state := sub.Next(ctx)
//   	 if state != EventState_QUEUED {
//       break
//     }
//   }
//
func (c *Channel) NewEventStateSubscription(id string) *EventStateSubscription {

	sub := &EventStateSubscription{
		eventID: id,
		channel: c,
		c:       make(chan deq.EventState, 3),
	}
	sub.C = sub.c

	c.shared.stateSubsMutex.Lock()
	defer c.shared.stateSubsMutex.Unlock()

	subs := c.shared.stateSubs[id]
	if subs == nil {
		subs = make(map[*EventStateSubscription]struct{})
		c.shared.stateSubs[id] = subs
	}
	subs[sub] = struct{}{}

	return sub
}

// Next blocks until an update is recieved, then returns the new state.
//
// It's possible for a subscription to miss updates if its internal buffer is full. In this case,
// it will skip earlier updates while preserving update order, such that the current state is always
// at the end of a full buffer.
func (sub *EventStateSubscription) Next(ctx context.Context) (deq.EventState, error) {
	select {
	case <-ctx.Done():
		return deq.EventStateUnspecified, ctx.Err()
	case state, ok := <-sub.C:
		if !ok {
			return deq.EventStateUnspecified, ErrSubscriptionClosed
		}
		return state, nil
	}
}

// Close closes the EventStateSubscription.
//
// This method should always be called when the EventStateSubscription is no longer needed.
// For example:
//
// 	sub := channel.NewEventStateSubscription(id)
// 	defer sub.Close()
//
func (sub *EventStateSubscription) Close() {
	sub.channel.shared.stateSubsMutex.Lock()
	defer sub.channel.shared.stateSubsMutex.Unlock()

	subs := sub.channel.shared.stateSubs[sub.eventID]
	delete(subs, sub)
	if len(subs) == 0 {
		delete(sub.channel.shared.stateSubs, sub.eventID)
	}

	close(sub.c)
}

// add adds an event to this subscription. It is not safe for concurrent use.
func (sub *EventStateSubscription) add(state deq.EventState) {
	// If our buffer is full, drop the least recent update
	if len(sub.c) == cap(sub.c) {
		<-sub.c
	}
	sub.c <- state
}
