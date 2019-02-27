package deq

import (
	"math"
	"time"
)

// BackoffFunc is a function that returns the requeue delay for an event.
type BackoffFunc func(Event) time.Duration

// ExponentialBackoff returns a BackoffFunc implementing an exponential backoff based on an event's
// RequeueCount, starting at a duration of d when an event's RequeueCount is 0 and doubling with
// each requeue. The maximum backoff is capped at one hour to prevent overflow.
func ExponentialBackoff(d time.Duration) BackoffFunc {
	return func(e Event) time.Duration {
		trueVal := math.Pow(2, float64(e.RequeueCount))
		// trueVal doesn't overflow, it's max is +inf, so apply the cap before converting to a duration.
		capped := math.Min(trueVal, float64(time.Hour/d))
		return time.Duration(capped) * d
	}
}

// LinearBackoff returns a BackoffFunc implementing a linear backoff based on an event's
// RequeueCount, starting at a duration of d when an event's RequeueCount is 0 and increasing by
// d with each requeue. The maximum backoff is capped at one hour to prevent overflow.
func LinearBackoff(d time.Duration) BackoffFunc {
	return func(e Event) time.Duration {
		trueVal := float64(d) * float64(e.RequeueCount+1)
		capped := math.Min(trueVal, float64(time.Hour))
		return time.Duration(capped)
	}
}
