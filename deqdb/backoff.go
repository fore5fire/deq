package deqdb

import (
	"math"
	"math/rand"
	"time"
)

type sendDelayFunc func(initial time.Duration, sendCount int) time.Duration

// sendDelayExp returns the send delay for an event with exponential backoff.
//
// When sendCount non-positive, a duration of 0 is returned. Otherwise, the delay calculated as
// initial doubled (sendCount - 1) times.
//
// The maximum delay is capped at one hour to prevent overflow.
func sendDelayExp(initial time.Duration, sendCount int) time.Duration {
	if sendCount < 1 {
		return 0
	}

	trueVal := math.Pow(2, float64(sendCount-1))
	// trueVal doesn't overflow, it's max is +inf, so apply the cap before converting to a duration.
	capped := math.Min(trueVal, float64(time.Hour/initial))
	return time.Duration(capped) * initial
}

// sendDelayLinear returns the send delay for an event with linear backoff.
//
// When sendCount is non-positive, a duration of 0 is returned. Otherwise, the delay is calculated
// as initial doubled (sendCount - 1) times.
//
// The maximum backoff is capped at one hour to prevent overflow.
func sendDelayLinear(initial time.Duration, sendCount int) time.Duration {
	if sendCount < 1 {
		return 0
	}

	trueVal := float64(initial) * float64(sendCount+1)
	capped := math.Min(trueVal, float64(time.Hour))
	return time.Duration(capped)
}

// sendDelayConstant returns the send delay for an event with no backoff.
//
// When sendCount is non-positive, a duration of 0 is returned. Otherwise sendDelayConstant acts as
// an identity function, and simply returns d.
func sendDelayConstant(d time.Duration, sendCount int) time.Duration {
	if sendCount < 1 {
		return 0
	}
	return d
}

func randomSendDelay(min, max time.Duration) time.Duration {
	return time.Duration(rand.Int63n(int64(max-min))) + min
}
