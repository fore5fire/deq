package deqopt

import "time"

// SyncOption is an option that can be passed to deq syncing methods ot customize their behavior.
type SyncOption interface {
	isSyncOption()
}

type SubOption interface {
	SyncOption
	isSubOption()
}

// IdleTimeout returns a SubOption that makes Sync or Sub return if idle for no less than d.
func IdleTimeout(d time.Duration) SubOption {
	return idleTimeout(d)
}

type idleTimeout time.Duration

func (idleTimeout) isSyncOption() {}
func (idleTimeout) isSubOption()  {}

// SyncOptionSet is a
type SyncOptionSet struct {
	IdleTimeout time.Duration
}

// NewSyncOptionSet compiles a slice of SyncOption into a SyncOptionSet and returns it.
func NewSyncOptionSet(opts []SyncOption) SyncOptionSet {
	var set SyncOptionSet
	for _, opt := range opts {
		switch opt := opt.(type) {
		case idleTimeout:
			set.IdleTimeout = time.Duration(opt)
		}
	}
	return set
}
