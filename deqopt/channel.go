package deqopt

import "time"

// ChannelOption is an option used to configure a channel.
type ChannelOption interface {
	isChannelOption()
}

// ChannelOptionSet is a complied set of ChannelOptions.
type ChannelOptionSet struct {
	ChannelTime time.Time
}

// NewChannelOptionSet compiles a slice of ChannelOption into a ChannelOptionSet and returns it.
func NewChannelOptionSet(opts []ChannelOption) ChannelOptionSet {
	var set ChannelOptionSet
	for _, opt := range opts {
		switch opt := opt.(type) {
		case channelTime:
			set.ChannelTime = time.Time(opt)
		}
	}
	return set
}

type channelTime time.Time

func (channelTime) isChannelOption() {}

// ChannelTime sets the time of the channel, making the channel ignore any events created after t.
func ChannelTime(t time.Time) ChannelOption {
	return channelTime(t)
}
