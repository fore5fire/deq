package legacyclient

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deq"
)

// Event is a deserialized event that is sent to or recieved from deq.
type Event struct {
	ID           string
	Msg          Message
	CreateTime   time.Time
	State        deq.State
	RequeueCount int
}

// Topic returns the topic of this event by inspecting it's Message type, or an empty string if the
// topic could not be determined.
func (e Event) Topic() string {
	return proto.MessageName(e.Msg)
}

// Equal returns true if e and other are equivelant and false otherwise.
func (e Event) Equal(other Event) bool {
	return e.ID == other.ID &&
		proto.Equal(e.Msg, other.Msg) &&
		e.CreateTime == other.CreateTime &&
		e.State == other.State &&
		e.RequeueCount == other.RequeueCount
}

// Message is a message payload that is sent by deq
type Message proto.Message
