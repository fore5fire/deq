package deqopt

// BatchGetOption is an option that can be passed to the BatchGet method of a DEQ client to
// customize its behavior. All GetOptions can also be used as BatchGetOptions.
type BatchGetOption interface {
	isBatchGetOption()
}

// AllowNotFound returns a BatchGetOption that prevents ErrNotFound being returned if one or
// more requested events is not found. Instead, the missing events will be excluded from the result
// map.
func AllowNotFound() BatchGetOption {
	return allowNotFound{}
}

type allowNotFound struct{}

func (allowNotFound) isBatchGetOption() {}

// BatchGetOptionSet is a complied set of BatchGet options.
type BatchGetOptionSet struct {
	AllowNotFound bool
	Await         bool
	UseIndex      bool
}

// NewBatchGetOptionSet compiles a slice of BatchGetOption into a BatchGetOptionSet and returns it.
func NewBatchGetOptionSet(opts []BatchGetOption) BatchGetOptionSet {
	var set BatchGetOptionSet
	for _, opt := range opts {
		switch opt.(type) {
		case allowNotFound:
			set.AllowNotFound = true
		case useIndex:
			set.UseIndex = true
		case await:
			set.Await = true
		}
	}
	return set
}
