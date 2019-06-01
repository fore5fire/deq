package deqopt

// GetOption is an option that can be passed to the Get method of a DEQ client to customize its
// behavior.
type GetOption interface {
	BatchGetOption
	isGetOption()
}

// UseIndex returns a GetOption that makes Get or BatchGet use the index to find results.
func UseIndex() GetOption {
	return useIndex{}
}

type useIndex struct{}

func (useIndex) isGetOption()      {}
func (useIndex) isBatchGetOption() {}

// Await returns a GetOption that makes Get or BatchGet block until the requested events have been
// published.
func Await() GetOption {
	return await{}
}

type await struct{}

func (await) isGetOption()      {}
func (await) isBatchGetOption() {}

// GetOptionSet is a complied set of BatchGet options.
type GetOptionSet struct {
	Await    bool
	UseIndex bool
}

// NewGetOptionSet compiles a slice of GetOption into a GetOptionSet and returns it.
func NewGetOptionSet(opts []GetOption) GetOptionSet {
	var set GetOptionSet
	for _, opt := range opts {
		switch opt.(type) {
		case useIndex:
			set.UseIndex = true
		case await:
			set.Await = true
		}
	}
	return set
}
