package deqclient

import (
	"testing"

	"gitlab.com/katcheCode/deq"
	api "gitlab.com/katcheCode/deq/api/v1/deq"
)

func TestIndexIter(t *testing.T) {
	channel := &clientChannel{
		deqClient: testIterClient{},
		topic:     "abc",
		name:      "123",
	}

	channel.NewIndexIter(deq.IterOptions{})
}

type testIterClient struct {
	api.DEQClient
}
