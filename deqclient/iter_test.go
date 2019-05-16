package deqclient

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
	api "gitlab.com/katcheCode/deq/api/v1/deq"
	"google.golang.org/grpc"
)

func TestIndexIter(t *testing.T) {
	ctx := context.Background()

	now := time.Now()

	events := []*api.Event{
		{
			Id:         "1",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
		},
		{
			Id:         "2",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
		},
		{
			Id:         "3",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
		},
		{
			Id:         "4",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
		},
		{
			Id:         "5",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
		},
		{
			Id:         "6",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
		},
	}

	expect := []deq.Event{
		{
			ID:         "1",
			Topic:      "abc",
			CreateTime: now,
		},
		{
			ID:         "2",
			Topic:      "abc",
			CreateTime: now,
		},
		{
			ID:         "3",
			Topic:      "abc",
			CreateTime: now,
		},
		{
			ID:         "4",
			Topic:      "abc",
			CreateTime: now,
		},
		{
			ID:         "5",
			Topic:      "abc",
			CreateTime: now,
		},
		{
			ID:         "6",
			Topic:      "abc",
			CreateTime: now,
		},
	}

	channel := &clientChannel{
		deqClient: testIterClient{
			events: events,
		},
		topic: "abc",
		name:  "123",
	}

	iter := channel.NewIndexIter(&deq.IterOptions{
		PrefetchCount: -1,
	})
	defer iter.Close()

	var actual []deq.Event
	for iter.Next(ctx) {
		actual = append(actual, iter.Event())
	}
	if iter.Err() != nil {
		t.Fatalf("next: %v", iter.Err())
	}
	if !cmp.Equal(expect, actual) {
		t.Errorf("\n%s", cmp.Diff(expect, actual))
	}
	t.Fatal()
}

type testIterClient struct {
	api.DEQClient
	events []*api.Event
}

func (c testIterClient) List(ctx context.Context, in *api.ListRequest, opts ...grpc.CallOption) (*api.ListResponse, error) {
	log.Println(in)
	pageSize := 20
	if in.PageSize != 0 {
		pageSize = int(in.PageSize)
	}

	start := len(c.events)
	for i, e := range c.events {
		if e.Id >= in.MinId {
			start = i
			break
		}
	}
	end := len(c.events)
	for i, e := range c.events {
		if e.Id > in.MaxId {
			end = i
			break
		}
	}

	if start+pageSize < end {
		end = start + pageSize
	}
	log.Println(start, end)
	return &api.ListResponse{
		Events: c.events[start:end],
	}, nil
}
