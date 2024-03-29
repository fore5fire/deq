package deqclient

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
	api "gitlab.com/katcheCode/deq/api/v1/deq"
	"google.golang.org/grpc"
)

func TestEventIter(t *testing.T) {
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
			Selector:   "1",
		},
		{
			ID:         "2",
			Topic:      "abc",
			CreateTime: now,
			Selector:   "2",
		},
		{
			ID:         "3",
			Topic:      "abc",
			CreateTime: now,
			Selector:   "3",
		},
		{
			ID:         "4",
			Topic:      "abc",
			CreateTime: now,
			Selector:   "4",
		},
		{
			ID:         "5",
			Topic:      "abc",
			CreateTime: now,
			Selector:   "5",
		},
		{
			ID:         "6",
			Topic:      "abc",
			CreateTime: now,
			Selector:   "6",
		},
	}

	channel := &clientChannel{
		deqClient: testIterClient{
			events: events,
		},
		topic: "abc",
		name:  "123",
	}

	iter := channel.NewEventIter(&deq.IterOptions{
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
}

func TestIndexIter(t *testing.T) {
	ctx := context.Background()

	now := time.Now()

	events := []*api.Event{
		{
			Id:         "1",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"1"},
		},
		{
			Id:         "2",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"2"},
		},
		{
			Id:         "3",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"3"},
		},
		{
			Id:         "4",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"4"},
		},
		{
			Id:         "5",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"5"},
		},
		{
			Id:         "6",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"6"},
		},
	}

	expect := []deq.Event{
		{
			ID:         "1",
			Topic:      "abc",
			CreateTime: now,
			Indexes:    []string{"1"},
			Selector:   "1",
		},
		{
			ID:         "2",
			Topic:      "abc",
			CreateTime: now,
			Indexes:    []string{"2"},
			Selector:   "2",
		},
		{
			ID:         "3",
			Topic:      "abc",
			CreateTime: now,
			Indexes:    []string{"3"},
			Selector:   "3",
		},
		{
			ID:         "4",
			Topic:      "abc",
			CreateTime: now,
			Indexes:    []string{"4"},
			Selector:   "4",
		},
		{
			ID:         "5",
			Topic:      "abc",
			CreateTime: now,
			Indexes:    []string{"5"},
			Selector:   "5",
		},
		{
			ID:         "6",
			Topic:      "abc",
			CreateTime: now,
			Indexes:    []string{"6"},
			Selector:   "6",
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
}

func TestReverseIndexIter(t *testing.T) {
	ctx := context.Background()

	now := time.Now()

	events := []*api.Event{
		{
			Id:         "1",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"1"},
		},
		{
			Id:         "2",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"2"},
		},
		{
			Id:         "3",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"3"},
		},
		{
			Id:         "4",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"4"},
		},
		{
			Id:         "5",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"5"},
		},
		{
			Id:         "6",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"6"},
		},
	}

	expect := []deq.Event{
		{
			ID:         "6",
			Topic:      "abc",
			CreateTime: now,
			Indexes:    []string{"6"},
			Selector:   "6",
		},
		{
			ID:         "5",
			Topic:      "abc",
			CreateTime: now,
			Indexes:    []string{"5"},
			Selector:   "5",
		},
		{
			ID:         "4",
			Topic:      "abc",
			CreateTime: now,
			Indexes:    []string{"4"},
			Selector:   "4",
		},
		{
			ID:         "3",
			Topic:      "abc",
			CreateTime: now,
			Indexes:    []string{"3"},
			Selector:   "3",
		},
		{
			ID:         "2",
			Topic:      "abc",
			CreateTime: now,
			Indexes:    []string{"2"},
			Selector:   "2",
		},
		{
			ID:         "1",
			Topic:      "abc",
			CreateTime: now,
			Indexes:    []string{"1"},
			Selector:   "1",
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
		Reversed:      true,
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
}

func TestIndexIterBounds(t *testing.T) {
	ctx := context.Background()

	now := time.Now()

	events := []*api.Event{
		{
			Id:         "1",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"1"},
		},
		{
			Id:         "2",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"2"},
		},
		{
			Id:         "3",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"3"},
		},
		{
			Id:         "4",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"4"},
		},
		{
			Id:         "5",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"5"},
		},
		{
			Id:         "6",
			Topic:      "abc",
			CreateTime: now.UnixNano(),
			Indexes:    []string{"6"},
		},
	}

	expect := []deq.Event{
		{
			ID:         "3",
			Topic:      "abc",
			CreateTime: now,
			Indexes:    []string{"3"},
			Selector:   "3",
		},
		{
			ID:         "4",
			Topic:      "abc",
			CreateTime: now,
			Indexes:    []string{"4"},
			Selector:   "4",
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
		Min:           "3",
		Max:           "40",
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
}

type testIterClient struct {
	api.DEQClient
	events []*api.Event
}

func (c testIterClient) List(ctx context.Context, in *api.ListRequest, opts ...grpc.CallOption) (*api.ListResponse, error) {
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
		if e.Id >= in.MaxId {
			end = i
			break
		}
	}

	if start+pageSize < end {
		if in.Reversed {
			start = end - pageSize
		} else {
			end = start + pageSize
		}
	}

	resp := &api.ListResponse{
		Events: c.events[start:end],
	}

	if in.Reversed {
		for i, j := 0, len(resp.Events)-1; i < j; i, j = i+1, j-1 {
			resp.Events[i], resp.Events[j] = resp.Events[j], resp.Events[i]
		}
	}

	return resp, nil
}
