package priority

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
)

func TestPush(t *testing.T) {

	now := time.Now()
	events := []*deq.Event{
		{
			ID: "0",
		},
		{
			ID: "1",
		},
		{
			ID: "2",
		},
		{
			ID: "3",
		},
	}

	priority := now.Add(time.Second * 2)
	expect := &Queue{
		head: &node{
			Event:    events[2],
			Priority: now.Add(time.Second * 2),
		},
	}
	actual := NewQueue()

	actual.Push(events[2], priority)

	if !cmp.Equal(expect.head, actual.head) {
		t.Errorf("push to empty: \n%v", cmp.Diff(expect.head, actual.head))
	}

	expect.head = &node{
		Event:    events[0],
		Priority: now,
		Next:     expect.head,
	}

	actual.Push(events[0], now)
	if !cmp.Equal(expect.head, actual.head) {
		t.Errorf("push before head: %v", cmp.Diff(expect.head, actual.head))
	}

	priority = now.Add(time.Second * 3)
	expect.head.Next.Next = &node{
		Event:    events[3],
		Priority: priority,
	}

	actual.Push(events[3], priority)
	if !cmp.Equal(expect.head, actual.head) {
		t.Errorf("push to end: %v", cmp.Diff(expect.head, actual.head))
	}

	priority = now.Add(time.Second * 1)
	expect.head.Next = &node{
		Event:    events[1],
		Priority: priority,
		Next:     expect.head.Next,
	}

	actual.Push(events[1], priority)
	if !cmp.Equal(expect.head, actual.head) {
		t.Errorf("push after head: %v", cmp.Diff(expect.head, actual.head))
	}
}

func TestPop(t *testing.T) {

	now := time.Now()
	events := []*deq.Event{
		{
			ID: "0",
		},
		{
			ID: "1",
		},
		{
			ID: "2",
		},
		{
			ID: "3",
		},
	}

	q := &Queue{
		head: &node{
			Event:    events[0],
			Priority: now,
			Next: &node{
				Event:    events[1],
				Priority: now.Add(time.Second),
				Next: &node{
					Event:    events[2],
					Priority: now.Add(time.Second * 2),
					Next: &node{
						Event:    events[3],
						Priority: now.Add(time.Second * 3),
					},
				},
			},
		},
	}

	for i, expect := range events {
		expectPriority := now.Add(time.Second * time.Duration(i))

		actual, actualPriority := q.Pop()
		if !cmp.Equal(expect, actual) {
			t.Errorf("pop %d: verify event: \n%s", i, cmp.Diff(expect, actual))
		}
		if expectPriority != actualPriority {
			t.Errorf("pop %d: verify priority: \n%s", i, cmp.Diff(expectPriority, actualPriority))
		}
	}
	if q.head != nil {
		t.Errorf("pop last: \n%s", cmp.Diff((*node)(nil), q.head))
	}
}

func TestPeek(t *testing.T) {

	now := time.Now()
	events := []*deq.Event{
		{
			ID: "0",
		},
		{
			ID: "1",
		},
		{
			ID: "2",
		},
		{
			ID: "3",
		},
	}

	q := &Queue{
		head: &node{
			Event:    events[0],
			Priority: now,
			Next: &node{
				Event:    events[1],
				Priority: now.Add(time.Second),
				Next: &node{
					Event:    events[2],
					Priority: now.Add(time.Second * 2),
					Next: &node{
						Event:    events[3],
						Priority: now.Add(time.Second * 3),
					},
				},
			},
		},
	}

	for i, expect := range events {
		expectPriority := now.Add(time.Second * time.Duration(i))

		actual, actualPriority := q.Peek()

		if !cmp.Equal(expect, actual) {
			t.Errorf("peek %d: verify event: \n%s", i, cmp.Diff(expect, actual))
		}
		if expectPriority != actualPriority {
			t.Errorf("peek %d: verify priority: \n%s", i, cmp.Diff(expectPriority, actualPriority))
		}

		q.head = q.head.Next
	}
}
