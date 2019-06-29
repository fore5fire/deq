package priority

import (
	"time"

	"gitlab.com/katcheCode/deq"
)

// Queue is a priority queue that holds events using their send time as the priority
type Queue struct {
	head *node
}

// NewQueue returns a new empty queue
func NewQueue() *Queue {
	return &Queue{}
}

type node struct {
	Event    *deq.Event
	Priority time.Time
	Next     *node
}

// Add adds an event to the queue, ordered according to the given priority.
func (q *Queue) Add(e *deq.Event, priority time.Time) {

	n := &node{
		Event:    e,
		Priority: priority,
	}

	if q.head == nil || q.head.Priority.After(priority) {
		n.Next = q.head
		q.head = n
		return
	}

	cur := q.head
	for cur.Next != nil && cur.Next.Priority.Before(priority) {
		cur = cur.Next
	}

	n.Next = cur.Next
	cur.Next = n
}

// Pop removes the lowest priority element from the queue, returning the removed event and its
// priority.
func (q *Queue) Pop() (*deq.Event, time.Time) {
	if q.head == nil {
		panic("cannot pop an empty queue")
	}
	e, t := q.head.Event, q.head.Priority
	q.head = q.head.Next
	return e, t
}

// Peek returns the lowest priority event from the queue, along with its priority, without modifying
// the queue. If the queue is empty, peek returns nil and the zero time.
func (q *Queue) Peek() (*deq.Event, time.Time) {
	if q.head == nil {
		return nil, time.Time{}
	}
	return q.head.Event, q.head.Priority
}
