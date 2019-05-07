//go:generate protoc -I=. -I$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogofaster_out=plugins=grpc,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types:. model.proto

package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/ack"
	"gitlab.com/katcheCode/deq/legacyclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var conn *grpc.ClientConn

func init() {
	var err error
	conn, err = grpc.Dial(os.Getenv("TEST_TARGET_URL"), grpc.WithInsecure())
	if err != nil {
		panic("Failed to connect: " + err.Error())
	}
}

func gatherTestModels(conn *grpc.ClientConn, duration time.Duration) (result []*TestModel, err error) {

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	sub := legacyclient.NewSubscriber(conn, legacyclient.SubscriberOpts{
		Channel:     "TestChannel1",
		IdleTimeout: time.Second / 3,
	})

	mut := sync.Mutex{}

	err = sub.Sub(ctx, &TestModel{}, func(e legacyclient.Event) ack.Code {
		mut.Lock()
		defer mut.Unlock()
		result = append(result, e.Msg.(*TestModel))
		return ack.DequeueOK
	})
	if err != nil {
		return nil, err
	}
	return result, nil

}

func TestCreateAndReceive(t *testing.T) {

	// events, err := gatherTestModels(c, time.Second)
	// if err == nil && len(events) > 0 {
	// 	t.Fatalf("Received event when none was created: %v\n", events)
	// }
	// if err != nil {
	// 	t.Fatalf("Error streaming events: %v", err)
	// }

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// beforeTime := time.Now()

	p := legacyclient.NewPublisher(conn, legacyclient.PublisherOpts{})
	expected := []*TestModel{
		&TestModel{
			Msg: "Hello world!",
		},
	}

	expectedE := legacyclient.Event{
		ID:  time.Now().String(),
		Msg: expected[0],
	}

	e, err := p.Pub(ctx, expectedE)
	if err != nil {
		t.Fatalf("Error Creating Event: %v", err)
	}
	expectedE.State = deq.EventStateQueued
	expectedE.CreateTime = e.CreateTime
	if !cmp.Equal(expectedE, e) {
		t.Errorf("(-want +got)\n%s", cmp.Diff(expectedE, e))
	}

	// TODO: fix test if server time is out of sync with local time... or just move to unit test
	// t.Logf("Event ID: %v", e.GetId())
	// createTime := legacyclient.TimeFromID(e.GetId())
	// afterTime := time.Now()
	//
	// if createTime.Before(beforeTime) || createTime.After(afterTime) {
	// 	t.Fatalf("Created event id has incorrect create time. Expected between %v and %v, got %v", beforeTime, afterTime, createTime)
	// }

	messages, err := gatherTestModels(conn, time.Second)
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}
	if !cmp.Equal(expected, messages) {
		t.Fatalf("Sub: (-want +got)\n%s", cmp.Diff(expected, messages))
	}
}

func TestPubDuplicate(t *testing.T) {

	// events, err := gatherTestModels(c, time.Second)
	// if err == nil && len(events) > 0 {
	// 	t.Fatalf("Received event when none was created: %v\n", events)
	// }
	// if err != nil {
	// 	t.Fatalf("Error streaming events: %v", err)
	// }

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// beforeTime := time.Now()

	p := legacyclient.NewPublisher(conn, legacyclient.PublisherOpts{})
	expected := []*TestModel{
		&TestModel{
			Msg: "Hello world!",
		},
	}

	expectedE := legacyclient.Event{
		ID:  time.Now().String(),
		Msg: expected[0],
	}

	e, err := p.Pub(ctx, expectedE)
	if err != nil {
		t.Fatalf("Error Creating Event: %v", err)
	}
	e, err = p.Pub(ctx, expectedE)
	if err != nil {
		t.Fatalf("Error Creating Event: %v", err)
	}
	expectedE.State = deq.EventStateQueued
	expectedE.CreateTime = e.CreateTime
	if !cmp.Equal(expectedE, e) {
		t.Errorf("(-want +got)\n%s", cmp.Diff(expectedE, e))
	}

	expectedE.Msg = &TestModel{
		Msg: "Hello world #2!",
	}
	e, err = p.Pub(ctx, expectedE)
	if err == nil {
		t.Fatalf("allowed duplicate keys with different payloads")
	}
}

func TestMassPublish(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	p := legacyclient.NewPublisher(conn, legacyclient.PublisherOpts{})
	now := time.Now().UnixNano()

	for i := 0; i < 500; i++ {
		_, err := p.Pub(ctx, legacyclient.Event{
			ID: fmt.Sprintf("%d-%.3d", now, i),
			Msg: &TestModel{
				Msg: fmt.Sprintf("Test Message - %.3d", i),
			},
		})
		if err != nil {
			t.Fatalf("create event %d: %v", i, err)
		}
	}

	events1, err := gatherTestModels(conn, time.Second*8)
	if err != nil {
		t.Fatalf("Error streaming events: %v", err)
	}

	for i := 500; i < 1000; i++ {
		_, err = p.Pub(ctx, legacyclient.Event{
			ID: fmt.Sprintf("%d-%d", now, i),
			Msg: &TestModel{
				Msg: fmt.Sprintf("Test Message - %.3d", i),
			},
		})
		if err != nil {
			t.Fatalf("Error Creating Event: %v", err)
		}
	}

	events2, err := gatherTestModels(conn, time.Second*8)
	if err != nil {
		t.Fatalf("Error streaming events: %v", err)
	}

	events := append(events1, events2...)

	var missed []int
outer:
	for i := 0; i < 1000; i++ {
		for _, m := range events {
			if m.GetMsg() == fmt.Sprintf("Test Message - %.3d", i) {
				continue outer
			}
		}
		missed = append(missed, i)
	}

	if len(missed) > 0 {
		t.Fatalf("Missed messages: %v", missed)
	}
}

func TestRequeue(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	p := legacyclient.NewPublisher(conn, legacyclient.PublisherOpts{})

	expected, err := p.Pub(ctx, legacyclient.Event{
		ID: "requeue-" + time.Now().String(),
		Msg: &TestRequeueModel{
			Msg: "Hello world of requeue!",
		},
	})
	if err != nil {
		t.Fatalf("Error Creating Event: %v", err)
	}
	expected.RequeueCount = 4

	time.Sleep(time.Second * 8)

	consumer := legacyclient.NewSubscriber(conn, legacyclient.SubscriberOpts{
		Channel:     "TestChannel1",
		IdleTimeout: time.Second * 10,
	})

	var results []legacyclient.Event
	err = consumer.Sub(ctx, &TestRequeueModel{}, func(e legacyclient.Event) ack.Code {
		results = append(results, e)
		if e.RequeueCount < 2 {
			return ack.RequeueExponential
		}
		if e.RequeueCount < 4 {
			return ack.RequeueExponential
		}
		if e.RequeueCount < 10 {
			return ack.RequeueExponential
		}
		return ack.DequeueOK
	})
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}
	if !cmp.Equal(expected, results) {
		t.Errorf("Sub: (-want +got)\n%s", cmp.Diff(expected, results))
	}
}

func TestNoTimeout(t *testing.T) {
	t.Parallel()

	sub := legacyclient.NewSubscriber(conn, legacyclient.SubscriberOpts{
		Channel:     "TestChannel1",
		IdleTimeout: 0,
	})

	expected := []legacyclient.Event{
		legacyclient.Event{
			ID: "NoTimeout-TestEvent1",
			Msg: &TestNoTimeoutModel{
				Msg: "hello no timeout!",
			},
		},
		legacyclient.Event{
			ID: "NoTimeout-TestEvent2",
			Msg: &TestNoTimeoutModel{
				Msg: "hello no timeout!",
			},
		},
		legacyclient.Event{
			ID: "NoTimeout-TestEvent3",
			Msg: &TestNoTimeoutModel{
				Msg: "hello no timeout!",
			},
		},
	}

	deadline := time.Now().Add(time.Second * 4)

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	var subErr error
	events := make(chan legacyclient.Event)
	go func() {
		subErr = sub.Sub(ctx, &TestNoTimeoutModel{}, func(e legacyclient.Event) ack.Code {
			events <- e
			return ack.DequeueOK
		})
		close(events)
	}()

	pub := legacyclient.NewPublisher(conn, legacyclient.PublisherOpts{})

	for i, e := range expected {
		created, err := pub.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
		next, ok := <-events
		if !ok {
			t.Fatalf("stream closed before %d", i)
		}
		if !cmp.Equal(created.ID, next.ID) {
			t.Errorf("%d: (-want +got)\n%s", i, cmp.Diff(created.ID, next.ID))
		}
	}

	for e := range events {
		t.Errorf("extra event %v", e)
	}

	// Allow one millisecond of lee-way
	endTime := time.Now()

	if grpc.Code(subErr) != codes.DeadlineExceeded {
		t.Fatal(subErr)
	}
	if endTime.Before(deadline) {
		t.Errorf("sub ended %v before deadline", deadline.Sub(endTime))
	}
}

func TestAwait(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	sub := legacyclient.NewSubscriber(conn, legacyclient.SubscriberOpts{
		Channel: "AwaitTestChannel",
	})
	pub := legacyclient.NewPublisher(conn, legacyclient.PublisherOpts{})

	expected, err := pub.Pub(ctx, legacyclient.Event{
		ID: "id-1",
		Msg: &TestAwaitModel{
			Msg: "abc 123",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var msg TestAwaitModel
	e, err := sub.Await(ctx, "id-1", &msg)
	if !cmp.Equal(e, expected) {
		t.Errorf("(-want +got)\n%s", cmp.Diff(expected, e))
	}

	var awaitErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		e, awaitErr = sub.Await(ctx, "id-2", &msg)
	}()

	time.Sleep(time.Second / 4)

	expected, err = pub.Pub(ctx, legacyclient.Event{
		ID: "id-2",
		Msg: &TestAwaitModel{
			Msg: "abc 123",
		},
	})
	if err != nil {
		t.Fatalf("pub: %v", err)
	}

	wg.Wait()
	if awaitErr != nil {
		t.Fatalf("await: %v", awaitErr)
	}
	if !cmp.Equal(e, expected) {
		t.Errorf("(-want +got)\n%s", cmp.Diff(expected, e))
	}
}
