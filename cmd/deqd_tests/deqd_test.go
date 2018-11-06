package main_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/ack"
	"gitlab.com/katcheCode/deq/pkg/test/model"
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

func gatherTestModels(conn *grpc.ClientConn, duration time.Duration) (result []*model.TestModel, err error) {

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	sub := deq.NewSubscriber(conn, deq.SubscriberOpts{
		Channel:     "TestChannel1",
		IdleTimeout: time.Second / 3,
	})

	mut := sync.Mutex{}

	err = sub.Sub(ctx, &model.TestModel{}, func(e deq.Event) ack.Code {
		mut.Lock()
		defer mut.Unlock()
		result = append(result, e.Msg.(*model.TestModel))
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

	p := deq.NewPublisher(conn, deq.PublisherOpts{})
	expected := []*model.TestModel{
		&model.TestModel{
			Msg: "Hello world!",
		},
	}

	expectedE := deq.Event{
		ID:  time.Now().String(),
		Msg: expected[0],
	}

	e, err := p.Pub(ctx, expectedE)
	if err != nil {
		t.Fatalf("Error Creating Event: %v", err)
	}
	expectedE.CreateTime = e.CreateTime
	if !reflect.DeepEqual(expectedE, e) {
		t.Errorf("expected %v, got %v", expectedE, e)
	}

	// TODO: fix test if server time is out of sync with local time... or just move to unit test
	// t.Logf("Event ID: %v", e.GetId())
	// createTime := deq.TimeFromID(e.GetId())
	// afterTime := time.Now()
	//
	// if createTime.Before(beforeTime) || createTime.After(afterTime) {
	// 	t.Fatalf("Created event id has incorrect create time. Expected between %v and %v, got %v", beforeTime, afterTime, createTime)
	// }

	messages, err := gatherTestModels(conn, time.Second)
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}
	if !reflect.DeepEqual(expected, messages) {
		t.Fatalf("Sub: expected %v, got %v", expected, messages)
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

	p := deq.NewPublisher(conn, deq.PublisherOpts{})
	expected := []*model.TestModel{
		&model.TestModel{
			Msg: "Hello world!",
		},
	}

	expectedE := deq.Event{
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
	expectedE.CreateTime = e.CreateTime
	if !reflect.DeepEqual(expectedE, e) {
		t.Errorf("expected %v, got %v", expectedE, e)
	}

	expectedE.Msg = &model.TestModel{
		Msg: "Hello world #2!",
	}
	e, err = p.Pub(ctx, expectedE)
	if err == nil {
		t.Fatalf("allowed duplicate keys with different payloads")
	}
}

func TestMassPublish(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	p := deq.NewPublisher(conn, deq.PublisherOpts{})
	now := time.Now().UnixNano()

	for i := 0; i < 500; i++ {
		_, err := p.Pub(ctx, deq.Event{
			ID: fmt.Sprintf("%d-%.3d", now, i),
			Msg: &model.TestModel{
				Msg: fmt.Sprintf("Test Message - %.3d", i),
			},
		})
		if err != nil {
			t.Fatalf("Error Creating Event: %v", err)
		}
	}

	events1, err := gatherTestModels(conn, time.Second*8)
	if err != nil {
		t.Fatalf("Error streaming events: %v", err)
	}

	for i := 500; i < 1000; i++ {
		_, err = p.Pub(ctx, deq.Event{
			ID: fmt.Sprintf("%d-%d", now, i),
			Msg: &model.TestModel{
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

	p := deq.NewPublisher(conn, deq.PublisherOpts{})

	expected, err := p.Pub(ctx, deq.Event{
		ID: "requeue-" + time.Now().String(),
		Msg: &model.TestRequeueModel{
			Msg: "Hello world of requeue!",
		},
	})
	if err != nil {
		t.Fatalf("Error Creating Event: %v", err)
	}

	time.Sleep(time.Second * 8)

	consumer := deq.NewSubscriber(conn, deq.SubscriberOpts{
		Channel:     "TestChannel1",
		IdleTimeout: time.Second * 10,
	})

	var results []deq.Event
	err = consumer.Sub(ctx, &model.TestRequeueModel{}, func(e deq.Event) ack.Code {
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
	if !reflect.DeepEqual(expected, results) {
		t.Errorf("Sub: expected %+v, got %+v", expected, results)
	}
}

func TestNoTimeout(t *testing.T) {
	t.Parallel()

	sub := deq.NewSubscriber(conn, deq.SubscriberOpts{
		Channel:     "TestChannel1",
		IdleTimeout: 0,
	})

	expected := []deq.Event{
		deq.Event{
			ID: "NoTimeout-TestEvent1",
			Msg: &model.TestNoTimeoutModel{
				Msg: "hello no timeout!",
			},
		},
		deq.Event{
			ID: "NoTimeout-TestEvent2",
			Msg: &model.TestNoTimeoutModel{
				Msg: "hello no timeout!",
			},
		},
		deq.Event{
			ID: "NoTimeout-TestEvent3",
			Msg: &model.TestNoTimeoutModel{
				Msg: "hello no timeout!",
			},
		},
	}

	deadline := time.Now().Add(time.Second * 4)

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	var subErr error
	events := make(chan deq.Event)
	go func() {
		subErr = sub.Sub(ctx, &model.TestNoTimeoutModel{}, func(e deq.Event) ack.Code {
			events <- e
			return ack.DequeueOK
		})
		close(events)
	}()

	pub := deq.NewPublisher(conn, deq.PublisherOpts{})

	for i, e := range expected {
		created, err := pub.Pub(ctx, e)
		if err != nil {
			t.Fatalf("pub: %v", err)
		}
		next, ok := <-events
		if !ok {
			t.Fatalf("stream closed before %d", i)
		}
		if !reflect.DeepEqual(created.ID, next.ID) {
			t.Errorf("%d: expected %v, got %v", i, created.ID, next.ID)
		}
	}

	for e := range events {
		t.Errorf("extra event %v", e)
	}

	// Allow one millisecond of lee-way
	endTime := time.Now().Add(time.Millisecond)

	if grpc.Code(subErr) != codes.DeadlineExceeded {
		t.Fatal(subErr)
	}
	if endTime.Before(deadline) {
		t.Errorf("sub ended %v before deadline", deadline.Sub(endTime))
	}
}
