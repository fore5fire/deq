package main_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"gitlab.com/katcheCode/deqd"

	// "gitlab.com/katcheCode/deqd/api/v1/deq"
	"gitlab.com/katcheCode/deqd/pkg/test/model"
	"google.golang.org/grpc"
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

	consumer := deq.NewConsumer(conn, deq.ConsumerOpts{
		Channel: "TestChannel1",
		Follow:  false,
	})

	err = consumer.Sub(ctx, &model.TestModel{}, func(ctx context.Context, e deq.Event) deq.AckCode {
		result = append(result, e.Message.(*model.TestModel))
		return deq.AckCodeDequeueOK
	})
	if err == io.EOF {
		return result, nil
	}
	return nil, err

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

	p := deq.NewProducer(conn, deq.ProducerOpts{})
	expected := &model.TestModel{
		Msg: "Hello world!",
	}

	err := p.Pub(ctx, deq.Event{
		ID:      time.Now().String(),
		Message: expected,
	})
	if err != nil {
		t.Fatalf("Error Creating Event: %v", err)
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
	if len(messages) == 0 {
		t.Fatalf("Sub: no message recieved")
	}
	if m := messages[0]; !proto.Equal(m, expected) {
		t.Fatalf("Sub: expected %v, got %v", expected, m.Msg)
	}
}

func TestRequeueTimeout(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	p := deq.NewProducer(conn, deq.ProducerOpts{})
	now := time.Now().UnixNano()

	for i := 0; i < 500; i++ {
		err := p.Pub(ctx, deq.Event{
			ID: fmt.Sprintf("%d-%d", now, i),
			Message: &model.TestModel{
				Msg: fmt.Sprintf("Test Message - %d", i),
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
		err = p.Pub(ctx, deq.Event{
			ID: fmt.Sprintf("%d-%d", now, i),
			Message: &model.TestModel{
				Msg: fmt.Sprintf("Test Message - %d", i),
			},
		})
		if err != nil {
			t.Fatalf("Error Creating Event: %v", err)
		}
	}
	// log.Println(events)

	events2, err := gatherTestModels(conn, time.Second*8)
	if err != nil {
		t.Fatalf("Error streaming events: %v", err)
	}

	events := append(events1, events2...)

	var missed []int
outer:
	for i := 0; i < 1000; i++ {
		for _, m := range events {
			if m.GetMsg() == fmt.Sprintf("Test Message - %d", i) {
				continue outer
			}
		}
		missed = append(missed, i)
	}

	if len(missed) > 0 {
		t.Fatalf("Missed messages: %v", missed)
	}
}
