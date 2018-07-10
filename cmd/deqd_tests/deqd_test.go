package main_test

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/types"
	"gitlab.com/katcheCode/deqd/api/v1/deq"
	"gitlab.com/katcheCode/deqd/pkg/test/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

func gatherTestModels(client deq.DEQClient, duration time.Duration) (result []model.TestModel, err error) {
	log.Println("Gathering Test Models")

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	stream, err := client.StreamEvents(ctx, &deq.StreamEventsRequest{
		Channel: "TestChannel1",
	})
	if err != nil {
		return nil, err
	}

	for {
		// log.Println("Receiving events...")
		response, err := stream.Recv()
		if status.Code(err) == codes.DeadlineExceeded {
			err = stream.CloseSend()
			if err != nil {
				return nil, err
			}
			return result, nil
		}
		if err != nil {
			return nil, err
		}

		testModel := model.TestModel{}

		err = types.UnmarshalAny(response.GetPayload(), &testModel)
		if err != nil {
			return nil, err
		}

		// log.Println("Got result")
		// log.Println(testModel)

		result = append(result, testModel)
	}
}

func createEvent(client deq.DEQClient, m model.TestModel, timeout time.Duration) (*deq.Event, error) {
	payload, err := types.MarshalAny(&m)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// log.Println("Creating event")
	return client.CreateEvent(ctx, &deq.CreateEventRequest{
		Event: &deq.Event{
			Payload: payload,
		},
	})
}

func TestCreateAndReceive(t *testing.T) {
	log.Println("Starting Test")

	conn, err := grpc.Dial(os.Getenv("TEST_TARGET_URL"), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to connect: %v\n", err)
	}
	defer conn.Close()

	c := deq.NewDEQClient(conn)

	events, err := gatherTestModels(c, time.Second)
	if err == nil && len(events) > 0 {
		t.Fatalf("Received event when none was created: %v\n", events)
	}
	if err != nil {
		t.Fatalf("Error streaming events: %v\n", err)
	}

	wg := sync.WaitGroup{}
	var eventsErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		events, eventsErr = gatherTestModels(c, time.Second*5)
	}()

	payload, err := types.MarshalAny(&model.TestModel{
		Msg: "Hello world!",
	})
	if err != nil {
		t.Fatalf("Error marshaling model.TestModel: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// beforeTime := time.Now()

	// log.Println("Creating event")
	_, err = c.CreateEvent(ctx, &deq.CreateEventRequest{
		Event: &deq.Event{
			Payload: payload,
		},
	})
	if err != nil {
		t.Fatalf("Error Creating Event: %v\n", err)
	}

	// TODO: fix test if server time is out of sync with local time... or just move to unit test
	// t.Logf("Event ID: %v", e.GetId())
	// createTime := deq.TimeFromID(e.GetId())
	// afterTime := time.Now()
	//
	// if createTime.Before(beforeTime) || createTime.After(afterTime) {
	// 	t.Fatalf("Created event id has incorrect create time. Expected between %v and %v, got %v", beforeTime, afterTime, createTime)
	// }

	wg.Wait()

	if eventsErr != nil {
		t.Fatalf("Error streaming events: %v\n", err)
	}
	if len(events) == 0 {
		t.Fatalf("Expected to get message but recieved none")
	}
	if m := events[0]; m.GetMsg() != "Hello world!" {
		t.Fatalf("Incorrect message: %s\n", m.GetMsg())
	}
}

func TestRequeueTimeout(t *testing.T) {
	log.Println("Starting TestRequeueTimeout")

	conn, err := grpc.Dial(os.Getenv("TEST_TARGET_URL"), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to connect: %v\n", err)
	}
	defer conn.Close()

	c := deq.NewDEQClient(conn)

	for i := 0; i < 500; i++ {
		_, err = createEvent(c, model.TestModel{
			Msg: fmt.Sprintf("Test Message - %d", i),
		}, time.Second*10)
		if err != nil {
			t.Fatalf("Error Creating Event: %v\n", err)
		}
	}

	wg := sync.WaitGroup{}
	var eventsErr error
	var events []model.TestModel
	wg.Add(1)
	go func() {
		defer wg.Done()
		events, eventsErr = gatherTestModels(c, time.Second*8)
	}()

	for i := 500; i < 1000; i++ {
		_, err = createEvent(c, model.TestModel{
			Msg: fmt.Sprintf("Test Message - %d", i),
		}, time.Second*10)
		if err != nil {
			t.Fatalf("Error Creating Event: %v\n", err)
		}
	}

	wg.Wait()

	// log.Println(events)

	if eventsErr != nil {
		t.Fatalf("Error streaming events: %v\n", err)
	}

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
