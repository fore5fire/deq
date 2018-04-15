package main_test

import (
	"context"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc"
	"os"
	pb "src.katchecode.com/event-store-tests/eventstore"
	"src.katchecode.com/event-store-tests/model"
	"testing"
	"time"
)

func TestQuery(t *testing.T) {

	conn, err := grpc.Dial(os.Getenv("TEST_TARGET_ENDPOINT"), grpc.WithInsecure())

	if err != nil {
		t.Fatalf("Failed to connect: %v\n", err)
	}
	defer conn.Close()

	c := pb.NewEventStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	payload, err := ptypes.MarshalAny(&model.TestModel{
		Msg: "Hello world!",
	})

	if err != nil {
		t.Fatalf("Error marshaling model.TestModel: %v", err)
	}

	_, err = c.CreateEvent(ctx, &pb.CreateEventRequest{
		Event: &pb.Event{
			Payload: payload,
		},
	})
	if err != nil {
		t.Fatalf("Error Creating Event: %v\n", err)
	}

	stream, err := c.ListEvents(ctx, &pb.ListEventsRequest{})

	if err != nil {
		t.Fatalf("Error opening list events stream: %v\n", err)
	}

	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Error receiving event: %v\n", err)
	}

	testModel := &model.TestModel{}

	err = ptypes.UnmarshalAny(response.GetEvent().GetPayload(), testModel)
	if err != nil {
		t.Fatalf("Error unmarshaling payload: %v\n", err)
	}

	if testModel.GetMsg() != "Hello world!" {
		t.Fatalf("Incorrect message: %s\n", testModel.GetMsg())
	}
}
