package eventstore

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/gogo/protobuf/types"
	pb "gitlab.com/katcheCode/deqd/api/v1/deq"
	"gitlab.com/katcheCode/deqd/pkg/eventstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server represents the gRPC server
type Server struct {
	store *eventstore.Store
}

// NewServer creates a new event store server initalized with a backing event store
func NewServer(eventStore *eventstore.Store) *Server {
	return &Server{eventStore}
}

// CreateEvent implements eventstore.CreateEvent
func (s *Server) CreateEvent(ctx context.Context, in *pb.CreateEventRequest) (*pb.Event, error) {

	e := in.GetEvent()

	if e == nil {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument 'event'")
	}

	// createEventLog.Debug().Interface("event", *e).Msg("creating event...")

	newEvent, err := s.store.Create(*e)
	if err != nil {
		log.Printf("create event: %v", err)
		return nil, status.Error(codes.Internal, "")
	}

	return &newEvent, nil
}

// StreamEvents implements eventstore.ListEvents
func (s *Server) StreamEvents(in *pb.StreamEventsRequest, stream pb.DEQ_StreamEventsServer) error {

	channelName := in.GetChannel()
	if channelName == "" {
		return status.Error(codes.InvalidArgument, "Missing required argument 'channel'")
	}

	requeueDelay := time.Duration(in.GetRequeueDelayMiliseconds()) * time.Millisecond
	if requeueDelay == 0 {
		requeueDelay = 8000 * time.Millisecond
	}

	log.Println("New client streaming events")

	channel := s.store.Channel(in.GetChannel())
	eventc, idle := channel.Follow()
	defer channel.Close()

	requeue := make(chan pb.Event, 1)
	cancelRequeue := make(chan struct{}, 1)
	defer close(requeue)

	go func() {
		for e := range requeue {
			select {
			case <-time.After(requeueDelay):
				channel.RequeueEvent(e)
			case <-cancelRequeue:
			}
		}
	}()

	for {

		select {
		case e, ok := <-eventc:
			if !ok {
				// Event stream closed, shutting down...
				err := channel.Err()
				if err != nil {
					log.Printf("read from channel: %v", err)
					return status.Error(codes.Internal, "")
				}
				log.Printf("Upstream closed, shutting down...")
				return nil
			}

			requeue <- e
			err := stream.Send(&e)
			if err != nil {
				cancelRequeue <- struct{}{}
				// TODO: fix duplicate event if we miss requeue window
				channel.RequeueEvent(e)
				log.Printf("send event: %v", err)
				return status.Error(codes.Internal, "")
			}
			// TODO: could this ever cancel the wrong requeue?
			cancelRequeue <- struct{}{}

			// Disconnect on idle if not following
		case <-idle:
			log.Printf("idle")
			if !in.Follow {
				return nil
			}

		// Poll to check if stream closed so we can free up memory
		case <-time.After(time.Second * 5):
			err := stream.Context().Err()
			if err != nil {
				log.Printf("Stream failed: %v", err)
				return nil
			}
		}
	}
}

// InsertEvents implements eventstore.InsertEvents
func (s *Server) InsertEvents(stream pb.DEQ_InsertEventsServer) error {

	for {
		// TODO: get a downstream done channel so we can end gracefully when the server shuts down
		in, err := stream.Recv()

		if status.Code(err) == codes.DeadlineExceeded || status.Code(err) == codes.Canceled {
			log.Printf("Client disconnected")
			return nil
		}

		if err != nil {
			log.Printf("process event from client: %v", err)
			return status.Error(codes.InvalidArgument, "Event was not able to be processed")
		}

		e := in.GetEvent()
		if e == nil {
			return status.Error(codes.InvalidArgument, "Missing required argument 'event'")
		}

		_, err = s.store.Insert(*e)
		if err != nil {
			log.Printf("insert event in store: %v", err)
			return status.Error(codes.Internal, "")
		}
	}
}

// StreamingUpdateEventStatus implements DEQ.StreamingUpdateEventStatus
func (s *Server) StreamingUpdateEventStatus(stream pb.DEQ_StreamingUpdateEventStatusServer) error {

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			log.Print("Got EOF receiving from client")
			return nil
		}
		if err != nil {
			log.Printf("receive from client: %v", err)
			return status.Error(codes.Unknown, "")
		}

		channelName := in.GetChannel()
		if channelName == "" {
			return status.Error(codes.InvalidArgument, "Missing required argument 'channel'")
		}

		key := in.GetKey()
		if len(key) == 0 {
			return status.Error(codes.InvalidArgument, "Missing required argument 'key'")
		}

		var eventStatus eventstore.EventStatus
		switch in.GetEventStatus() {
		case pb.Event_PENDING:
			eventStatus = eventstore.EventStatusPending
		case pb.Event_PROCESSED:
			eventStatus = eventstore.EventStatusPending
		case pb.Event_WILL_NOT_PROCESS:
			eventStatus = eventstore.EventStatusWillNotProcess
		default:
			return status.Error(codes.InvalidArgument, "Invalid value for argument 'event_status'")
		}

		err = s.store.Channel(channelName).SetEventStatus(key, eventStatus)
		if err != nil {
			log.Printf("set event status: %v", err)
			return status.Error(codes.Internal, "")
		}
	}
}

// UpdateEventStatus implements DEQ.UpdateEventStatus
func (s *Server) UpdateEventStatus(ctx context.Context, in *pb.UpdateEventStatusRequest) (*pb.UpdateEventStatusResponse, error) {

	channelName := in.GetChannel()
	if channelName == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument 'channel'")
	}

	key := in.GetKey()
	if len(key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument 'key'")
	}

	var eventStatus eventstore.EventStatus
	switch in.GetEventStatus() {
	case pb.Event_PENDING:
		eventStatus = eventstore.EventStatusPending
	case pb.Event_PROCESSED:
		eventStatus = eventstore.EventStatusPending
	case pb.Event_WILL_NOT_PROCESS:
		eventStatus = eventstore.EventStatusWillNotProcess
	default:
		return nil, status.Error(codes.InvalidArgument, "Invalid value for argument 'event_status'")
	}

	err := s.store.Channel(channelName).SetEventStatus(key, eventStatus)
	if err != nil {
		log.Printf("set event status: %v", err)
		return nil, status.Error(codes.Internal, "")
	}

	return &pb.UpdateEventStatusResponse{}, nil
}

// GetChannel implements DEQ.GetChannel
func (s *Server) GetChannel(ctx context.Context, in *pb.GetChannelRequest) (*pb.Channel, error) {

	channelName := in.GetName()
	if channelName == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument 'name'")
	}

	// channel := s.store.Channel(channelName)
	//
	// settings, err := channel.Settings()
	// if err != nil {
	// 	getChannelLog.Error().Err(err).Msg("Error getting channel")
	// 	return nil, status.Error(codes.Internal, "")
	// }

	result := &pb.Channel{
		Name: channelName,
	}

	return result, nil
}

// EnsureChannel implements EventStore.EnsureChannel
func (s *Server) EnsureChannel(ctx context.Context, in *pb.EnsureChannelRequest) (*types.Empty, error) {

	pbChannel := in.GetChannel()
	if pbChannel == nil {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument 'channel'")
	}

	channelName := pbChannel.GetName()
	if channelName == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument 'channel.name'")
	}

	// channel := s.store.Channel(channelName)
	// err := channel.EnsureSettings()

	return new(types.Empty), nil
}

// func (s *Server) ResetChannel(ctx context.Context, in *pb.ResetChannelRequest) (*pb.ResetChannelResponse, error) {
//
// }
