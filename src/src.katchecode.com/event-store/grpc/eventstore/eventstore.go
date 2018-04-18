package eventstore

import (
	"context"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	pb "src.katchecode.com/event-store/api/v1/eventstore"
	"src.katchecode.com/event-store/eventstore"
	"src.katchecode.com/event-store/logger"
	"time"
)

var log = logger.With().Str("pkg", "src.katchecode.com/event-store/grpc/eventstore").Logger()
var createEventLog = log.With().Str("handler", "CreateEvent").Logger()
var streamEventsLog = log.With().Str("handler", "StreamEvents").Logger()
var insertEventsLog = log.With().Str("handler", "InsertEvents").Logger()
var updateEventStatusLog = log.With().Str("handler", "UpdateEvent").Logger()
var ensureChannelLog = log.With().Str("handler", "EnsureChannel").Logger()
var getChannelLog = log.With().Str("handler", "GetChannel").Logger()

// Server represents the gRPC server
type Server struct {
	store       *eventstore.Store
	payloadType string
}

// NewServer creates a new event store server initalized with a backing event store
func NewServer(eventStore *eventstore.Store, payloadType string) *Server {
	return &Server{eventStore, payloadType}
}

// CreateEvent implements eventstore.CreateEvent
func (s *Server) CreateEvent(ctx context.Context, in *pb.CreateEventRequest) (*pb.Event, error) {

	e := in.GetEvent()

	if e == nil {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument 'event'")
	}

	recievedType, err := ptypes.AnyMessageName(e.GetPayload())

	if err != nil {
		createEventLog.Debug().Err(err).Msg("Error reading payload type")
		return nil, status.Error(codes.InvalidArgument, "Payload type could not be identified")
	}
	if recievedType != s.payloadType {
		createEventLog.Debug().Msg("Recieved incorrect payload type: " + recievedType)
		return nil, status.Error(codes.InvalidArgument, "Payload type did not match store type")
	}

	// createEventLog.Debug().Interface("event", *e).Msg("creating event...")

	newEvent, err := s.store.Create(*e)
	if err != nil {
		createEventLog.Error().Err(err).Msg("Error creating event")
		return nil, status.Error(codes.Internal, "")
	}

	return &newEvent, nil
}

// StreamEvents implements eventstore.ListEvents
func (s *Server) StreamEvents(in *pb.StreamEventsRequest, stream pb.EventStore_StreamEventsServer) error {

	channelName := in.GetChannel()
	if channelName == "" {
		return status.Error(codes.InvalidArgument, "Missing required argument 'channel'")
	}

	requeueDelay := time.Duration(in.GetRequeueDelayMiliseconds()) * time.Millisecond
	if requeueDelay == 0 {
		requeueDelay = 8000 * time.Millisecond
	}

	streamEventsLog.Debug().Msg("New client streaming events")

	channel := s.store.Channel(in.GetChannel())
	eventc, done := channel.Follow()
	defer close(done)

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
				if err := channel.Err(); err != nil {
					streamEventsLog.Error().Err(err).Msg("Error fetching events")
					return status.Error(codes.Internal, "")
				}
				streamEventsLog.Debug().Msg("Upstream closed, shutting down...")
				return nil
			}

			// streamEventsLog.Debug().Interface("event", e).Msg("sending event...")

			requeue <- e
			err := stream.Send(&e)
			if status.Code(err) == codes.Canceled {
				cancelRequeue <- struct{}{}
				channel.RequeueEvent(e)
				streamEventsLog.Debug().Msg("Client disconnected")
				return nil
			}
			if status.Code(err) == codes.DeadlineExceeded {
				cancelRequeue <- struct{}{}
				channel.RequeueEvent(e)
				streamEventsLog.Debug().Msg("Connection timed out")
				return nil
			}
			if err != nil {
				cancelRequeue <- struct{}{}
				channel.RequeueEvent(e)
				streamEventsLog.Error().Err(err).Msg("Error sending event")
				return status.Error(codes.Internal, "")
			}

			cancelRequeue <- struct{}{}

			// streamEventsLog.Debug().Interface("event", e).Msg("Event sent!")

		// Poll to check if client closed connection
		case <-time.After(time.Second * 5):
			log.Debug().Msg("Polling for client disconnect")
			err := stream.Context().Err()
			if err == context.Canceled || err == context.DeadlineExceeded {
				streamEventsLog.Debug().Msg("Client disconnected")
				return nil
			}
		}
	}
}

// InsertEvents implements eventstore.InsertEvents
func (s *Server) InsertEvents(stream pb.EventStore_InsertEventsServer) error {

	for {
		// TODO: get a downstream done channel so we can end gracefully when the server shuts down
		in, err := stream.Recv()

		if status.Code(err) == codes.DeadlineExceeded || status.Code(err) == codes.Canceled {
			insertEventsLog.Debug().Msg("Client disconnected")
			return nil
		}

		if err != nil {
			insertEventsLog.Debug().Err(err).Msg("Error processing event from client")
			return status.Error(codes.InvalidArgument, "Event was not able to be processed")
		}

		e := in.GetEvent()
		if e == nil {
			return status.Error(codes.InvalidArgument, "Missing required argument 'event'")
		}

		_, err = s.store.Insert(*e)
		if err != nil {
			insertEventsLog.Error().Err(err).Msg("Error inserting event to store")
			return status.Error(codes.Internal, "")
		}
	}
}

// UpdateEventStatus implements EventStore.UpdateEventStatus
func (s *Server) UpdateEventStatus(stream pb.EventStore_UpdateEventStatusServer) error {

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			updateEventStatusLog.Debug().Msg("Got EOF receiving from client")
			return nil
		}
		if err != nil {
			updateEventStatusLog.Error().Err(err).Msg("Error receiving from client")
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
			updateEventStatusLog.Debug().Str("event_status", in.GetEventStatus().String()).Msg("Invalid argument 'event_status'")
			return status.Error(codes.InvalidArgument, "Invalid value for argument 'event_status'")
		}

		err = s.store.Channel(channelName).SetEventStatus(key, eventStatus)
		if err != nil {
			updateEventStatusLog.Error().Err(err).Msg("Error setting event status")
			return status.Error(codes.Internal, "")
		}
	}
}

// GetChannel implements EventStore.GetChannel
func (s *Server) GetChannel(ctx context.Context, in *pb.GetChannelRequest) (*pb.Channel, error) {

	channelName := in.GetName()
	if channelName == "" {
		getChannelLog.Debug().Str("name", channelName).Msg("Missing required argument 'name'")
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
func (s *Server) EnsureChannel(ctx context.Context, in *pb.EnsureChannelRequest) (*empty.Empty, error) {

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

	return new(empty.Empty), nil
}

// func (s *Server) ResetChannel(ctx context.Context, in *pb.ResetChannelRequest) (*pb.ResetChannelResponse, error) {
//
// }
