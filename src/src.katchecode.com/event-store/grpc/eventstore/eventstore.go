package eventstore

import (
	"context"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	pb "src.katchecode.com/event-store/api/v1/eventstore"
	"src.katchecode.com/event-store/eventstore"
	"src.katchecode.com/event-store/logger"
)

var log = logger.With().Str("pkg", "src.katchecode.com/event-store/grpc/eventstore").Logger()

// Server represents the gRPC server
type Server struct {
	store       *eventstore.Store
	payloadType string
}

// NewServer creates a new event store server initalized with a backing event store
func NewServer(eventStore *eventstore.Store, payloadType string) *Server {
	return &Server{eventStore, payloadType}
}

// GetStoreMeta implements eventstore.GetStoreMeta
func (s *Server) GetStoreMeta(ctx context.Context, in *pb.GetStoreMetaRequest) (*pb.GetStoreMetaResponse, error) {
	meta, err := s.store.GetMeta()
	if err != nil {
		log.Error().Err(err).Msg("Error fetching store meta")
		return nil, status.Error(codes.Internal, "")
	}
	response := &pb.GetStoreMetaResponse{
		Meta: &pb.StoreMeta{
			StoreId: meta.StoreId,
		},
	}
	return response, nil
}

// CreateEvent implements eventstore.CreateEvent
func (s *Server) CreateEvent(ctx context.Context, in *pb.CreateEventRequest) (*pb.CreateEventResponse, error) {

	e := in.GetEvent()

	recievedType, err := ptypes.AnyMessageName(e.GetPayload())

	if err != nil {
		log.Debug().Err(err).Msg("Error reading payload type")
		return nil, status.Error(codes.InvalidArgument, "Payload type could not be identified")
	}
	if recievedType != s.payloadType {
		log.Debug().Msg("Recieved incorrect payload type: " + recievedType)
		return nil, status.Error(codes.InvalidArgument, "Payload type did not match store type")
	}

	err = s.store.Create(e)
	if err != nil {
		log.Error().Err(err).Msg("Error creating event")
		return nil, status.Error(codes.Internal, "")
	}

	response := &pb.CreateEventResponse{}
	return response, nil
}

// ListEvents implements eventstore.ListEvents
func (s *Server) ListEvents(in *pb.ListEventsRequest, stream pb.EventStore_ListEventsServer) error {

	const listBufferSize = 100

	done := make(chan struct{})
	defer close(done)
	it := s.store.NewIterator(in.GetAfterKey())
	eventc, err := it.Fetch(in.GetFollow(), done)

	if err != nil {
		log.Error().Err(err).Msg("Error starting fetch")
		return status.Error(codes.Internal, "")
	}

	log.Debug().Msg("ListEvents")

	for e := range eventc {

		log.Debug().Interface("event", e).Msg("sending event...")

		response := pb.ListEventsResponse{&e}
		err = stream.Send(&response)

		if err == io.EOF {
			log.Debug().Msg("Client closed ListEvents stream")
			return nil
		}
		if err != nil {
			log.Error().Err(err).Msg("Error sending event")
			return status.Error(codes.Internal, "")
		}
	}

	if err = it.Err(); err != nil {
		log.Error().Err(err).Msg("Error fetching events")
		return status.Error(codes.Internal, "")
	}

	return nil
}

// InsertEvents implements eventstore.InsertEvents
func (s *Server) InsertEvents(stream pb.EventStore_InsertEventsServer) error {

	for {

		in, err := stream.Recv()

		if err == io.EOF {
			log.Debug().Msg("InsertEvents Recv EOF")
			response := &pb.InsertEventsResponse{}
			err = stream.SendAndClose(response)
			if err == io.EOF {
				log.Debug().Msg("Send and Close EOF")
				return nil
			}
			if err != nil {
				log.Error().Err(err).Msg("Error closing InsertEvents stream")
				return status.Error(codes.Internal, "")
			}
		}

		if err != nil {
			log.Debug().Err(err).Msg("Error processing event from client")
			return status.Error(codes.InvalidArgument, "Event was not able to be processed")
		}

		err = s.store.Insert(in.GetEvent())
		if err != nil {
			log.Error().Err(err).Msg("Error inserting event to store")
			return status.Error(codes.Internal, "")
		}
	}
}
