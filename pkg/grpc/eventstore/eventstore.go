package eventstore

import (
	"context"
	"log"
	"time"

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

// Pub implements DEQ.Pub
func (s *Server) Pub(ctx context.Context, in *pb.PubRequest) (*pb.Event, error) {

	if in.Event == nil {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument event")
	}
	if in.Event.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument event.id")
	}
	if in.Event.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument event.topic")
	}

	err := s.store.Pub(in.Event)
	if err != nil {
		log.Printf("create event: %v", err)
		return nil, status.Error(codes.Internal, "")
	}

	return in.Event, nil
}

// Sub implements DEQ.Sub
func (s *Server) Sub(in *pb.SubRequest, stream pb.DEQ_SubServer) error {

	if in.Channel == "" {
		return status.Error(codes.InvalidArgument, "Missing required argument 'channel'")
	}

	requeueDelay := time.Duration(in.GetRequeueDelayMiliseconds()) * time.Millisecond
	if requeueDelay == 0 {
		requeueDelay = 8000 * time.Millisecond
	}

	channel := s.store.Channel(in.Channel, in.Topic)
	eventc, idle := channel.Follow()
	defer channel.Close()

	requeue := make(chan *pb.Event, 1)
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
				// Upstream closed, shut down
				// TODO: expose running streams metric
				return nil
			}

			requeue <- e
			err := stream.Send(e)
			if status.Code(err) == codes.Canceled {
				cancelRequeue <- struct{}{}
				channel.RequeueEvent(e)
				return nil
			}
			if status.Code(err) == codes.DeadlineExceeded {
				cancelRequeue <- struct{}{}
				channel.RequeueEvent(e)
				return nil
			}
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
			if !in.Follow {
				return nil
			}

		// Poll to check if stream closed so we can free up memory
		case <-time.After(time.Second * 5):
			err := stream.Context().Err()
			if err == context.Canceled || err == context.DeadlineExceeded {
				return nil
			}
			if err != nil {
				log.Printf("Stream failed: %v", err)
				return nil
			}
		}
	}
}

// Ack implements DEQ.Ack
func (s *Server) Ack(ctx context.Context, in *pb.AckRequest) (*pb.AckResponse, error) {

	if in.Channel == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument 'channel'")
	}
	if in.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument 'topic'")
	}
	if in.EventId == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument 'event_id'")
	}

	channel := s.store.Channel(in.Channel, in.Topic)

	eventState := pb.EventState_UNSPECIFIED_STATE
	switch in.Code {
	case pb.AckCode_DEQUEUE_OK:
		eventState = pb.EventState_DEQUEUED_OK
	case pb.AckCode_DEQUEUE_ERROR:
		eventState = pb.EventState_DEQUEUED_ERROR
	case pb.AckCode_REQUEUE_CONSTANT:
		eventState = pb.EventState_QUEUED

	case pb.AckCode_REQUEUE_LINEAR:
		eventState = pb.EventState_QUEUED

	case pb.AckCode_REQUEUE_EXPONENTIAL:
		eventState = pb.EventState_QUEUED

	case pb.AckCode_RESET_TIMEOUT:

	default:
		return nil, status.Error(codes.InvalidArgument, "Invalid value for argument 'code'")
	}

	if eventState != pb.EventState_UNSPECIFIED_STATE {
		err := channel.SetEventState(in.Topic, eventState)
		if err == eventstore.ErrNotFound {
			return nil, status.Error(codes.NotFound, "")
		}
		if err != nil {
			log.Printf("set event status: %v", err)
			return nil, status.Error(codes.Internal, "")
		}
	}

	return &pb.AckResponse{}, nil
}
