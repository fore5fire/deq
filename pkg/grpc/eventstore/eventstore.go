package eventstore

import (
	"context"
	"log"
	"math"
	"time"

	pb "gitlab.com/katcheCode/deq/api/v1/deq"
	"gitlab.com/katcheCode/deq/pkg/eventstore"
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
	if in.Event.DefaultState == pb.EventState_UNSPECIFIED_STATE {
		in.Event.DefaultState = pb.EventState_QUEUED
	}

	in.Event.CreateTime = time.Now().UnixNano()

	channel := s.store.Channel(in.AwaitChannel, in.Event.Topic)

	var await chan pb.EventState
	if in.AwaitChannel != "" {
		await = channel.AwaitEventStateChange(ctx, in.Event.Id)
	}

	err := s.store.Pub(*in.Event)
	if err == eventstore.ErrAlreadyExists {
		return nil, status.Error(codes.AlreadyExists, "a different event with the same id already exists")
	}
	if err != nil {
		log.Printf("create event: %v", err)
		return nil, status.Error(codes.Internal, "")
	}

	if await != nil {
		state, ok := <-await
		if !ok {
			log.Printf("await event dequeue: %v", channel.Err())
			return nil, status.Error(codes.Internal, "")
		}
		in.Event.State = state
	}

	return in.Event, nil
}

// Sub implements DEQ.Sub
func (s *Server) Sub(in *pb.SubRequest, stream pb.DEQ_SubServer) error {

	if in.Channel == "" {
		return status.Error(codes.InvalidArgument, "Missing required argument 'channel'")
	}

	baseRequeueDelay := time.Duration(in.RequeueDelayMilliseconds) * time.Millisecond
	if baseRequeueDelay == 0 {
		baseRequeueDelay = 8 * time.Second
	}
	idleTimeout := time.Duration(in.IdleTimeoutMilliseconds) * time.Millisecond
	if idleTimeout == 0 && in.Follow {
		idleTimeout = time.Second
	}

	channel := s.store.Channel(in.Channel, in.Topic)
	defer channel.Close()

	// No buffer here - we don't want to pull of the next event until we're at least sending the previous
	events := make(chan pb.Event)
	ready := make(chan struct{})
	defer close(ready)
	var nextErr error
	go func() {
		for range ready {
			e, err := channel.Next(stream.Context())
			if err != nil {
				nextErr = err
				close(events)
				return
			}
			events <- e
		}
	}()

	idleTimer := time.NewTimer(time.Hour)
	defer idleTimer.Stop()

	for {
		if !idleTimer.Stop() {
			<-idleTimer.C
		}
		if idleTimeout > 0 {
			idleTimer.Reset(idleTimeout)
		}
		ready <- struct{}{}
		select {
		case e, ok := <-events:
			if !ok {
				if nextErr != nil {
					log.Printf("read from channel: %v", nextErr)
					return status.Error(codes.Internal, "")
				}
				// Event stream closed, shutting down...
				// TODO: expose running streams metric
				return nil
			}

			if baseRequeueDelay > 0 {
				err := channel.RequeueEvent(e, time.Duration(math.Pow(2, float64(e.RequeueCount)))*baseRequeueDelay)
				if err != nil {
					log.Printf("send event: requeue: %v", err)
					return status.Error(codes.Internal, "")
				}
			}

			err := stream.Send(&e)
			if err != nil {
				// channel.RequeueEvent(e, 0)
				log.Printf("send event: %v", err)
				return status.Error(codes.Internal, "")
			}
			// Poll for disconect when idle
		case <-idleTimer.C:
			if channel.Idle() {
				return nil
			}
			// // Poll to check if stream closed so we can free up memory
			// err := stream.Context().Err()
			// if err == context.Canceled || err == context.DeadlineExceeded {
			// 	return nil
			// }
			// if err != nil {
			// 	log.Printf("Stream failed: %v", err)
			// 	return nil
			// }
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

	var eventState pb.EventState
	switch in.Code {
	case pb.AckCode_DEQUEUE_OK:
		eventState = pb.EventState_DEQUEUED_OK
	case pb.AckCode_DEQUEUE_ERROR:
		eventState = pb.EventState_DEQUEUED_ERROR
	case pb.AckCode_REQUEUE_CONSTANT, pb.AckCode_REQUEUE_LINEAR, pb.AckCode_REQUEUE_EXPONENTIAL:
		eventState = pb.EventState_QUEUED
	case pb.AckCode_RESET_TIMEOUT:

	case pb.AckCode_UNSPECIFIED:
		return nil, status.Error(codes.InvalidArgument, "argument code is required")
	default:
		return nil, status.Error(codes.InvalidArgument, "Invalid value for argument 'code'")
	}

	channel := s.store.Channel(in.Channel, in.Topic)

	e, err := channel.Get(in.EventId)
	if err == eventstore.ErrNotFound {
		return nil, status.Error(codes.NotFound, "")
	}
	if err != nil {
		log.Printf("Delete: get event: %v", err)
		return nil, status.Error(codes.Internal, "")
	}

	err = channel.SetEventState(in.EventId, eventState)
	if err == eventstore.ErrNotFound {
		return nil, status.Error(codes.NotFound, "")
	}
	if err != nil {
		log.Printf("set event %s status: %v", in.EventId, err)
		return nil, status.Error(codes.Internal, "")
	}

	if eventState == pb.EventState_QUEUED {

		baseTime := time.Second
		var delay time.Duration
		switch in.Code {
		case pb.AckCode_REQUEUE_CONSTANT:
			delay = baseTime
		case pb.AckCode_REQUEUE_LINEAR:
			delay = baseTime * time.Duration(e.RequeueCount)
		case pb.AckCode_REQUEUE_EXPONENTIAL:
			delay = time.Duration(math.Pow(2, float64(e.RequeueCount))) * baseTime
		}

		if delay < 0 || delay > time.Hour {
			delay = time.Hour
		}

		channel.RequeueEvent(e, delay)
	}

	return &pb.AckResponse{}, nil
}

// Get implements DEQ.Get
func (s *Server) Get(ctx context.Context, in *pb.GetRequest) (*pb.Event, error) {

	if in.EventId == "" {
		return nil, status.Error(codes.InvalidArgument, "argument event_id is required")
	}
	if in.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	if in.Channel == "" {
		return nil, status.Error(codes.InvalidArgument, "argument channel is required")
	}

	e, err := s.store.Channel(in.Channel, in.Topic).Get(in.EventId)
	if err == eventstore.ErrNotFound {
		return nil, status.Error(codes.NotFound, "")
	}
	if err != nil {
		log.Printf("Get: %v", err)
		return nil, status.Error(codes.Internal, "")
	}

	return &e, nil
}

// Del implements DEQ.Del
func (s *Server) Del(ctx context.Context, in *pb.DelRequest) (*pb.Empty, error) {

	if in.EventId == "" {
		return nil, status.Error(codes.InvalidArgument, "argument event_id is required")
	}
	if in.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}

	err := s.store.Del(in.Topic, in.EventId)
	if err == eventstore.ErrNotFound {
		return nil, status.Error(codes.NotFound, "")
	}
	if err != nil {
		log.Printf("Del: %v", err)
		return nil, status.Error(codes.Internal, "")
	}

	return &pb.Empty{}, nil
}
