package eventstore

import (
	"context"
	"log"
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

	var await chan pb.EventState
	if in.AwaitChannel != "" {
		await = make(chan pb.EventState)
		go func() {
			state, err := s.store.Channel(in.AwaitChannel, in.Event.Topic).AwaitDequeue(ctx, in.Event.Id)
			if err != nil {
				log.Printf("Pub %s: await channel %s: %v", in.Event.Id, in.AwaitChannel, err)
				close(await)
				return
			}
			await <- state
		}()
	}

	err := s.store.Pub(*in.Event)
	if err != nil {
		log.Printf("create event: %v", err)
		return nil, status.Error(codes.Internal, "")
	}

	if await != nil {
		state, ok := <-await
		if !ok {
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

	requeueDelay := time.Duration(in.GetRequeueDelayMiliseconds()) * time.Millisecond
	if requeueDelay == 0 {
		requeueDelay = 8000 * time.Millisecond
	}

	channel := s.store.Channel(in.Channel, in.Topic)
	eventc := channel.Follow()
	defer channel.Close()

	requeue := make(chan *pb.Event, 1)
	cancelRequeue := make(chan struct{}, 1)
	requeueNow := make(chan struct{}, 1)
	defer close(requeue)

	go func() {
		timer := time.NewTimer(0)

		for e := range requeue {
			timer.Reset(requeueDelay)
			select {
			case <-timer.C:
				channel.RequeueEvent(e)
			case <-requeueNow:
				channel.RequeueEvent(e)
			case <-cancelRequeue:
			}
		}

		timer.Stop()
	}()

	idleTimer := time.NewTimer(time.Second / 2)
	idleTimer.Stop()
	defer idleTimer.Stop()

	clientDisconnectTimer := time.NewTimer(time.Second * 5)
	defer clientDisconnectTimer.Stop()

	for {

		if !in.Follow {
			idleTimer.Reset(time.Second / 2)
		}

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
			if status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
				requeueNow <- struct{}{}
				return nil
			}
			if err != nil {
				requeueNow <- struct{}{}
				log.Printf("send event: %v", err)
				return status.Error(codes.Internal, "")
			}
			// TODO: could this ever cancel the wrong requeue?
			cancelRequeue <- struct{}{}

			// Disconnect on idle if not following
		case <-idleTimer.C:
			if channel.Idle() {
				return nil
			}

			// Poll to check if stream closed so we can free up memory
		case <-clientDisconnectTimer.C:
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

	err = channel.SetEventState(in.Topic, eventState)
	if err == eventstore.ErrNotFound {
		return nil, status.Error(codes.NotFound, "")
	}
	if err != nil {
		log.Printf("set event status: %v", err)
		return nil, status.Error(codes.Internal, "")
	}

	if eventState == pb.EventState_QUEUED {
		go func() {
			switch in.Code {
			// TODO: Implement dynamic requeue delay
			case pb.AckCode_REQUEUE_CONSTANT:
				time.Sleep(time.Second * 7)
			case pb.AckCode_REQUEUE_LINEAR:
				time.Sleep(time.Second * 7)
			case pb.AckCode_REQUEUE_EXPONENTIAL:
				time.Sleep(time.Second * 7)
			}
			channel.RequeueEvent(e)
		}()
	}

	return &pb.AckResponse{}, nil
}

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

	return e, nil
}

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
