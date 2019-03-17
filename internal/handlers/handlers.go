package handlers

import (
	"context"
	"log"
	"math"
	"time"

	"gitlab.com/katcheCode/deq"
	pb "gitlab.com/katcheCode/deq/api/v1/deq"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server represents the gRPC server
type Server struct {
	store *deq.Store
}

// NewServer creates a new event store server initalized with a backing event store
func NewServer(eventStore *deq.Store) *Server {
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
	if in.Event.CreateTime <= 0 {
		in.Event.CreateTime = time.Now().UnixNano()
	}

	channel := s.store.Channel(in.AwaitChannel, in.Event.Topic)
	defer channel.Close()

	var sub *deq.EventStateSubscription
	if in.AwaitChannel != "" {
		sub = channel.NewEventStateSubscription(in.Event.Id)
		defer sub.Close()
	}

	e, err := s.store.Pub(ctx, protoToEvent(in.Event))
	if err == deq.ErrAlreadyExists {
		return nil, status.Error(codes.AlreadyExists, "a different event with the same id already exists")
	}
	if err != nil {
		log.Printf("create event: %v", err)
		return nil, status.Error(codes.Internal, "")
	}

	if sub != nil {
		for e.State == deq.EventStateQueued {
			e.State, err = sub.Next(ctx)
			if err == context.DeadlineExceeded || err == context.Canceled {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			if err != nil {
				log.Printf("create event: await dequeue: %v", err)
				return nil, status.Error(codes.Internal, "")
			}
		}
	}

	return eventToProto(e), nil
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
	if idleTimeout == 0 && !in.Follow {
		idleTimeout = time.Second
	}

	channel := s.store.Channel(in.Channel, in.Topic)
	defer channel.Close()

	channel.BackoffFunc(deq.ExponentialBackoff(baseRequeueDelay))

	nextCtx := stream.Context()
	cancel := func() {}
	defer cancel()

	for {

		if idleTimeout > 0 {
			cancel()
			nextCtx, cancel = context.WithTimeout(stream.Context(), idleTimeout)
		}

		e, err := channel.Next(nextCtx)
		if err == context.DeadlineExceeded || err == context.Canceled {
			if err == stream.Context().Err() { // error is from actual request context
				return status.FromContextError(stream.Context().Err()).Err()
			}
			if channel.Idle() {
				return nil
			}
			continue
		}
		if err != nil {
			log.Printf("Sub: get next event from channel: %v", err)
			return status.Error(codes.Internal, "")
		}

		err = stream.Send(eventToProto(e))
		if err != nil {
			// channel.RequeueEvent(e, 0)
			log.Printf("send event: %v", err)
			return status.Error(codes.Internal, "")
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

	var eventState deq.EventState
	switch in.Code {
	case pb.AckCode_DEQUEUE_OK:
		eventState = deq.EventStateDequeuedOK
	case pb.AckCode_DEQUEUE_ERROR:
		eventState = deq.EventStateDequeuedError
	case pb.AckCode_REQUEUE_CONSTANT, pb.AckCode_REQUEUE_LINEAR, pb.AckCode_REQUEUE_EXPONENTIAL:
		eventState = deq.EventStateQueued
	case pb.AckCode_RESET_TIMEOUT:

	case pb.AckCode_UNSPECIFIED:
		return nil, status.Error(codes.InvalidArgument, "argument code is required")
	default:
		return nil, status.Error(codes.InvalidArgument, "Invalid value for argument 'code'")
	}

	channel := s.store.Channel(in.Channel, in.Topic)
	defer channel.Close()

	e, err := channel.Get(in.EventId)
	if err == deq.ErrNotFound {
		return nil, status.Error(codes.NotFound, "")
	}
	if err != nil {
		log.Printf("Delete: get event: %v", err)
		return nil, status.Error(codes.Internal, "")
	}

	err = channel.SetEventState(in.EventId, eventState)
	if err == deq.ErrNotFound {
		return nil, status.Error(codes.NotFound, "")
	}
	if err != nil {
		log.Printf("set event %s status: %v", in.EventId, err)
		return nil, status.Error(codes.Internal, "")
	}

	if eventState == deq.EventStateQueued {

		baseTime := time.Second
		var delay time.Duration
		switch in.Code {
		case pb.AckCode_REQUEUE_CONSTANT:
			delay = baseTime
		case pb.AckCode_REQUEUE_LINEAR:
			delay = baseTime * time.Duration(e.RequeueCount+1)
		case pb.AckCode_REQUEUE_EXPONENTIAL:
			cappedRequeue := math.Min(float64(e.RequeueCount), 12)
			delay = time.Duration(math.Pow(2, cappedRequeue)) * baseTime
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

	channel := s.store.Channel(in.Channel, in.Topic)
	defer channel.Close()

	var e deq.Event
	var err error

	if in.Await {
		e, err = channel.Await(ctx, in.EventId)
		if err == context.Canceled || err == context.DeadlineExceeded {
			return nil, status.FromContextError(err).Err()
		}
		if err != nil {
			log.Printf("Get: await event: %v", err)
			return nil, status.Error(codes.Internal, "")
		}
	} else {
		e, err = channel.Get(in.EventId)
		if err != nil {
			log.Printf("Get: get event: %v", err)
			return nil, status.Error(codes.Internal, "")
		}
	}

	return eventToProto(e), nil
}

// List implements DEQ.List
func (s *Server) List(ctx context.Context, in *pb.ListRequest) (*pb.ListResponse, error) {

	if in.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	if in.Channel == "" {
		return nil, status.Error(codes.InvalidArgument, "argument channel is required")
	}

	channel := s.store.Channel(in.Channel, in.Topic)
	defer channel.Close()

	events := make([]deq.Event, 0, in.PageSize)

	opts := deq.DefaultIterOpts
	opts.Reversed = in.Reversed
	opts.Min = in.MinId
	opts.Max = in.MaxId

	iter := channel.NewEventIter(opts)
	defer iter.Close()

	for {
		for len(events) < cap(events) && iter.Next() {
			events = append(events, iter.Event())
		}
		if iter.Err() == nil {
			break
		}
		log.Printf("List: iterate event: %v", iter.Err())
	}

	results := make([]*pb.Event, len(events))
	for i, e := range events {
		results[i] = eventToProto(e)
	}

	return &pb.ListResponse{
		Events: results,
	}, nil
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
	if err == deq.ErrNotFound {
		return nil, status.Error(codes.NotFound, "")
	}
	if err != nil {
		log.Printf("Del: %v", err)
		return nil, status.Error(codes.Internal, "")
	}

	return &pb.Empty{}, nil
}

// Topics implements DEQ.Topics
func (s *Server) Topics(ctx context.Context, in *pb.TopicsRequest) (*pb.TopicsResponse, error) {

	var topics []string

	opts := deq.DefaultIterOpts
	// TODO: expose paging and sorting options

	iter := s.store.NewTopicIter(opts)
	defer iter.Close()

	for ctx.Err() == nil && iter.Next() {
		topics = append(topics, iter.Topic())
	}

	return &pb.TopicsResponse{
		Topics: topics,
	}, nil
}

func eventToProto(e deq.Event) *pb.Event {
	return &pb.Event{
		Id:           e.ID,
		Topic:        e.Topic,
		Payload:      e.Payload,
		CreateTime:   e.CreateTime.UnixNano(),
		DefaultState: stateToProto(e.DefaultState),
		State:        stateToProto(e.State),
		RequeueCount: int32(e.RequeueCount),
	}
}

func protoToEvent(e *pb.Event) deq.Event {
	return deq.Event{
		ID:           e.Id,
		Topic:        e.Topic,
		Payload:      e.Payload,
		CreateTime:   time.Unix(0, e.CreateTime),
		DefaultState: protoToState(e.DefaultState),
		State:        protoToState(e.State),
		RequeueCount: int(e.RequeueCount),
	}
}

func protoToState(s pb.EventState) deq.EventState {
	switch s {
	case pb.EventState_UNSPECIFIED_STATE:
		return deq.EventStateUnspecified
	case pb.EventState_QUEUED:
		return deq.EventStateQueued
	case pb.EventState_DEQUEUED_OK:
		return deq.EventStateDequeuedOK
	case pb.EventState_DEQUEUED_ERROR:
		return deq.EventStateDequeuedError
	default:
		panic("unrecognized EventState")
	}
}

func stateToProto(s deq.EventState) pb.EventState {
	switch s {
	case deq.EventStateUnspecified:
		return pb.EventState_UNSPECIFIED_STATE
	case deq.EventStateQueued:
		return pb.EventState_QUEUED
	case deq.EventStateDequeuedOK:
		return pb.EventState_DEQUEUED_OK
	case deq.EventStateDequeuedError:
		return pb.EventState_DEQUEUED_ERROR
	default:
		panic("unrecognized EventState")
	}
}
