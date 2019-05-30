package handler

import (
	"context"
	"log"
	"math"
	"time"

	"gitlab.com/katcheCode/deq"
	pb "gitlab.com/katcheCode/deq/api/v1/deq"
	"gitlab.com/katcheCode/deq/deqdb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handler handles requests for service DEQ.
type Handler struct {
	store *deqdb.Store
}

// New creates a new Handler with the provided backing event store
func New(eventStore *deqdb.Store) *Handler {
	return &Handler{eventStore}
}

// Pub implements DEQ.Pub
func (s *Handler) Pub(ctx context.Context, in *pb.PubRequest) (*pb.Event, error) {

	if in.Event == nil {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument event")
	}
	if in.Event.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument event.id")
	}
	if in.Event.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument event.topic")
	}
	if in.Event.DefaultState == pb.Event_UNSPECIFIED_STATE {
		in.Event.DefaultState = pb.Event_QUEUED
	}
	if in.Event.CreateTime <= 0 {
		in.Event.CreateTime = time.Now().UnixNano()
	}

	channel := s.store.Channel(in.AwaitChannel, in.Event.Topic)
	defer channel.Close()

	var sub *deqdb.EventStateSubscription
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
		for e.State == deq.StateQueued {
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
func (s *Handler) Sub(in *pb.SubRequest, stream pb.DEQ_SubServer) error {

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

	channel.BackoffFunc(deqdb.ExponentialBackoff(baseRequeueDelay))

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
func (s *Handler) Ack(ctx context.Context, in *pb.AckRequest) (*pb.AckResponse, error) {

	if in.Channel == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument 'channel'")
	}
	if in.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument 'topic'")
	}
	if in.EventId == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required argument 'event_id'")
	}

	var eventState deq.State
	switch in.Code {
	case pb.AckCode_OK:
		eventState = deq.StateOK
	case pb.AckCode_INVALID:
		eventState = deq.StateInvalid
	case pb.AckCode_INTERNAL:
		eventState = deq.StateInternal
	case pb.AckCode_DEQUEUE_ERROR:
		eventState = deq.StateDequeuedError
	case pb.AckCode_REQUEUE_CONSTANT, pb.AckCode_REQUEUE_LINEAR, pb.AckCode_REQUEUE:
		eventState = deq.StateQueued
	case pb.AckCode_RESET_TIMEOUT:

	case pb.AckCode_UNSPECIFIED:
		return nil, status.Error(codes.InvalidArgument, "argument code is required")
	default:
		return nil, status.Error(codes.InvalidArgument, "Invalid value for argument 'code'")
	}

	channel := s.store.Channel(in.Channel, in.Topic)
	defer channel.Close()

	e, err := channel.Get(ctx, in.EventId)
	if err == deq.ErrNotFound {
		return nil, status.Error(codes.NotFound, "")
	}
	if err != nil {
		log.Printf("Delete: get event: %v", err)
		return nil, status.Error(codes.Internal, "")
	}

	err = channel.SetEventState(ctx, in.EventId, eventState)
	if err == deq.ErrNotFound {
		return nil, status.Error(codes.NotFound, "")
	}
	if err != nil {
		log.Printf("set event %s status: %v", in.EventId, err)
		return nil, status.Error(codes.Internal, "")
	}

	if eventState == deq.StateQueued {

		baseTime := time.Second
		var delay time.Duration
		switch in.Code {
		case pb.AckCode_REQUEUE:
			cappedRequeue := math.Min(float64(e.RequeueCount), 12)
			delay = time.Duration(math.Pow(2, cappedRequeue)) * baseTime
		case pb.AckCode_REQUEUE_LINEAR:
			delay = baseTime * time.Duration(e.RequeueCount+1)
		case pb.AckCode_REQUEUE_CONSTANT:
			delay = baseTime
		}

		if delay < 0 || delay > time.Hour {
			delay = time.Hour
		}

		channel.RequeueEvent(ctx, e, delay)
	}

	return &pb.AckResponse{}, nil
}

// Get implements DEQ.Get
func (s *Handler) Get(ctx context.Context, in *pb.GetRequest) (*pb.Event, error) {

	if in.Event == "" {
		return nil, status.Error(codes.InvalidArgument, "argument event is required")
	}
	if in.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	if in.Channel == "" {
		return nil, status.Error(codes.InvalidArgument, "argument channel is required")
	}
	if in.Await && in.UseIndex {
		return nil, status.Error(codes.InvalidArgument, "await and use_index canoot both be true")
	}

	channel := s.store.Channel(in.Channel, in.Topic)
	defer channel.Close()

	var e deq.Event
	var err error

	if in.Await {
		e, err = channel.Await(ctx, in.Event)
		if err == context.Canceled || err == context.DeadlineExceeded {
			return nil, status.FromContextError(err).Err()
		}
		if err != nil {
			log.Printf("Get: await event: %v", err)
			return nil, status.Error(codes.Internal, "")
		}
	} else if in.UseIndex {
		e, err = channel.GetIndex(ctx, in.Event)
		if err == deq.ErrNotFound {
			return nil, status.Error(codes.NotFound, "")
		}
		if err != nil {
			log.Printf("Get: get event by index: %v", err)
			return nil, status.Error(codes.Internal, "")
		}
	} else {
		e, err = channel.Get(ctx, in.Event)
		if err == deq.ErrNotFound {
			return nil, status.Error(codes.NotFound, "")
		}
		if err != nil {
			log.Printf("Get: get event: %v", err)
			return nil, status.Error(codes.Internal, "")
		}
	}

	return eventToProto(e), nil
}

// BatchGet implements DEQ.BatchGet
func (s *Handler) BatchGet(ctx context.Context, in *pb.BatchGetRequest) (*pb.BatchGetResponse, error) {

	if len(in.Events) == 0 {
		return nil, status.Error(codes.InvalidArgument, "argument events is required")
	}
	if in.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	if in.Channel == "" {
		return nil, status.Error(codes.InvalidArgument, "argument channel is required")
	}

	channel := s.store.Channel(in.Channel, in.Topic)
	defer channel.Close()

	var events map[string]deq.Event
	var err error

	if in.UseIndex {
		events, err = channel.BatchGetIndex(ctx, in.Events)
		if err == deq.ErrNotFound {
			return nil, status.Error(codes.NotFound, "")
		}
		if err != nil {
			log.Printf("Get: get event by index: %v", err)
			return nil, status.Error(codes.Internal, "")
		}
	} else {
		events, err = channel.BatchGet(ctx, in.Events)
		if err == deq.ErrNotFound {
			return nil, status.Error(codes.NotFound, "")
		}
		if err != nil {
			log.Printf("Get: get event: %v", err)
			return nil, status.Error(codes.Internal, "")
		}
	}

	result := make(map[string]*pb.Event, len(events))
	for a, e := range events {
		result[a] = eventToProto(e)
	}
	return &pb.BatchGetResponse{
		Events: result,
	}, nil
}

// List implements DEQ.List
func (s *Handler) List(ctx context.Context, in *pb.ListRequest) (*pb.ListResponse, error) {

	if in.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	if in.Channel == "" {
		return nil, status.Error(codes.InvalidArgument, "channel is required")
	}

	pageSize := 20
	if in.PageSize < 0 {
		return nil, status.Error(codes.InvalidArgument, "page_size cannot be negative")
	}
	if in.PageSize > 0 {
		pageSize = int(in.PageSize)
	}

	channel := s.store.Channel(in.Channel, in.Topic)
	defer channel.Close()

	events := make([]deq.Event, 0, pageSize)

	opts := &deq.IterOptions{
		Reversed:      in.Reversed,
		Min:           in.MinId,
		Max:           in.MaxId,
		PrefetchCount: pageSize,
	}

	var iter deq.EventIter
	if in.UseIndex {
		iter = channel.NewIndexIter(opts)
		defer iter.Close()
	} else {
		iter = channel.NewEventIter(opts)
		defer iter.Close()
	}

	for {
		for len(events) < pageSize && iter.Next(ctx) {
			events = append(events, iter.Event())
		}
		if iter.Err() == nil {
			break
		}
		log.Printf("[WARN] List: iterate event: %v", iter.Err())
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
func (s *Handler) Del(ctx context.Context, in *pb.DelRequest) (*pb.Empty, error) {

	if in.EventId == "" {
		return nil, status.Error(codes.InvalidArgument, "argument event_id is required")
	}
	if in.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}

	err := s.store.Del(ctx, in.Topic, in.EventId)
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
func (s *Handler) Topics(ctx context.Context, in *pb.TopicsRequest) (*pb.TopicsResponse, error) {

	var topics []string

	// TODO: expose paging and sorting options

	iter := s.store.NewTopicIter(&deq.IterOptions{})
	defer iter.Close()

	for ctx.Err() == nil && iter.Next(ctx) {
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
		Indexes:      e.Indexes,
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
		Indexes:      e.Indexes,
		CreateTime:   time.Unix(0, e.CreateTime),
		DefaultState: protoToState(e.DefaultState),
		State:        protoToState(e.State),
		RequeueCount: int(e.RequeueCount),
	}
}

func protoToState(s pb.Event_State) deq.State {
	switch s {
	case pb.Event_UNSPECIFIED_STATE:
		return deq.StateUnspecified
	case pb.Event_QUEUED:
		return deq.StateQueued
	case pb.Event_OK:
		return deq.StateOK
	case pb.Event_INTERNAL:
		return deq.StateInternal
	case pb.Event_INVALID:
		return deq.StateInvalid
	case pb.Event_DEQUEUED_ERROR:
		return deq.StateDequeuedError
	default:
		panic("unrecognized EventState")
	}
}

func stateToProto(s deq.State) pb.Event_State {
	switch s {
	case deq.StateUnspecified:
		return pb.Event_UNSPECIFIED_STATE
	case deq.StateQueued:
		return pb.Event_QUEUED
	case deq.StateOK:
		return pb.Event_OK
	case deq.StateInternal:
		return pb.Event_INTERNAL
	case deq.StateInvalid:
		return pb.Event_INVALID
	case deq.StateDequeuedError:
		return pb.Event_DEQUEUED_ERROR
	default:
		panic("unrecognized EventState")
	}
}
