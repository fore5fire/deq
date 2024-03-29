///
//  Generated code. Do not modify.
//  source: example/greeter/greeter.proto
///
package greeter

import (
	"context"
	"fmt"
	"sync"
	"strings"
	"time"

	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/deqopt"
	
)

type HelloRequestEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	Selector        string
	SelectorVersion int64

	HelloRequest *HelloRequest
}

type _HelloRequestTopicConfig interface {
	EventToHelloRequestEvent(deq.Event) (*HelloRequestEvent, error)
}

// HelloRequestEventIter is an iterator for HelloRequestEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a HelloRequestEvent.
type HelloRequestEventIter interface {
	Next(ctx context.Context) (*HelloRequestEvent, error)
	Close()
}

type XXX_HelloRequestEventIter struct {
	Iter   deq.EventIter
	Config _HelloRequestTopicConfig
}

// Next returns the next HelloRequestEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_HelloRequestEventIter) Next(ctx context.Context) (*HelloRequestEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToHelloRequestEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to HelloRequestEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_HelloRequestEventIter) Close() {
	it.Iter.Close()
}


type HelloReplyEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	Selector        string
	SelectorVersion int64

	HelloReply *HelloReply
}

type _HelloReplyTopicConfig interface {
	EventToHelloReplyEvent(deq.Event) (*HelloReplyEvent, error)
}

// HelloReplyEventIter is an iterator for HelloReplyEvents. It has an identical interface to
// deq.EventIter, except that the Event method returns a HelloReplyEvent.
type HelloReplyEventIter interface {
	Next(ctx context.Context) (*HelloReplyEvent, error)
	Close()
}

type XXX_HelloReplyEventIter struct {
	Iter   deq.EventIter
	Config _HelloReplyTopicConfig
}

// Next returns the next HelloReplyEvent, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_HelloReplyEventIter) Next(ctx context.Context) (*HelloReplyEvent, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventToHelloReplyEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to HelloReplyEvent: %v", err)
	}

	return e, nil
}

func (it *XXX_HelloReplyEventIter) Close() {
	it.Iter.Close()
}

type GreeterTopicConfig struct {
	topics map[string]string
}

func NewGreeterTopicConfig() *GreeterTopicConfig {
	return &GreeterTopicConfig{
		topics: make(map[string]string),
	}
}

func (c *GreeterTopicConfig) EventToHelloRequestEvent(e deq.Event) (*HelloRequestEvent, error) {

	if e.Topic != c.HelloRequestTopic() {
		return nil, fmt.Errorf("incorrect topic %s", e.Topic)
	}

	msg := new(HelloRequest)
	err := msg.Unmarshal(e.Payload)
	if err != nil {
		return nil, fmt.Errorf("unmarshal payload: %v", err)
	}

	return &HelloRequestEvent{
		ID:           e.ID,
		HelloRequest:  msg,
		CreateTime:   e.CreateTime,
		DefaultState: e.DefaultState,
		State:        e.State,
		Indexes:      e.Indexes,
		
		Selector:        e.Selector,
		SelectorVersion: e.SelectorVersion,
	}, nil
}

func (c *GreeterTopicConfig) HelloRequestEventToEvent(e *HelloRequestEvent) (deq.Event, error) {

	buf, err := e.HelloRequest.Marshal()
	if err != nil {
		return deq.Event{}, err
	}

	return deq.Event{
		ID:           e.ID,
		Payload:      buf,
		CreateTime:   e.CreateTime,
		DefaultState: e.DefaultState,
		State:        e.State,
		Topic:        c.HelloRequestTopic(),
		Indexes:      e.Indexes,

		Selector:        e.Selector,
		SelectorVersion: e.SelectorVersion,
	}, nil
}

func (c *GreeterTopicConfig) HelloRequestTopic() string {
	if c == nil {
		return "greeter.HelloRequest"
	}

	topic, ok := c.topics["greeter.HelloRequest"]
	if ok {
		return topic
	}
	return "greeter.HelloRequest"
}

func (c *GreeterTopicConfig) SetHelloRequestTopic(topic string) {
	c.topics["greeter.HelloRequest"] = topic
}

func (c *GreeterTopicConfig) EventToHelloReplyEvent(e deq.Event) (*HelloReplyEvent, error) {

	if e.Topic != c.HelloReplyTopic() {
		return nil, fmt.Errorf("incorrect topic %s", e.Topic)
	}

	msg := new(HelloReply)
	err := msg.Unmarshal(e.Payload)
	if err != nil {
		return nil, fmt.Errorf("unmarshal payload: %v", err)
	}

	return &HelloReplyEvent{
		ID:           e.ID,
		HelloReply:  msg,
		CreateTime:   e.CreateTime,
		DefaultState: e.DefaultState,
		State:        e.State,
		Indexes:      e.Indexes,
		
		Selector:        e.Selector,
		SelectorVersion: e.SelectorVersion,
	}, nil
}

func (c *GreeterTopicConfig) HelloReplyEventToEvent(e *HelloReplyEvent) (deq.Event, error) {

	buf, err := e.HelloReply.Marshal()
	if err != nil {
		return deq.Event{}, err
	}

	return deq.Event{
		ID:           e.ID,
		Payload:      buf,
		CreateTime:   e.CreateTime,
		DefaultState: e.DefaultState,
		State:        e.State,
		Topic:        c.HelloReplyTopic(),
		Indexes:      e.Indexes,

		Selector:        e.Selector,
		SelectorVersion: e.SelectorVersion,
	}, nil
}

func (c *GreeterTopicConfig) HelloReplyTopic() string {
	if c == nil {
		return "greeter.HelloReply"
	}

	topic, ok := c.topics["greeter.HelloReply"]
	if ok {
		return topic
	}
	return "greeter.HelloReply"
}

func (c *GreeterTopicConfig) SetHelloReplyTopic(topic string) {
	c.topics["greeter.HelloReply"] = topic
}


type GreeterClient interface {
	SyncAllTo(ctx context.Context, dest deq.Client, opts ...deqopt.SyncOption) error
	GetHelloRequestEvent(ctx context.Context, id string, options ...deqopt.GetOption) (*HelloRequestEvent, error)
	BatchGetHelloRequestEvents(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]*HelloRequestEvent, error)
	SubHelloRequestEvent(ctx context.Context, handler func(context.Context, *HelloRequestEvent) error) error
	NewHelloRequestEventIter(opts *deq.IterOptions) HelloRequestEventIter
	NewHelloRequestIndexIter(opts *deq.IterOptions) HelloRequestEventIter
	PubHelloRequestEvent(ctx context.Context, e *HelloRequestEvent) (*HelloRequestEvent, error)
	SyncHelloRequestEventsTo(ctx context.Context, dest deq.Client, opts ...deqopt.SyncOption) error
	
	GetHelloReplyEvent(ctx context.Context, id string, options ...deqopt.GetOption) (*HelloReplyEvent, error)
	BatchGetHelloReplyEvents(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]*HelloReplyEvent, error)
	SubHelloReplyEvent(ctx context.Context, handler func(context.Context, *HelloReplyEvent) error) error
	NewHelloReplyEventIter(opts *deq.IterOptions) HelloReplyEventIter
	NewHelloReplyIndexIter(opts *deq.IterOptions) HelloReplyEventIter
	PubHelloReplyEvent(ctx context.Context, e *HelloReplyEvent) (*HelloReplyEvent, error)
	SyncHelloReplyEventsTo(ctx context.Context, dest deq.Client, opts ...deqopt.SyncOption) error
	
	SayHello(ctx context.Context, e *HelloRequestEvent) (*HelloReplyEvent, error)
	}

type _GreeterClient struct {
	db      deq.Client
	channel string
	config *GreeterTopicConfig
}

// NewGreeterClient creates a GreeterClient, optionally overriding the default channel and topic config.
// If db's default channel is not set then channel is required.
func NewGreeterClient(db deq.Client, channel string, config *GreeterTopicConfig) GreeterClient {
	if channel == "" {
		channel = db.DefaultChannel()
		if channel == "" {
			panic("channel is required")
		}
	}
	
	return &_GreeterClient{
		db: db,
		channel: channel,
		config: config,
	}
}

func (c *_GreeterClient) SyncAllTo(ctx context.Context, remote deq.Client, opts ...deqopt.SyncOption) error {
	errc := make(chan error, 1)
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(ctx)
	
	
	wg.Add(1)
	go func() {
		defer wg.Done()
		errc <- c.SyncHelloRequestEventsTo(ctx, remote)
	}()
	
	wg.Add(1)
	go func() {
		defer wg.Done()
		errc <- c.SyncHelloReplyEventsTo(ctx, remote)
	}()
	

	go func() { 
		wg.Wait()
		close(errc)
	}()

	var firstErr error
	for err := range errc {
		cancel()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
func (c *_GreeterClient) SyncHelloRequestEventsTo(ctx context.Context, remote deq.Client, opts ...deqopt.SyncOption) error {

	optsSet := deqopt.NewSyncOptionSet(opts)

	channel := c.db.Channel(c.channel, c.config.HelloRequestTopic())
	defer channel.Close()

	if optsSet.IdleTimeout != 0 {
		channel.SetIdleTimeout(optsSet.IdleTimeout)
	}

	return deq.SyncTo(ctx, remote, channel)
} 

func (c *_GreeterClient) GetHelloRequestEvent(ctx context.Context, id string, options ...deqopt.GetOption) (*HelloRequestEvent, error) {
	
	channel := c.db.Channel(c.channel, c.config.HelloRequestTopic())
	defer channel.Close()

	deqEvent, err := channel.Get(ctx, id, options...)
	if err != nil {
		return nil, err
	}

	event, err := c.config.EventToHelloRequestEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to HelloRequestEvent: %v", err)
	}

	return event, nil
}

func (c *_GreeterClient) BatchGetHelloRequestEvents(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]*HelloRequestEvent, error) {
	
	channel := c.db.Channel(c.channel, c.config.HelloRequestTopic())
	defer channel.Close()

	deqEvents, err := channel.BatchGet(ctx, ids, options...)
	if err != nil {
		return nil, err
	}

	events := make(map[string]*HelloRequestEvent, len(deqEvents))
	for a, e := range deqEvents {
		event, err := c.config.EventToHelloRequestEvent(e)
		if err != nil {
			return nil, fmt.Errorf("convert deq.Event to HelloRequestEvent: %v", err)
		}
		events[a] = event 
	}

	return events, nil
}

func (c *_GreeterClient) SubHelloRequestEvent(ctx context.Context, handler func(context.Context, *HelloRequestEvent) error) error {
	channel := c.db.Channel(c.channel, c.config.HelloRequestTopic())
	defer channel.Close()

	return channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {
			event, err := c.config.EventToHelloRequestEvent(e)
			if err != nil {
				panic("convert deq.Event to HelloRequestEvent: " + err.Error())
			}

			return nil, handler(ctx, event)
	})
}

func (c *_GreeterClient) NewHelloRequestEventIter(opts *deq.IterOptions) HelloRequestEventIter {
	
	channel := c.db.Channel(c.channel, c.config.HelloRequestTopic())
	defer channel.Close()
	
	return &XXX_HelloRequestEventIter{
		Iter:   channel.NewEventIter(opts),
		Config: c.config,
	}
}

func (c *_GreeterClient) NewHelloRequestIndexIter(opts *deq.IterOptions) HelloRequestEventIter {
	
	channel := c.db.Channel(c.channel, c.config.HelloRequestTopic())
	defer channel.Close()
	
	return &XXX_HelloRequestEventIter{
		Iter:   channel.NewIndexIter(opts),
		Config: c.config,
	}
}

func (c *_GreeterClient) PubHelloRequestEvent(ctx context.Context, e *HelloRequestEvent) (*HelloRequestEvent, error) {
	deqEvent, err := c.config.HelloRequestEventToEvent(e)
	if err != nil {
		return nil, fmt.Errorf("convert HelloRequestEvent to deq.Event: %v", err)
	}

	deqEvent, err = c.db.Pub(ctx, deqEvent)
	if err != nil {
		return nil, err
	}

	e, err = c.config.EventToHelloRequestEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to HelloRequestEvent: %v", err)
	}

	return e, nil
}


func (c *_GreeterClient) SyncHelloReplyEventsTo(ctx context.Context, remote deq.Client, opts ...deqopt.SyncOption) error {

	optsSet := deqopt.NewSyncOptionSet(opts)

	channel := c.db.Channel(c.channel, c.config.HelloReplyTopic())
	defer channel.Close()

	if optsSet.IdleTimeout != 0 {
		channel.SetIdleTimeout(optsSet.IdleTimeout)
	}

	return deq.SyncTo(ctx, remote, channel)
} 

func (c *_GreeterClient) GetHelloReplyEvent(ctx context.Context, id string, options ...deqopt.GetOption) (*HelloReplyEvent, error) {
	
	channel := c.db.Channel(c.channel, c.config.HelloReplyTopic())
	defer channel.Close()

	deqEvent, err := channel.Get(ctx, id, options...)
	if err != nil {
		return nil, err
	}

	event, err := c.config.EventToHelloReplyEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to HelloReplyEvent: %v", err)
	}

	return event, nil
}

func (c *_GreeterClient) BatchGetHelloReplyEvents(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]*HelloReplyEvent, error) {
	
	channel := c.db.Channel(c.channel, c.config.HelloReplyTopic())
	defer channel.Close()

	deqEvents, err := channel.BatchGet(ctx, ids, options...)
	if err != nil {
		return nil, err
	}

	events := make(map[string]*HelloReplyEvent, len(deqEvents))
	for a, e := range deqEvents {
		event, err := c.config.EventToHelloReplyEvent(e)
		if err != nil {
			return nil, fmt.Errorf("convert deq.Event to HelloReplyEvent: %v", err)
		}
		events[a] = event 
	}

	return events, nil
}

func (c *_GreeterClient) SubHelloReplyEvent(ctx context.Context, handler func(context.Context, *HelloReplyEvent) error) error {
	channel := c.db.Channel(c.channel, c.config.HelloReplyTopic())
	defer channel.Close()

	return channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {
			event, err := c.config.EventToHelloReplyEvent(e)
			if err != nil {
				panic("convert deq.Event to HelloReplyEvent: " + err.Error())
			}

			return nil, handler(ctx, event)
	})
}

func (c *_GreeterClient) NewHelloReplyEventIter(opts *deq.IterOptions) HelloReplyEventIter {
	
	channel := c.db.Channel(c.channel, c.config.HelloReplyTopic())
	defer channel.Close()
	
	return &XXX_HelloReplyEventIter{
		Iter:   channel.NewEventIter(opts),
		Config: c.config,
	}
}

func (c *_GreeterClient) NewHelloReplyIndexIter(opts *deq.IterOptions) HelloReplyEventIter {
	
	channel := c.db.Channel(c.channel, c.config.HelloReplyTopic())
	defer channel.Close()
	
	return &XXX_HelloReplyEventIter{
		Iter:   channel.NewIndexIter(opts),
		Config: c.config,
	}
}

func (c *_GreeterClient) PubHelloReplyEvent(ctx context.Context, e *HelloReplyEvent) (*HelloReplyEvent, error) {
	deqEvent, err := c.config.HelloReplyEventToEvent(e)
	if err != nil {
		return nil, fmt.Errorf("convert HelloReplyEvent to deq.Event: %v", err)
	}

	deqEvent, err = c.db.Pub(ctx, deqEvent)
	if err != nil {
		return nil, err
	}

	e, err = c.config.EventToHelloReplyEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to HelloReplyEvent: %v", err)
	}

	return e, nil
}



func (c *_GreeterClient) SayHello(ctx context.Context, e *HelloRequestEvent) (*HelloReplyEvent, error) {

	_, err := c.PubHelloRequestEvent(ctx, e)
	if err != nil {
		return nil, fmt.Errorf("pub: %v", err)
	}

	result, err := c.GetHelloReplyEvent(ctx, e.ID, deqopt.Await())
	if err != nil {
		return nil, fmt.Errorf("get response: %v", err)
	}

	return result, nil
}

type GreeterHandlers interface {
	SayHello(ctx context.Context, req *HelloRequestEvent) (*HelloReplyEvent, error)
}

type GreeterServer interface {
	Listen(ctx context.Context) error
	Close()
}

type _GreeterServer struct {
	handlers GreeterHandlers
	db       deq.Client
	channel  string
	done     chan struct{}
	config   *GreeterTopicConfig
}

func NewGreeterServer(db deq.Client, handlers GreeterHandlers, channelPrefix string, config *GreeterTopicConfig) GreeterServer {
	return &_GreeterServer{
		handlers: handlers,
		channel: channelPrefix,
		db: db,
		done: make(chan struct{}),
		config: config,
	}
}

func (s *_GreeterServer) Listen(ctx context.Context) error {
	errc := make(chan error, 1)
	wg := sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	
	wg.Add(1)
  	go func() {
		defer wg.Done()
		
		channel := s.db.Channel(strings.TrimSuffix(s.channel, ".") + ".SayHello", s.config.HelloRequestTopic())
		defer channel.Close()
		
		err := channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {
			event, err := s.config.EventToHelloRequestEvent(e)
			if err != nil {
				panic("convert deq.Event to HelloRequestEvent: " + err.Error())
			}

			response, code := s.handlers.SayHello(ctx, event)
			if response == nil {
				return nil, code
			}

			deqResponse, err := s.config.HelloReplyEventToEvent(response)
			if err != nil {
				panic("convert response of type HelloReplyEvent to deq.Event: " + err.Error())
			}
			return &deqResponse, code
		})
		if err != nil {
			if err != ctx.Err() {
				err = fmt.Errorf("SayHello: %v", err)
			}
			// If no one is recieving then the outer function already has returned
			select {
			case errc <- err:
			default:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(errc)
	}()

	select {
	case <-s.done:
		return nil
	case err := <-errc:
		return err
	}
}

func (s *_GreeterServer) Close() {
	close(s.done)
}


