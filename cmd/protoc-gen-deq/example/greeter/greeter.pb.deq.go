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
	"gitlab.com/katcheCode/deq/ack"
	
	
)

type HelloRequestEvent struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.EventState
	State        deq.EventState
	Indexes      []string

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
	DefaultState deq.EventState
	State        deq.EventState
	Indexes      []string

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
	SyncAllTo(ctx context.Context, remote deq.Client) error
	GetHelloRequestEvent(ctx context.Context, id string) (*HelloRequestEvent, error)
	GetHelloRequestIndex(ctx context.Context, index string) (*HelloRequestEvent, error)
	AwaitHelloRequestEvent(ctx context.Context, id string) (*HelloRequestEvent, error)
	SubHelloRequestEvent(ctx context.Context, handler func(context.Context, *HelloRequestEvent) ack.Code) error
	NewHelloRequestEventIter(opts *deq.IterOptions) HelloRequestEventIter
	NewHelloRequestIndexIter(opts *deq.IterOptions) HelloRequestEventIter
	PubHelloRequestEvent(ctx context.Context, e *HelloRequestEvent) (*HelloRequestEvent, error)
	DelHelloRequestEvent(ctx context.Context, id string) error
	
	GetHelloReplyEvent(ctx context.Context, id string) (*HelloReplyEvent, error)
	GetHelloReplyIndex(ctx context.Context, index string) (*HelloReplyEvent, error)
	AwaitHelloReplyEvent(ctx context.Context, id string) (*HelloReplyEvent, error)
	SubHelloReplyEvent(ctx context.Context, handler func(context.Context, *HelloReplyEvent) ack.Code) error
	NewHelloReplyEventIter(opts *deq.IterOptions) HelloReplyEventIter
	NewHelloReplyIndexIter(opts *deq.IterOptions) HelloReplyEventIter
	PubHelloReplyEvent(ctx context.Context, e *HelloReplyEvent) (*HelloReplyEvent, error)
	DelHelloReplyEvent(ctx context.Context, id string) error
	
	SayHello(ctx context.Context, e *HelloRequestEvent) (*HelloReplyEvent, error)
	}

type _GreeterClient struct {
	db      deq.Client
	channel string
	config *GreeterTopicConfig
}

func NewGreeterClient(db deq.Client, channel string, config *GreeterTopicConfig) GreeterClient {
	return &_GreeterClient{
		db: db,
		channel: channel,
		config: config,
	}
}

func (c *_GreeterClient) SyncAllTo(ctx context.Context, remote deq.Client) error {
	errc := make(chan error, 1)
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(ctx)
	
	
	wg.Add(1)
	go func() {
		defer wg.Done()

		channel := c.db.Channel(c.channel, c.config.HelloRequestTopic())
		defer channel.Close()

		err := deq.SyncTo(ctx, remote, channel)
		if err != nil {
			select {
			default:
			case errc <- err:
			}
		}
	}()
	
	wg.Add(1)
	go func() {
		defer wg.Done()

		channel := c.db.Channel(c.channel, c.config.HelloReplyTopic())
		defer channel.Close()

		err := deq.SyncTo(ctx, remote, channel)
		if err != nil {
			select {
			default:
			case errc <- err:
			}
		}
	}()
	

	go func() { 
		wg.Wait()
		close(errc)
	}()

	err := <-errc
	cancel()
	wg.Wait()

	return err
}
func (c *_GreeterClient) GetHelloRequestEvent(ctx context.Context, id string) (*HelloRequestEvent, error) {
	
	channel := c.db.Channel(c.channel, c.config.HelloRequestTopic())
	defer channel.Close()

	deqEvent, err := channel.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	event, err := c.config.EventToHelloRequestEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to HelloRequestEvent: %v", err)
	}

	return event, nil
}

func (c *_GreeterClient) GetHelloRequestIndex(ctx context.Context, index string) (*HelloRequestEvent, error) {
	
	channel := c.db.Channel(c.channel, c.config.HelloRequestTopic())
	defer channel.Close()

	deqEvent, err := channel.GetIndex(ctx, index)
	if err != nil {
		return nil, err
	}

	event, err := c.config.EventToHelloRequestEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to HelloRequestEvent: %v", err)
	}

	return event, nil
}

func (c *_GreeterClient) AwaitHelloRequestEvent(ctx context.Context, id string) (*HelloRequestEvent, error) {
	
	channel := c.db.Channel(c.channel, c.config.HelloRequestTopic())
	defer channel.Close()

	deqEvent, err := channel.Await(ctx, id)
	if err != nil {
		return nil, err
	}

	event, err := c.config.EventToHelloRequestEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to HelloRequestEvent: %v", err)
	}

	return event, nil
}

func (c *_GreeterClient) SubHelloRequestEvent(ctx context.Context, handler func(context.Context, *HelloRequestEvent) ack.Code) error {
	channel := c.db.Channel(c.channel, c.config.HelloRequestTopic())
	defer channel.Close()

	return channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, ack.Code) {
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
		return nil, fmt.Errorf("pub: %v", err)
	}

	e, err = c.config.EventToHelloRequestEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to HelloRequestEvent: %v", err)
	}

	return e, nil
}

func (c *_GreeterClient) DelHelloRequestEvent(ctx context.Context, id string) error {
	return c.db.Del(ctx, c.config.HelloRequestTopic(), id)
}

func (c *_GreeterClient) GetHelloReplyEvent(ctx context.Context, id string) (*HelloReplyEvent, error) {
	
	channel := c.db.Channel(c.channel, c.config.HelloReplyTopic())
	defer channel.Close()

	deqEvent, err := channel.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	event, err := c.config.EventToHelloReplyEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to HelloReplyEvent: %v", err)
	}

	return event, nil
}

func (c *_GreeterClient) GetHelloReplyIndex(ctx context.Context, index string) (*HelloReplyEvent, error) {
	
	channel := c.db.Channel(c.channel, c.config.HelloReplyTopic())
	defer channel.Close()

	deqEvent, err := channel.GetIndex(ctx, index)
	if err != nil {
		return nil, err
	}

	event, err := c.config.EventToHelloReplyEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to HelloReplyEvent: %v", err)
	}

	return event, nil
}

func (c *_GreeterClient) AwaitHelloReplyEvent(ctx context.Context, id string) (*HelloReplyEvent, error) {
	
	channel := c.db.Channel(c.channel, c.config.HelloReplyTopic())
	defer channel.Close()

	deqEvent, err := channel.Await(ctx, id)
	if err != nil {
		return nil, err
	}

	event, err := c.config.EventToHelloReplyEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to HelloReplyEvent: %v", err)
	}

	return event, nil
}

func (c *_GreeterClient) SubHelloReplyEvent(ctx context.Context, handler func(context.Context, *HelloReplyEvent) ack.Code) error {
	channel := c.db.Channel(c.channel, c.config.HelloReplyTopic())
	defer channel.Close()

	return channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, ack.Code) {
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
		return nil, fmt.Errorf("pub: %v", err)
	}

	e, err = c.config.EventToHelloReplyEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to HelloReplyEvent: %v", err)
	}

	return e, nil
}

func (c *_GreeterClient) DelHelloReplyEvent(ctx context.Context, id string) error {
	return c.db.Del(ctx, c.config.HelloReplyTopic(), id)
}


func (c *_GreeterClient) SayHello(ctx context.Context, e *HelloRequestEvent) (*HelloReplyEvent, error) {

	_, err := c.PubHelloRequestEvent(ctx, e)
	if err != nil {
		return nil, fmt.Errorf("pub: %v", err)
	}

	result, err := c.AwaitHelloReplyEvent(ctx, e.ID)
	if err != nil {
		return nil, fmt.Errorf("get response: %v", err)
	}

	return result, nil
}

type GreeterHandlers interface {
	SayHello(ctx context.Context, req *HelloRequestEvent) (*HelloReplyEvent, ack.Code)
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	
	wg.Add(1)
  go func() {
		defer wg.Done()
		
		channel := s.db.Channel(strings.TrimSuffix(s.channel, ".") + ".SayHello", s.config.HelloRequestTopic())
		defer channel.Close()
		
		err := channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, ack.Code) {
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


