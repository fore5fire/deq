///
//  Generated code. Do not modify.
//  source: example/greeter/greeter2.proto
///
package greeter

import (
	"context"
	"fmt"
	"sync"
	"strings"
	

	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/deqopt"
	
	deqtype "gitlab.com/katcheCode/deq/deqtype"
	types "github.com/gogo/protobuf/types"
)
type Greeter2TopicConfig struct {
	topics map[string]string
}

func NewGreeter2TopicConfig() *Greeter2TopicConfig {
	return &Greeter2TopicConfig{
		topics: make(map[string]string),
	}
}

func (c *Greeter2TopicConfig) EventToHelloRequestEvent(e deq.Event) (*HelloRequestEvent, error) {

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

func (c *Greeter2TopicConfig) HelloRequestEventToEvent(e *HelloRequestEvent) (deq.Event, error) {

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

func (c *Greeter2TopicConfig) HelloRequestTopic() string {
	if c == nil {
		return "greeter.HelloRequest"
	}

	topic, ok := c.topics["greeter.HelloRequest"]
	if ok {
		return topic
	}
	return "greeter.HelloRequest"
}

func (c *Greeter2TopicConfig) SetHelloRequestTopic(topic string) {
	c.topics["greeter.HelloRequest"] = topic
}

func (c *Greeter2TopicConfig) EventToHelloReplyEvent(e deq.Event) (*HelloReplyEvent, error) {

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

func (c *Greeter2TopicConfig) HelloReplyEventToEvent(e *HelloReplyEvent) (deq.Event, error) {

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

func (c *Greeter2TopicConfig) HelloReplyTopic() string {
	if c == nil {
		return "greeter.HelloReply"
	}

	topic, ok := c.topics["greeter.HelloReply"]
	if ok {
		return topic
	}
	return "greeter.HelloReply"
}

func (c *Greeter2TopicConfig) SetHelloReplyTopic(topic string) {
	c.topics["greeter.HelloReply"] = topic
}

func (c *Greeter2TopicConfig) EventToEmptyEvent(e deq.Event) (*deqtype.EmptyEvent, error) {

	if e.Topic != c.EmptyTopic() {
		return nil, fmt.Errorf("incorrect topic %s", e.Topic)
	}

	msg := new(types.Empty)
	err := msg.Unmarshal(e.Payload)
	if err != nil {
		return nil, fmt.Errorf("unmarshal payload: %v", err)
	}

	return &deqtype.EmptyEvent{
		ID:           e.ID,
		Empty:  msg,
		CreateTime:   e.CreateTime,
		DefaultState: e.DefaultState,
		State:        e.State,
		Indexes:      e.Indexes,
		
		Selector:        e.Selector,
		SelectorVersion: e.SelectorVersion,
	}, nil
}

func (c *Greeter2TopicConfig) EmptyEventToEvent(e *deqtype.EmptyEvent) (deq.Event, error) {

	buf, err := e.Empty.Marshal()
	if err != nil {
		return deq.Event{}, err
	}

	return deq.Event{
		ID:           e.ID,
		Payload:      buf,
		CreateTime:   e.CreateTime,
		DefaultState: e.DefaultState,
		State:        e.State,
		Topic:        c.EmptyTopic(),
		Indexes:      e.Indexes,

		Selector:        e.Selector,
		SelectorVersion: e.SelectorVersion,
	}, nil
}

func (c *Greeter2TopicConfig) EmptyTopic() string {
	if c == nil {
		return "google.protobuf.Empty"
	}

	topic, ok := c.topics["google.protobuf.Empty"]
	if ok {
		return topic
	}
	return "google.protobuf.Empty"
}

func (c *Greeter2TopicConfig) SetEmptyTopic(topic string) {
	c.topics["google.protobuf.Empty"] = topic
}


type Greeter2Client interface {
	SyncAllTo(ctx context.Context, remote deq.Client) error
	GetHelloRequestEvent(ctx context.Context, id string, options ...deqopt.GetOption) (*HelloRequestEvent, error)
	BatchGetHelloRequestEvents(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]*HelloRequestEvent, error)
	SubHelloRequestEvent(ctx context.Context, handler func(context.Context, *HelloRequestEvent) error) error
	NewHelloRequestEventIter(opts *deq.IterOptions) HelloRequestEventIter
	NewHelloRequestIndexIter(opts *deq.IterOptions) HelloRequestEventIter
	PubHelloRequestEvent(ctx context.Context, e *HelloRequestEvent) (*HelloRequestEvent, error)
	
	GetHelloReplyEvent(ctx context.Context, id string, options ...deqopt.GetOption) (*HelloReplyEvent, error)
	BatchGetHelloReplyEvents(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]*HelloReplyEvent, error)
	SubHelloReplyEvent(ctx context.Context, handler func(context.Context, *HelloReplyEvent) error) error
	NewHelloReplyEventIter(opts *deq.IterOptions) HelloReplyEventIter
	NewHelloReplyIndexIter(opts *deq.IterOptions) HelloReplyEventIter
	PubHelloReplyEvent(ctx context.Context, e *HelloReplyEvent) (*HelloReplyEvent, error)
	
	GetEmptyEvent(ctx context.Context, id string, options ...deqopt.GetOption) (*deqtype.EmptyEvent, error)
	BatchGetEmptyEvents(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]*deqtype.EmptyEvent, error)
	SubEmptyEvent(ctx context.Context, handler func(context.Context, *deqtype.EmptyEvent) error) error
	NewEmptyEventIter(opts *deq.IterOptions) deqtype.EmptyEventIter
	NewEmptyIndexIter(opts *deq.IterOptions) deqtype.EmptyEventIter
	PubEmptyEvent(ctx context.Context, e *deqtype.EmptyEvent) (*deqtype.EmptyEvent, error)
	
	SayHello(ctx context.Context, e *HelloRequestEvent) (*HelloReplyEvent, error)
	
	SayNothing(ctx context.Context, e *HelloRequestEvent) (*deqtype.EmptyEvent, error)
	}

type _Greeter2Client struct {
	db      deq.Client
	channel string
	config *Greeter2TopicConfig
}

func NewGreeter2Client(db deq.Client, channel string, config *Greeter2TopicConfig) Greeter2Client {
	if channel == "" {
		panic("channel is required")
	}
	
	return &_Greeter2Client{
		db: db,
		channel: channel,
		config: config,
	}
}

func (c *_Greeter2Client) SyncAllTo(ctx context.Context, remote deq.Client) error {
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
	
	wg.Add(1)
	go func() {
		defer wg.Done()

		channel := c.db.Channel(c.channel, c.config.EmptyTopic())
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
func (c *_Greeter2Client) GetHelloRequestEvent(ctx context.Context, id string, options ...deqopt.GetOption) (*HelloRequestEvent, error) {
	
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

func (c *_Greeter2Client) BatchGetHelloRequestEvents(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]*HelloRequestEvent, error) {
	
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

func (c *_Greeter2Client) SubHelloRequestEvent(ctx context.Context, handler func(context.Context, *HelloRequestEvent) error) error {
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

func (c *_Greeter2Client) NewHelloRequestEventIter(opts *deq.IterOptions) HelloRequestEventIter {
	
	channel := c.db.Channel(c.channel, c.config.HelloRequestTopic())
	defer channel.Close()
	
	return &XXX_HelloRequestEventIter{
		Iter:   channel.NewEventIter(opts),
		Config: c.config,
	}
}

func (c *_Greeter2Client) NewHelloRequestIndexIter(opts *deq.IterOptions) HelloRequestEventIter {
	
	channel := c.db.Channel(c.channel, c.config.HelloRequestTopic())
	defer channel.Close()
	
	return &XXX_HelloRequestEventIter{
		Iter:   channel.NewIndexIter(opts),
		Config: c.config,
	}
}

func (c *_Greeter2Client) PubHelloRequestEvent(ctx context.Context, e *HelloRequestEvent) (*HelloRequestEvent, error) {
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


func (c *_Greeter2Client) GetHelloReplyEvent(ctx context.Context, id string, options ...deqopt.GetOption) (*HelloReplyEvent, error) {
	
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

func (c *_Greeter2Client) BatchGetHelloReplyEvents(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]*HelloReplyEvent, error) {
	
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

func (c *_Greeter2Client) SubHelloReplyEvent(ctx context.Context, handler func(context.Context, *HelloReplyEvent) error) error {
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

func (c *_Greeter2Client) NewHelloReplyEventIter(opts *deq.IterOptions) HelloReplyEventIter {
	
	channel := c.db.Channel(c.channel, c.config.HelloReplyTopic())
	defer channel.Close()
	
	return &XXX_HelloReplyEventIter{
		Iter:   channel.NewEventIter(opts),
		Config: c.config,
	}
}

func (c *_Greeter2Client) NewHelloReplyIndexIter(opts *deq.IterOptions) HelloReplyEventIter {
	
	channel := c.db.Channel(c.channel, c.config.HelloReplyTopic())
	defer channel.Close()
	
	return &XXX_HelloReplyEventIter{
		Iter:   channel.NewIndexIter(opts),
		Config: c.config,
	}
}

func (c *_Greeter2Client) PubHelloReplyEvent(ctx context.Context, e *HelloReplyEvent) (*HelloReplyEvent, error) {
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


func (c *_Greeter2Client) GetEmptyEvent(ctx context.Context, id string, options ...deqopt.GetOption) (*deqtype.EmptyEvent, error) {
	
	channel := c.db.Channel(c.channel, c.config.EmptyTopic())
	defer channel.Close()

	deqEvent, err := channel.Get(ctx, id, options...)
	if err != nil {
		return nil, err
	}

	event, err := c.config.EventToEmptyEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to deqtype.EmptyEvent: %v", err)
	}

	return event, nil
}

func (c *_Greeter2Client) BatchGetEmptyEvents(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]*deqtype.EmptyEvent, error) {
	
	channel := c.db.Channel(c.channel, c.config.EmptyTopic())
	defer channel.Close()

	deqEvents, err := channel.BatchGet(ctx, ids, options...)
	if err != nil {
		return nil, err
	}

	events := make(map[string]*deqtype.EmptyEvent, len(deqEvents))
	for a, e := range deqEvents {
		event, err := c.config.EventToEmptyEvent(e)
		if err != nil {
			return nil, fmt.Errorf("convert deq.Event to deqtype.EmptyEvent: %v", err)
		}
		events[a] = event 
	}

	return events, nil
}

func (c *_Greeter2Client) SubEmptyEvent(ctx context.Context, handler func(context.Context, *deqtype.EmptyEvent) error) error {
	channel := c.db.Channel(c.channel, c.config.EmptyTopic())
	defer channel.Close()

	return channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {
			event, err := c.config.EventToEmptyEvent(e)
			if err != nil {
				panic("convert deq.Event to deqtype.EmptyEvent: " + err.Error())
			}

			return nil, handler(ctx, event)
	})
}

func (c *_Greeter2Client) NewEmptyEventIter(opts *deq.IterOptions) deqtype.EmptyEventIter {
	
	channel := c.db.Channel(c.channel, c.config.EmptyTopic())
	defer channel.Close()
	
	return &deqtype.XXX_EmptyEventIter{
		Iter:   channel.NewEventIter(opts),
		Config: c.config,
	}
}

func (c *_Greeter2Client) NewEmptyIndexIter(opts *deq.IterOptions) deqtype.EmptyEventIter {
	
	channel := c.db.Channel(c.channel, c.config.EmptyTopic())
	defer channel.Close()
	
	return &deqtype.XXX_EmptyEventIter{
		Iter:   channel.NewIndexIter(opts),
		Config: c.config,
	}
}

func (c *_Greeter2Client) PubEmptyEvent(ctx context.Context, e *deqtype.EmptyEvent) (*deqtype.EmptyEvent, error) {
	deqEvent, err := c.config.EmptyEventToEvent(e)
	if err != nil {
		return nil, fmt.Errorf("convert deqtype.EmptyEvent to deq.Event: %v", err)
	}

	deqEvent, err = c.db.Pub(ctx, deqEvent)
	if err != nil {
		return nil, fmt.Errorf("pub: %v", err)
	}

	e, err = c.config.EventToEmptyEvent(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to deqtype.EmptyEvent: %v", err)
	}

	return e, nil
}



func (c *_Greeter2Client) SayHello(ctx context.Context, e *HelloRequestEvent) (*HelloReplyEvent, error) {

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

func (c *_Greeter2Client) SayNothing(ctx context.Context, e *HelloRequestEvent) (*deqtype.EmptyEvent, error) {

	_, err := c.PubHelloRequestEvent(ctx, e)
	if err != nil {
		return nil, fmt.Errorf("pub: %v", err)
	}

	result, err := c.GetEmptyEvent(ctx, e.ID, deqopt.Await())
	if err != nil {
		return nil, fmt.Errorf("get response: %v", err)
	}

	return result, nil
}

type Greeter2Handlers interface {
	SayHello(ctx context.Context, req *HelloRequestEvent) (*HelloReplyEvent, error)
	SayNothing(ctx context.Context, req *HelloRequestEvent) (*deqtype.EmptyEvent, error)
}

type Greeter2Server interface {
	Listen(ctx context.Context) error
	Close()
}

type _Greeter2Server struct {
	handlers Greeter2Handlers
	db       deq.Client
	channel  string
	done     chan struct{}
	config   *Greeter2TopicConfig
}

func NewGreeter2Server(db deq.Client, handlers Greeter2Handlers, channelPrefix string, config *Greeter2TopicConfig) Greeter2Server {
	return &_Greeter2Server{
		handlers: handlers,
		channel: channelPrefix,
		db: db,
		done: make(chan struct{}),
		config: config,
	}
}

func (s *_Greeter2Server) Listen(ctx context.Context) error {
	errc := make(chan error, 1)
	wg := sync.WaitGroup{}
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
	wg.Add(1)
  go func() {
		defer wg.Done()
		
		channel := s.db.Channel(strings.TrimSuffix(s.channel, ".") + ".SayNothing", s.config.HelloRequestTopic())
		defer channel.Close()
		
		err := channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {
			event, err := s.config.EventToHelloRequestEvent(e)
			if err != nil {
				panic("convert deq.Event to HelloRequestEvent: " + err.Error())
			}

			response, code := s.handlers.SayNothing(ctx, event)
			if response == nil {
				return nil, code
			}

			deqResponse, err := s.config.EmptyEventToEvent(response)
			if err != nil {
				panic("convert response of type EmptyEvent to deq.Event: " + err.Error())
			}
			return &deqResponse, code
		})
		if err != nil {
			if err != ctx.Err() {
				err = fmt.Errorf("SayNothing: %v", err)
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

func (s *_Greeter2Server) Close() {
	close(s.done)
}


