///
//  Generated code. Do not modify.
//  source: {{ .Source }}
///
package {{ .Package }}

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/ack"
)

type TopicConfig struct {
	topics map[string]string
}

func NewTopicConfig() *TopicConfig {
	return &TopicConfig{
		topics: make(map[string]string),
	}
}

{{- range .Types }}

type {{.GoName}}Event struct {
	ID 				   string
	Msg 			   *{{.GoName}}
	CreateTime   time.Time
	DefaultState deq.EventState
	State        deq.EventState
	Indexes      []string
}

func (c *TopicConfig) EventTo{{.GoName}}Event(e deq.Event) (*{{.GoName}}Event, error) {

	if e.Topic != c.{{.GoName}}Topic() {
		return nil, fmt.Errorf("incorrect topic %s", e.Topic)
	}

	msg := new({{ .GoName }})
	err := msg.Unmarshal(e.Payload)
	if err != nil {
		return nil, fmt.Errorf("unmarshal payload: %v", err)
	}

	return &{{.GoName}}Event{
		ID:           e.ID,
		Msg:          msg,
		CreateTime:   e.CreateTime,
		DefaultState: e.DefaultState,
		State:        e.State,
		Indexes:      e.Indexes,
	}, nil
}


func (c *TopicConfig) {{.GoName}}EventToEvent(e *{{.GoName}}Event) (deq.Event, error) {

	buf, err := e.Msg.Marshal()
	if err != nil {
		return deq.Event{}, err
	}

	return deq.Event{
		ID:           e.ID,
		Payload:      buf,
		CreateTime:   e.CreateTime,
		DefaultState: e.DefaultState,
		State:        e.State,
		Topic:        c.{{.GoName}}Topic(),
		Indexes:      e.Indexes,
	}, nil
}

func (c *TopicConfig) {{.GoName}}Topic() string {
	if c == nil {
		return "{{.ProtoFullName}}"
	}

	topic, ok := c.topics["{{.GoName}}"]
	if ok {
		return topic
	}
	return "{{.ProtoFullName}}"
}

func (c *TopicConfig) Set{{.GoName}}Topic(topic string) {
	c.topics["{{.GoName}}"] = topic
}

// {{.GoName}}EventIter is an iterator for {{.GoName}}Events. It has an identical interface to
// deq.EventIter, except that the Event method returns a {{.GoName}}Event.
type {{.GoName}}EventIter struct {
	iter   deq.EventIter
	config *TopicConfig
}

// Next returns the next {{.GoName}}Event, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *{{.GoName}}EventIter) Next(ctx context.Context) (*{{.GoName}}Event, error) {

	if !it.iter.Next(ctx) {
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.iter.Event()

	e, err := it.config.EventTo{{.GoName}}Event(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to {{.GoName}}Event: %v", err)
	}

	return e, nil
}

func (it *{{.GoName}}EventIter) Close() {
	it.iter.Close()
}

{{- end -}}

{{ range .Services }}

{{ template "client" . }}
{{ template "service" . }}
{{ end -}}

{{ define "client" }}
{{- $ServiceName := .Name -}}
type {{ .Name }}Client struct {
	db      deq.Client
	channel string
	config *TopicConfig
}

func New{{ .Name }}Client(db deq.Client, channel string, config *TopicConfig) *{{.Name}}Client {
	return &{{.Name}}Client{
		db: db,
		channel: channel,
		config: config,
	}
}

func (c *{{.Name}}Client) SyncAllTo(ctx context.Context, remote deq.Client) error {
	errc := make(chan error, 1)
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(ctx)
	
	{{ range .Types }}
	wg.Add(1)
	go func() {
		defer wg.Done()

		channel := c.db.Channel(c.channel, c.config.{{.GoName}}Topic())
		defer channel.Close()

		err := deq.SyncTo(ctx, remote, channel)
		if err != nil {
			select {
			default:
			case errc <- err:
			}
		}
	}()
	{{ end }}

	go func() { 
		wg.Wait()
		close(errc)
	}()

	err := <-errc
	cancel()
	wg.Wait()

	return err
} 

{{- range .Types }}
func (c *{{ $ServiceName }}Client) Get{{ .GoName }}Event(ctx context.Context, id string) (*{{ .GoName }}Event, error) {
	
	channel := c.db.Channel(c.channel, c.config.{{.GoName}}Topic())
	defer channel.Close()

	deqEvent, err := channel.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	event, err := c.config.EventTo{{.GoName}}Event(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to {{.GoName}}Event: %v", err)
	}

	return event, nil
}

func (c *{{ $ServiceName }}Client) Await{{ .GoName }}Event(ctx context.Context, id string) (*{{ .GoName }}Event, error) {
	
	channel := c.db.Channel(c.channel, c.config.{{.GoName}}Topic())
	defer channel.Close()

	deqEvent, err := channel.Await(ctx, id)
	if err != nil {
		return nil, err
	}

	event, err := c.config.EventTo{{.GoName}}Event(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to {{.GoName}}Event: %v", err)
	}

	return event, nil
}

func (c *{{ $ServiceName }}Client) Sub{{ .GoName }}Event(ctx context.Context, handler func(context.Context, *{{.GoName}}Event) ack.Code) error {
	channel := c.db.Channel(c.channel, c.config.{{.GoName}}Topic())
	defer channel.Close()

	return channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, ack.Code) {
			event, err := c.config.EventTo{{.GoName}}Event(e)
			if err != nil {
				panic("convert deq.Event to {{.GoName}}Event: " + err.Error())
			}

			return nil, handler(ctx, event)
	})
}

func (c *{{ $ServiceName }}Client) New{{ .GoName }}EventIter(opts deq.IterOpts) *{{ .GoName }}EventIter {
	
	channel := c.db.Channel(c.channel, c.config.{{.GoName}}Topic())
	defer channel.Close()
	
	return &{{.GoName}}EventIter{
		iter:   channel.NewEventIter(opts),
		config: c.config,
	}
}

func (c *{{ $ServiceName }}Client) New{{ .GoName }}IndexIter(opts deq.IterOpts) *{{ .GoName }}EventIter {
	
	channel := c.db.Channel(c.channel, c.config.{{.GoName}}Topic())
	defer channel.Close()
	
	return &{{.GoName}}EventIter{
		iter:   channel.NewIndexIter(opts),
		config: c.config,
	}
}

func (c *{{ $ServiceName }}Client) Pub{{.GoName}}Event(ctx context.Context, e *{{.GoName}}Event) (*{{.GoName}}Event, error) {
	deqEvent, err := c.config.{{.GoName}}EventToEvent(e)
	if err != nil {
		return nil, fmt.Errorf("convert {{.GoName}}Event to deq.Event: %v", err)
	}

	deqEvent, err = c.db.Pub(ctx, deqEvent)
	if err != nil {
		return nil, fmt.Errorf("pub: %v", err)
	}

	e, err = c.config.EventTo{{.GoName}}Event(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to {{.GoName}}Event: %v", err)
	}

	return e, nil
}

func (c *{{ $ServiceName }}Client) Del{{.GoName}}Event(ctx context.Context, id string) error {
	return c.db.Del(ctx, c.config.{{.GoName}}Topic(), id)
}
{{ end -}}

{{- range .Methods }}

func (c *{{ $ServiceName }}Client) {{ .Name }}(ctx context.Context, e *{{.InType}}Event) (*{{.OutType}}Event, error) {

	_, err := c.Pub{{.InType}}Event(ctx, e)
	if err != nil {
		return nil, fmt.Errorf("pub: %v", err)
	}

	channel := c.db.Channel(c.channel, c.config.{{.OutType}}Topic())
	defer channel.Close()

	deqResult, err := channel.Await(ctx, e.ID)
	if err != nil {
		return nil, fmt.Errorf("get response: %v", err)
	}

	result, err := c.config.EventTo{{.OutType}}Event(deqResult)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to {{.OutType}}Event: %v", err)
	}

	return result, nil
}
{{- end -}}
{{ end }}

{{ define "service" }}
type {{ .Name }}Handlers interface {
{{- range .Methods }}
	{{ .Name }}(ctx context.Context, req *{{ .InType }}Event) (*{{ .OutType }}Event, ack.Code)
{{- end }}
}

type {{ .Name }}Server struct {
	handlers {{ .Name }}Handlers
	db       deq.Client
	channel  string
	done     chan struct{}
	config   *TopicConfig
}

func New{{ .Name }}Server(db deq.Client, handlers {{ .Name }}Handlers, channel string, config *TopicConfig) (*{{ .Name }}Server) {
	return &{{ .Name }}Server{
		handlers: handlers,
		channel: channel,
		db: db,
		done: make(chan struct{}),
		config: config,
	}
}

func (s *{{ .Name }}Server) Listen(ctx context.Context) error {
	errc := make(chan error, 1)
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	{{ range .Methods }}
	wg.Add(1)
  go func() {
		defer wg.Done()
		
		channel := s.db.Channel(s.channel, s.config.{{.InType}}Topic())
		defer channel.Close()
		
		err := channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, ack.Code) {
			event, err := s.config.EventTo{{.InType}}Event(e)
			if err != nil {
				panic("convert deq.Event to {{.InType}}Event: " + err.Error())
			}

			response, code := s.handlers.{{ .Name }}(ctx, event)
			if response == nil {
				return nil, code
			}

			deqResponse, err := s.config.{{.OutType}}EventToEvent(response)
			if err != nil {
				panic("convert response of type {{.OutType}}Event to deq.Event: " + err.Error())
			}
			return &deqResponse, code
		})
		if err != nil {
			if err != ctx.Err() {
				err = fmt.Errorf("{{.Name}}: %v", err)
			}
			// If no one is recieving then the outer function already has returned
			select {
			case errc <- err:
			default:
			}
		}
	}()
{{- end }}

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

func (s *{{ .Name }}Server) Close() {
	close(s.done)
}

{{- end -}}

