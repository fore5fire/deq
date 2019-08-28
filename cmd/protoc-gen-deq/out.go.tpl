///
//  Generated code. Do not modify.
//  source: {{ .Source }}
///
package {{ .Package }}

import (
	"context"
	"fmt"
	{{ if .HasMethods -}}
	"sync"
	"strings"
	{{ end -}}
	{{if .Types}}"time"{{end}}

	"gitlab.com/katcheCode/deq"
	{{ if .HasMethods -}}
	"gitlab.com/katcheCode/deq/deqopt"
	{{ end -}}
	{{- range $pkg, $path := .Imports }}
	{{$pkg}} "{{$path}}"
	{{- end }}
)

{{- range .Types }}

type {{.GoName}}Event struct {
	ID 				   string
	CreateTime   time.Time
	DefaultState deq.State
	State        deq.State
	Indexes      []string

	Selector        string
	SelectorVersion int64

	{{.GoName}} *{{.GoRef}}
}

type _{{.GoName}}TopicConfig interface {
	EventTo{{.GoName}}Event(deq.Event) (*{{.GoName}}Event, error)
}

// {{.GoName}}EventIter is an iterator for {{.GoName}}Events. It has an identical interface to
// deq.EventIter, except that the Event method returns a {{.GoName}}Event.
type {{.GoName}}EventIter interface {
	Next(ctx context.Context) (*{{.GoName}}Event, error)
	Close()
}

type XXX_{{.GoName}}EventIter struct {
	Iter   deq.EventIter
	Config _{{.GoName}}TopicConfig
}

// Next returns the next {{.GoName}}Event, deq.ErrIterationComplete if iteration completed, or an error,
// if one occured. See deq.EventIter.Next for more information.
func (it *XXX_{{.GoName}}EventIter) Next(ctx context.Context) (*{{.GoName}}Event, error) {

	if !it.Iter.Next(ctx) {
		if it.Iter.Err() != nil {
			return nil, it.Iter.Err()
		}
		return nil, deq.ErrIterationComplete
	}
	
	deqEvent := it.Iter.Event()

	e, err := it.Config.EventTo{{.GoName}}Event(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to {{.GoName}}Event: %v", err)
	}

	return e, nil
}

func (it *XXX_{{.GoName}}EventIter) Close() {
	it.Iter.Close()
}
{{ end -}}

{{ range .Services }}
type {{.Name}}TopicConfig struct {
	topics map[string]string
}

func New{{.Name}}TopicConfig() *{{.Name}}TopicConfig {
	return &{{.Name}}TopicConfig{
		topics: make(map[string]string),
	}
}
{{ $ServiceName := .Name -}}
{{ range .Types }}
func (c *{{$ServiceName}}TopicConfig) EventTo{{.GoName}}Event(e deq.Event) (*{{.GoEventRef}}Event, error) {

	if e.Topic != c.{{.GoName}}Topic() {
		return nil, fmt.Errorf("incorrect topic %s", e.Topic)
	}

	msg := new({{ .GoRef }})
	err := msg.Unmarshal(e.Payload)
	if err != nil {
		return nil, fmt.Errorf("unmarshal payload: %v", err)
	}

	return &{{.GoEventRef}}Event{
		ID:           e.ID,
		{{.GoName}}:  msg,
		CreateTime:   e.CreateTime,
		DefaultState: e.DefaultState,
		State:        e.State,
		Indexes:      e.Indexes,
		
		Selector:        e.Selector,
		SelectorVersion: e.SelectorVersion,
	}, nil
}

func (c *{{$ServiceName}}TopicConfig) {{.GoName}}EventToEvent(e *{{.GoEventRef}}Event) (deq.Event, error) {

	buf, err := e.{{.GoName}}.Marshal()
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

		Selector:        e.Selector,
		SelectorVersion: e.SelectorVersion,
	}, nil
}

func (c *{{$ServiceName}}TopicConfig) {{.GoName}}Topic() string {
	if c == nil {
		return "{{.ProtoFullName}}"
	}

	topic, ok := c.topics["{{.ProtoFullName}}"]
	if ok {
		return topic
	}
	return "{{.ProtoFullName}}"
}

func (c *{{$ServiceName}}TopicConfig) Set{{.GoName}}Topic(topic string) {
	c.topics["{{.ProtoFullName}}"] = topic
}
{{ end }}

{{ template "client" . }}
{{ template "service" . }}
{{ end -}}

{{ define "client" }}
{{- $ServiceName := .Name -}}
type {{ .Name }}Client interface {
	SyncAllTo(ctx context.Context, remote deq.Client) error
	{{- range .Types }}
	Get{{ .GoName }}Event(ctx context.Context, id string, options ...deqopt.GetOption) (*{{ .GoEventRef }}Event, error)
	BatchGet{{ .GoName }}Events(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]*{{ .GoEventRef }}Event, error)
	Sub{{ .GoName }}Event(ctx context.Context, handler func(context.Context, *{{.GoEventRef}}Event) error) error
	New{{ .GoName }}EventIter(opts *deq.IterOptions) {{ .GoEventRef }}EventIter
	New{{ .GoName }}IndexIter(opts *deq.IterOptions) {{ .GoEventRef }}EventIter
	Pub{{.GoName}}Event(ctx context.Context, e *{{.GoEventRef}}Event) (*{{.GoEventRef}}Event, error)
	{{ end -}}
	{{ range .Methods }}
	{{ .Name }}(ctx context.Context, e *{{.InType.GoEventRef}}Event) (*{{.OutType.GoEventRef}}Event, error)
	{{ end -}}
}

type _{{ .Name }}Client struct {
	db      deq.Client
	channel string
	config *{{$ServiceName}}TopicConfig
}

func New{{ .Name }}Client(db deq.Client, channel string, config *{{$ServiceName}}TopicConfig) {{.Name}}Client {
	if channel == "" {
		panic("channel is required")
	}
	
	return &_{{.Name}}Client{
		db: db,
		channel: channel,
		config: config,
	}
}

func (c *_{{.Name}}Client) SyncAllTo(ctx context.Context, remote deq.Client) error {
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
func (c *_{{ $ServiceName }}Client) Get{{ .GoName }}Event(ctx context.Context, id string, options ...deqopt.GetOption) (*{{ .GoEventRef }}Event, error) {
	
	channel := c.db.Channel(c.channel, c.config.{{.GoName}}Topic())
	defer channel.Close()

	deqEvent, err := channel.Get(ctx, id, options...)
	if err != nil {
		return nil, err
	}

	event, err := c.config.EventTo{{.GoName}}Event(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to {{.GoEventRef}}Event: %v", err)
	}

	return event, nil
}

func (c *_{{ $ServiceName }}Client) BatchGet{{ .GoName }}Events(ctx context.Context, ids []string, options ...deqopt.BatchGetOption) (map[string]*{{ .GoEventRef }}Event, error) {
	
	channel := c.db.Channel(c.channel, c.config.{{.GoName}}Topic())
	defer channel.Close()

	deqEvents, err := channel.BatchGet(ctx, ids, options...)
	if err != nil {
		return nil, err
	}

	events := make(map[string]*{{.GoEventRef}}Event, len(deqEvents))
	for a, e := range deqEvents {
		event, err := c.config.EventTo{{.GoName}}Event(e)
		if err != nil {
			return nil, fmt.Errorf("convert deq.Event to {{.GoEventRef}}Event: %v", err)
		}
		events[a] = event 
	}

	return events, nil
}

func (c *_{{ $ServiceName }}Client) Sub{{ .GoName }}Event(ctx context.Context, handler func(context.Context, *{{.GoEventRef}}Event) error) error {
	channel := c.db.Channel(c.channel, c.config.{{.GoName}}Topic())
	defer channel.Close()

	return channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {
			event, err := c.config.EventTo{{.GoName}}Event(e)
			if err != nil {
				panic("convert deq.Event to {{.GoEventRef}}Event: " + err.Error())
			}

			return nil, handler(ctx, event)
	})
}

func (c *_{{ $ServiceName }}Client) New{{ .GoName }}EventIter(opts *deq.IterOptions) {{ .GoEventRef }}EventIter {
	
	channel := c.db.Channel(c.channel, c.config.{{.GoName}}Topic())
	defer channel.Close()
	
	return &{{.GoEventPkg}}XXX_{{.GoName}}EventIter{
		Iter:   channel.NewEventIter(opts),
		Config: c.config,
	}
}

func (c *_{{ $ServiceName }}Client) New{{ .GoName }}IndexIter(opts *deq.IterOptions) {{ .GoEventRef }}EventIter {
	
	channel := c.db.Channel(c.channel, c.config.{{.GoName}}Topic())
	defer channel.Close()
	
	return &{{.GoEventPkg}}XXX_{{.GoName}}EventIter{
		Iter:   channel.NewIndexIter(opts),
		Config: c.config,
	}
}

func (c *_{{ $ServiceName }}Client) Pub{{.GoName}}Event(ctx context.Context, e *{{.GoEventRef}}Event) (*{{.GoEventRef}}Event, error) {
	deqEvent, err := c.config.{{.GoName}}EventToEvent(e)
	if err != nil {
		return nil, fmt.Errorf("convert {{.GoEventRef}}Event to deq.Event: %v", err)
	}

	deqEvent, err = c.db.Pub(ctx, deqEvent)
	if err != nil {
		return nil, err
	}

	e, err = c.config.EventTo{{.GoName}}Event(deqEvent)
	if err != nil {
		return nil, fmt.Errorf("convert deq.Event to {{.GoEventRef}}Event: %v", err)
	}

	return e, nil
}

{{ end -}}

{{- range .Methods }}

func (c *_{{ $ServiceName }}Client) {{ .Name }}(ctx context.Context, e *{{.InType.GoEventRef}}Event) (*{{.OutType.GoEventRef}}Event, error) {

	_, err := c.Pub{{.InType}}Event(ctx, e)
	if err != nil {
		return nil, fmt.Errorf("pub: %v", err)
	}

	result, err := c.Get{{.OutType}}Event(ctx, e.ID, deqopt.Await())
	if err != nil {
		return nil, fmt.Errorf("get response: %v", err)
	}

	return result, nil
}
{{- end -}}
{{ end }}

{{ define "service" }}
type {{ .Name }}Handlers interface {
{{- range .Methods }}
	{{ .Name }}(ctx context.Context, req *{{ .InType.GoEventRef }}Event) (*{{ .OutType.GoEventRef }}Event, error)
{{- end }}
}

type {{.Name}}Server interface {
	Listen(ctx context.Context) error
	Close()
}

type _{{ .Name }}Server struct {
	handlers {{ .Name }}Handlers
	db       deq.Client
	channel  string
	done     chan struct{}
	config   *{{.Name}}TopicConfig
}

func New{{ .Name }}Server(db deq.Client, handlers {{ .Name }}Handlers, channelPrefix string, config *{{.Name}}TopicConfig) {{ .Name }}Server {
	return &_{{ .Name }}Server{
		handlers: handlers,
		channel: channelPrefix,
		db: db,
		done: make(chan struct{}),
		config: config,
	}
}

func (s *_{{ .Name }}Server) Listen(ctx context.Context) error {
	errc := make(chan error, 1)
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	{{ range .Methods }}
	wg.Add(1)
  go func() {
		defer wg.Done()
		
		channel := s.db.Channel(strings.TrimSuffix(s.channel, ".") + ".{{.Name}}", s.config.{{.InType}}Topic())
		defer channel.Close()
		
		err := channel.Sub(ctx, func(ctx context.Context, e deq.Event) (*deq.Event, error) {
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

func (s *_{{ .Name }}Server) Close() {
	close(s.done)
}

{{- end -}}

