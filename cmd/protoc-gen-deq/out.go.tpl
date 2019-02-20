///
//  Generated code. Do not modify.
//  source: {{ .Source }}
///
package {{ .Package }}

import (
	"context"
	"fmt"
	"log"
	"sync"

	"gitlab.com/katcheCode/deq/deqc"
	"gitlab.com/katcheCode/deq/ack"
	"google.golang.org/grpc"
)

{{- range .Types }}

type {{.Name}}Event struct {
	ID string
	Msg *{{.Name}}
}
{{- end -}}

{{ range .Services }}

{{ template "client" . }}
{{ template "service" . }}
{{ end -}}

{{ define "client" }}
{{- $ServiceName := .Name -}}
type {{ .Name }}Client struct {
	publisher  *deq.Publisher
	subscriber *deq.Subscriber
}

func New{{ .Name }}Client(conn *grpc.ClientConn, channel string) *{{.Name}}Client {
	return &{{.Name}}Client{
		publisher:  deq.NewPublisher(conn, deq.PublisherOpts{}),
		subscriber: deq.NewSubscriber(conn, deq.SubscriberOpts{
			Channel:  channel,
		}),
	}
}

{{- range .Types }}
func (c *{{ $ServiceName }}Client) Get{{ .Name }}(ctx context.Context, id string) ({{ .Name }}Event, error) {
	msg := new({{ .Name }})
	e, err := c.subscriber.Get(ctx, id, msg)
	if err != nil {
		return {{.Name}}Event{}, err
	}
	
	return {{.Name}}Event{
		ID:  e.ID,
		Msg: e.Msg.(*{{.Name}}),
	}, nil
}
{{ end -}}

{{- range .Methods }}

func (s *{{ $ServiceName }}Client) {{ .Name }}(ctx context.Context, e {{.InType}}Event) ({{ .OutType }}Event, error) {
	
	_, err := s.publisher.Pub(ctx, deq.Event{
		ID:  e.ID,
		Msg: e.Msg,
	})
	if err != nil {
		return {{ .OutType }}Event{}, fmt.Errorf("pub: %v", err)
	}

	resultMsg := new({{ .OutType }})
	result, err := s.subscriber.Await(ctx, e.ID, resultMsg)
	if err != nil {
		return {{.OutType}}Event{}, err
	}

	return {{.OutType}}Event{
		ID:  result.ID,
		Msg: resultMsg,
	}, nil
}
{{- end -}}
{{ end }}

{{ define "service" }}
type {{ .Name }}Handlers interface {
{{- range .Methods }}
	{{ .Name }}(ctx context.Context, e deq.Event, req *{{ .InType }}) (*{{ .OutType }}, ack.Code)
{{- end }}
}

type {{ .Name }}Server struct {
	handlers {{ .Name }}Handlers
	subscriber *deq.Subscriber
	publisher  *deq.Publisher
}

func New{{ .Name }}Server(conn *grpc.ClientConn, handlers {{ .Name }}Handlers, channel string) (*{{ .Name }}Server) {
	return &{{ .Name }}Server{
		handlers: handlers,
		subscriber: deq.NewSubscriber(conn, deq.SubscriberOpts{
			Channel: channel,
		}),
		publisher: deq.NewPublisher(conn, deq.PublisherOpts{}),
	}
}

func (s *{{ .Name }}Server) Listen(ctx context.Context) error {
	errorc := make(chan error, 1)
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
{{ range .Methods }}
	wg.Add(1)
  go func() {
		defer wg.Done()
		err := s.subscriber.Sub(ctx, (*{{ .InType }})(nil), func(e deq.Event) ack.Code {
			var code ack.Code
			response, code := s.handlers.{{ .Name }}(ctx, e, e.Msg.(*{{ .InType }}))

			if code == ack.DequeueOK && response != nil {
				_, err := s.publisher.Pub(ctx, deq.Event{
					ID: e.ID,
					Msg: response,
				})
				if err != nil {
					log.Printf("pub response: %v", err)
					return ack.RequeueExponential
				}
			}

			return code
		})
		if err != nil {
			// If no one is recieving then the outer function already has returned
			select {
			case errorc <- err:
			default:
			}
		}
	}()
{{- end }}

	go func() {
		wg.Wait()
		close(errorc)
	}()
	return <-errorc
}

{{- end -}}

