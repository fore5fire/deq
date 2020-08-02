package deqclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"os"

	"gitlab.com/katcheCode/deq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func init() {
	deq.RegisterProvider("h2c", provider)
	deq.RegisterProvider("https", provider)
}

var provider remoteProvider

type remoteProvider struct{}

func (remoteProvider) Open(ctx context.Context, u *url.URL) (deq.Client, error) {
	if u.Scheme != "h2c" && u.Scheme != "https" {
		return nil, fmt.Errorf("parse connection URI: remote client requires scheme h2c or https")
	}
	if u.Path != "" {
		return nil, fmt.Errorf("parse connection URI: path is not permitted with scheme h2c or https")
	}

	secret := os.Getenv("DEQ_CLIENT_SIMPLE_TOKEN")

	var opts []grpc.DialOption
	if secret != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(&sharedSecretCredentials{secret}))
	}
	if u.Scheme == "h2c" {
		opts = append(opts, grpc.WithInsecure())
	} else {
		creds := credentials.NewTLS(&tls.Config{
			ServerName: u.Query().Get("san_override"),
		})
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}

	conn, err := grpc.DialContext(ctx, u.Host, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial host %q: %v", u.Host, err)
	}
	client := New(conn)
	client.SetDefaultChannel(u.Query().Get("channel"))
	return client, nil
}

type sharedSecretCredentials struct {
	Secret string
}

func (c *sharedSecretCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": c.Secret,
	}, nil
}

func (*sharedSecretCredentials) RequireTransportSecurity() bool {
	return false
}
