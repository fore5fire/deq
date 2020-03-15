package deqclient

import (
	"crypto/tls"
	"fmt"
	"net/url"

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

	var opts []grpc.DialOption
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
	return New(conn), nil
}
