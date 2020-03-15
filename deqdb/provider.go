package deqdb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"

	"gitlab.com/katcheCode/deq"
)

func init() {
	deq.RegisterProvider("file", localProvider{})
}

type localProvider struct{}

func (localProvider) Open(ctx context.Context, uri *url.URL) (deq.Client, error) {
	opts, err := parseConnectionString(uri)
	if err != nil {
		return nil, fmt.Errorf("parse connection string")
	}

	db, err := Open(opts)
	if err != nil {
		return nil, err
	}
	return AsClient(db), nil
}

// ParseConnectionString parses a connection string with scheme file:// and
// returns corresponding options.
//
// Debug logging, returning an error when opening an improperly closed database,
// and allowing database disk format upgrades can be enabled in the returned
// options object by adding debug=true, keepCorrupt=true, or
// upgradeIfNeeded=true respectively in uri's query string.
func ParseConnectionString(uri string) (Options, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return Options{}, err
	}
	return parseConnectionString(u)
}

func parseConnectionString(u *url.URL) (opts Options, err error) {
	if u.Scheme != "file" {
		return opts, errors.New("local client requires scheme file://")
	}

	path := filepath.FromSlash(u.Path)
	// Relative paths interpreate the first section as the host
	if u.Host != "" {
		path = filepath.Join(u.Host, path)
	}

	opts.Dir = path
	opts.KeepCorrupt = u.Query().Get("keepCorrupt") == "true"
	opts.UpgradeIfNeeded = u.Query().Get("upgradeIfNeeded") == "true"
	opts.Info = log.New(os.Stderr, "", log.LstdFlags)

	if u.Query().Get("debug") == "true" {
		opts.Debug = log.New(os.Stdout, "DEBUG: ", log.LstdFlags)
	}

	return opts, nil
}
