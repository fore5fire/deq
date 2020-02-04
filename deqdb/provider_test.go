package deqdb

import (
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseConnectionStringAbsolute(t *testing.T) {
	expect := Options{
		Dir: filepath.FromSlash("/absolute/path"),
	}
	actual, err := ParseConnectionString("file:///absolute/path")
	if err != nil {
		t.Fatal(err)
	}
	if actual.Debug != nil {
		t.Error("debug logger set")
	}
	if actual.Info == nil {
		t.Error("info logger not set")
	}
	actual.Info = nil
	if diff := cmp.Diff(expect, actual); diff != "" {
		t.Error(diff)
	}
}

func TestParseConnectionStringRelative(t *testing.T) {
	expect := Options{
		Dir:             filepath.FromSlash("relative/path"),
		KeepCorrupt:     true,
		UpgradeIfNeeded: true,
	}
	actual, err := ParseConnectionString("file://relative/path?debug=true&upgradeIfNeeded=true&keepCorrupt=true")
	if err != nil {
		t.Fatal(err)
	}
	if actual.Debug == nil {
		t.Error("debug logger not set")
	}
	if actual.Info == nil {
		t.Error("info logger not set")
	}
	actual.Debug = nil
	actual.Info = nil
	if diff := cmp.Diff(expect, actual); diff != "" {
		t.Error(diff)
	}
}

func TestParseConnectionStringInvalidScheme(t *testing.T) {
	_, err := ParseConnectionString("https:///absolute/path?debug=true&upgradeIfNeeded=true&keepCorrupt=true")
	if err == nil {
		t.Error("parse succeeded with incorrect scheme")
	}
}
