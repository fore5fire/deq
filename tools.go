// +build tools

package deq

// This file's only job is to ensure that a specific version of the tools required for code
// generation are included in the go.mod. These are then invoked using the go run command by the
// code generators.
import (
	_ "github.com/go-bindata/go-bindata/go-bindata"
	_ "github.com/gogo/protobuf/protoc-gen-gogofaster"
)
