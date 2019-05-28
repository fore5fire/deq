package main

import (
	"fmt"
	"strings"
)

type Params struct {
	ImportOverrides    map[string]string
	DEQImportOverrides map[string]string
}

func ParseParams(params string) (*Params, error) {
	parsed := &Params{
		ImportOverrides: map[string]string{
			"google/protobuf/api.proto":        "github.com/gogo/protobuf/types",
			"google/protobuf/any.proto":        "github.com/gogo/protobuf/types",
			"google/protobuf/descriptor.proto": "protobuf/protoc-gen-gogo/descriptor",
			"google/protobuf/duration.proto":   "github.com/gogo/protobuf/types",
			"google/protobuf/empty.proto":      "github.com/gogo/protobuf/types",
			"google/protobuf/field_mask.proto": "github.com/gogo/protobuf/types",
			"google/protobuf/timestamp.proto":  "github.com/gogo/protobuf/types",
			"google/protobuf/wrappers.proto":   "github.com/gogo/protobuf/types",
		},
		DEQImportOverrides: map[string]string{
			"google/protobuf/field_mask.proto": "gitlab.com/katcheCode/deq/deqtype",
			"google/protobuf/empty.proto":      "gitlab.com/katcheCode/deq/deqtype",
			"google/protobuf/timestamp.proto":  "gitlab.com/katcheCode/deq/deqtype",
			"google/protobuf/any.proto":        "gitlab.com/katcheCode/deq/deqtype",
			"google/protobuf/api.proto":        "gitlab.com/katcheCode/deq/deqtype",
			"google/protobuf/descriptor.proto": "gitlab.com/katcheCode/deq/deqtype",
			"google/protobuf/duration.proto":   "gitlab.com/katcheCode/deq/deqtype",
			"google/protobuf/wrappers.proto":   "gitlab.com/katcheCode/deq/deqtype",
		},
	}

	split := strings.Split(params, ",")
	for i, param := range split {
		if len(param) == 0 {
			continue
		}
		switch param[0] {
		case 'M':
			parts := strings.SplitN(param[1:], "=", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("parameter %d is invalid", i)
			}
			parsed.ImportOverrides[parts[0]] = parts[1]
		case 'D':
			parts := strings.SplitN(param[1:], "=", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("parameter %d is invalid", i)
			}
			parsed.DEQImportOverrides[parts[0]] = parts[1]
		default:
			return nil, fmt.Errorf("unrecognized argument %s", param)
		}
	}

	return parsed, nil
}
