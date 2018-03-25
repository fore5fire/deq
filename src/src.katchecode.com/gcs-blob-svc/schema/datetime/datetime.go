package datetime

import (
	"fmt"
	"strconv"
	"time"
)

type GraphQLDateTime struct {
	time.Time
}

func New(t *time.Time) *GraphQLDateTime {
	if t == nil {
		return nil
	}
	return &GraphQLDateTime{*t}
}

func (GraphQLDateTime) ImplementsGraphQLType(name string) bool {
	return name == "DateTime"
}

func (t *GraphQLDateTime) UnmarshalGraphQL(input interface{}) error {
	switch input := input.(type) {
	case string:
		var err error
		t.Time, err = time.Parse(input, time.RFC3339)
		return err
	case time.Time:
		t.Time = input
		return nil
	default:
		return fmt.Errorf("wrong type for GraphQLDateTime")
	}
}

func (t GraphQLDateTime) MarshalJSON() ([]byte, error) {
	return strconv.AppendQuote(nil, t.Format(time.RFC3339)), nil
}
