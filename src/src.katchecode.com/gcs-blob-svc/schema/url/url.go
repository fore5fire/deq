package url

import (
	"fmt"
	"net/url"
	"strconv"
)

type GraphQLURL struct {
	url.URL
}

func New(u *url.URL) *GraphQLURL {
	if u == nil {
		return nil
	}
	return &GraphQLURL{*u}
}

func (GraphQLURL) ImplementsGraphQLType(name string) bool {
	return name == "Url"
}

func (u *GraphQLURL) UnmarshalGraphQL(input interface{}) error {
	switch input := input.(type) {
	case string:
		var (
			err error
			res *url.URL
		)
		res, err = url.Parse(input)
		u.URL = *res
		return err
	default:
		return fmt.Errorf("wrong type for GraphQLURL")
	}
}

func (u GraphQLURL) MarshalJSON() ([]byte, error) {
	return strconv.AppendQuote(nil, u.String()), nil
}

func Parse(rawurl string) (*GraphQLURL, error) {
	parsed, err := url.Parse(rawurl)
	u := &GraphQLURL{}
	u.URL = *parsed
	return u, err
}
