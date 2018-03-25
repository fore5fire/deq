package main_test

import (
  "os"
  "fmt"
  "context"
  "testing"
  "github.com/shurcooL/graphql"
)

var client = graphql.NewClient(os.Getenv("TEST_TARGET_ENDPOINT"), nil)

type HelloWorld struct {
  Hello graphql.String
  // Hello2 graphql.String `graphql:"hello(to: "me")"`
}

func Hello() (HelloWorld, error) {

  request := HelloWorld {}

  return request, client.Query(context.Background(), &request, nil)
}

func TestQuery(t *testing.T) {

  response, err := Hello()

  if err != nil {
    fmt.Println(err)
    t.FailNow()
  }
  if response.Hello != "world!" {
    fmt.Println(response)
    t.FailNow()
  }
  fmt.Println(response.Hello)
}
