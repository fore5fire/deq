package auth

import (
	"context"
	"fmt"
)

func ExampleValidator() {
	ctx := context.Background()
	v := Validator{}

	a, err := v.UnmarshalAndValidate(ctx, "")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(a)
	// OUTPUT: abc
}
