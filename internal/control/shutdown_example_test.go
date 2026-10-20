package control

import (
	"context"
	"errors"
	"fmt"
)

func ExampleControl_Join() {
	var c Control
	err := c.Join(context.Background())
	fmt.Println(errors.Is(err, ErrUnavailable))
	// Output: true
}
