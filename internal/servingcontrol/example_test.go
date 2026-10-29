package servingcontrol_test

import (
	"fmt"

	"github.com/tmc/skygo/internal/servingcontrol"
)

func ExampleNew() {
	_, err := servingcontrol.New(servingcontrol.Config{})
	fmt.Println(err)
	// Output:
	// service controller requires a store, an auth engine and a runtime
}
