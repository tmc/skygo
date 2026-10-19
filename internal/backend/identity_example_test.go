package backend_test

import (
	"fmt"
	"github.com/tmc/skygo/internal/backend"
)

func ExampleTaskBytes() {
	data, err := backend.TaskBytes(backend.Task{Argv: []string{"trainer"}})
	fmt.Println(string(data), err)
	// Output: {"argv":["trainer"]} <nil>
}

func ExampleLocalBinding() {
	binding, err := backend.LocalBinding("0123456789abcdef0123456789abcdef")
	fmt.Println(binding, err)
	// Output: sha256:144481267d9841772b470124aeae800db7143265da3bb29f98457197c0bce382 <nil>
}
