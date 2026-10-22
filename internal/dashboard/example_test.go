package dashboard_test

import (
	"fmt"

	"github.com/tmc/skygo/internal/dashboard"
)

func ExampleListen() {
	for _, addr := range []string{"0.0.0.0:8080", "localhost:8080", "127.0.0.1:0"} {
		ln, err := dashboard.Listen(addr)
		if err != nil {
			fmt.Printf("%s: %v\n", addr, err)
			continue
		}
		fmt.Printf("%s: listening\n", addr)
		ln.Close()
	}
	// Output:
	// 0.0.0.0:8080: dashboard listens only on a loopback address
	// localhost:8080: dashboard listens only on a loopback address
	// 127.0.0.1:0: listening
}
