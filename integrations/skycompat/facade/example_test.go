package facade_test

import (
	"fmt"
	"github.com/tmc/skygo/integrations/skycompat/facade"
)

func ExampleNewHandler() {
	_, err := facade.NewHandler(facade.Options{Endpoint: "http://127.0.0.1:46580"})
	fmt.Println(err)
	// Output: <nil>
}
