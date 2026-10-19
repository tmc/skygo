package wire_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tmc/skygo/integrations/skycompat/wire"
)

func Example() {
	body, err := wire.DecodeValidate(strings.NewReader(`{"dag":"run: echo hello"}`))
	fmt.Println(body.DAG, err)
	// Output: run: echo hello <nil>
}
func ExampleDecodeValidate() {
	body, err := wire.DecodeValidate(strings.NewReader(`{"dag":"run: echo hello","request_options":null}`))
	fmt.Println(body.DAG, err)
	// Output: run: echo hello <nil>
}
func ExampleDecodeError() {
	_, err := wire.DecodeValidate(strings.NewReader(`{"dag":"run: hello","unknown":true}`))
	fmt.Println(err)
	// Output: wire: unsupported request body
}
func ExampleEncodeRequest() {
	var out bytes.Buffer
	err := wire.EncodeRequest(&out, wire.Request{ID: "request-1", Name: "sky.example", UserID: "user-1", Status: wire.Succeeded, Schedule: wire.Short, Result: json.RawMessage(`42`)})
	var body struct {
		ReturnValue string `json:"return_value"`
	}
	json.Unmarshal(out.Bytes(), &body)
	fmt.Println(body.ReturnValue, err)
	// Output: 42 <nil>
}
func ExampleEncodeRequestError() {
	var out bytes.Buffer
	err := wire.EncodeRequestError(&out, wire.Request{ID: "request-1", Name: "sky.example", UserID: "user-1", Status: wire.Failed, Schedule: wire.Short}, wire.Exception{Type: wire.RuntimeError, Message: "operation unavailable"})
	var body struct{ Detail struct{ Status string } }
	json.Unmarshal(out.Bytes(), &body)
	fmt.Println(body.Detail.Status, err)
	// Output: FAILED <nil>
}
func ExampleEncodeValidationError() {
	var out bytes.Buffer
	err := wire.EncodeValidationError(&out, wire.Exception{Type: wire.ValueError, Message: "unsupported task field"})
	fmt.Println(out.String(), err)
	// Output: {"detail":{"type":"ValueError","message":"unsupported task field","args":["unsupported task field"],"attributes":{},"stacktrace":null}} <nil>
}
