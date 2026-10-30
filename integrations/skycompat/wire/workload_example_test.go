package wire_test

import (
	"fmt"
	"strings"

	"github.com/tmc/skygo/integrations/skycompat/wire"
)

func ExampleLaunchBody() {
	body := wire.LaunchBody{DAG: "run: echo hello", ClusterName: "example"}
	fmt.Println(body.ClusterName, body.DAG)
	// Output: example run: echo hello
}

func ExampleExecBody() {
	body := wire.ExecBody{DAG: "run: echo again", ClusterName: "example"}
	fmt.Println(body.ClusterName, body.DAG)
	// Output: example run: echo again
}

func ExampleDecodeLaunch() {
	body, err := wire.DecodeLaunch(strings.NewReader(`{"task":"run: echo hello","cluster_name":"example","optimize_target":0}`))
	fmt.Println(body.ClusterName, body.DAG, err)
	// Output: example run: echo hello <nil>
}

func ExampleDecodeExec() {
	body, err := wire.DecodeExec(strings.NewReader(`{"task":"run: echo again","cluster_name":"example"}`))
	fmt.Println(body.ClusterName, body.DAG, err)
	// Output: example run: echo again <nil>
}
