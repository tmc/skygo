package rpc_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

func Example() {
	dir, err := os.MkdirTemp("", "skygo-rpc-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	s, err := store.Open(dir)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	b, err := local.New(filepath.Join(dir, "logs"))
	if err != nil {
		panic(err)
	}
	defer b.Close()
	engine := auth.NewLegacy("")
	c, err := control.New(s, []backend.Target{{Name: "local", Kind: "local", Binding: "test-local", Backend: b}}, engine)
	if err != nil {
		panic(err)
	}
	server := httptest.NewServer(rpc.NewHandler(c, engine))
	defer server.Close()
	client := skyv1connect.NewControlServiceClient(server.Client(), server.URL)
	response, err := client.ListTargets(context.Background(), connect.NewRequest(&v1.ListTargetsRequest{}))
	if err != nil {
		panic(err)
	}
	fmt.Println(response.Msg.Targets[0].Name)
	// Output: local
}
