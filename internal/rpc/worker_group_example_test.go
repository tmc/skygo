package rpc_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/rpc"
)

func ExampleNewWorkerHandler() {
	// Nil workers leaves the dedicated route unmounted. Authentication still
	// runs before routing; a configured caller supplies its Control and Engine.
	h := rpc.NewWorkerHandler(nil, auth.NewLegacy("example-secret"), nil)
	request := httptest.NewRequest(http.MethodPost, "/skygo.v1.WorkerGroupService/GetWorkerRanks", nil)
	request.Header.Set("Authorization", "Bearer example-secret")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	fmt.Println(response.Code)
	// Output: 404
}
