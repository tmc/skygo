package store

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

func ExampleStore_MaintainRetention() {
	dir, err := os.MkdirTemp("", "skygo-epoch-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	info, err := EnableRetention(dir, DefaultRetentionPolicy())
	if err != nil {
		panic(err)
	}
	s, err := Open(dir)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	// This same-package example advances the store clock; no wall-clock sleep
	// or server process is needed to demonstrate request-epoch retirement.
	s.now = func() time.Time { return info.RetryUntil }
	if err = s.MaintainRetention(info.RetryUntil); err != nil {
		panic(err)
	}
	_, _, err = s.LookupRetained(testActor(), info.Domain, "old-domain", backend.Task{Argv: []string{"trainer"}})
	var expired *ExpiryError
	if !errors.As(err, &expired) {
		panic(err)
	}
	current, err := s.RetentionInfo(testActor())
	fmt.Println(expired.Reason == RequestEpochExpired, current.Domain.Epoch > info.Domain.Epoch, err)
	// Output: true true <nil>
}
