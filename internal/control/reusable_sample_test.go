package control

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/store"
	bolt "go.etcd.io/bbolt"
)

func TestSampleClosedStoreCannotPublishReusableZero(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	closeStore := sync.OnceValue(s.Close)
	t.Cleanup(func() {
		if err := closeStore(); err != nil {
			t.Error(err)
		}
	})
	c := &Control{store: s}
	previous := &RetentionAdmin{ReusableDatabaseBytes: 31, ObservedAt: time.Now()}
	c.retention.sample = previous
	c.retention.wantSample.Store(true)
	m := &maintenanceState{input: make(chan maintenanceRequest, 1)}
	if err := closeStore(); err != nil {
		t.Fatal(err)
	}
	if err := c.dispatchSample(m); !errors.Is(err, bolt.ErrDatabaseNotOpen) {
		t.Fatal("closed sample did not propagate original error", err)
	}
	if m.pending != nil || m.serial != 0 || len(m.input) != 0 {
		t.Fatal("failed sample dispatched a successful zero observation")
	}
	if !c.retention.wantSample.Load() || c.retention.sample != previous {
		t.Fatal("failed sample replaced prior state or cleared requested refresh")
	}
}
