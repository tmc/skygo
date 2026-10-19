package control

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
	bolt "go.etcd.io/bbolt"
)

func auditControl(t *testing.T) (*Control, *store.Store, string, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	job, err := s.Submit(testActor(), "audit-job", backend.Task{Argv: []string{"work"}}, "local")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Update(job.ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	c, err := New(s, []backend.Target{{Name: "local", Backend: &configuredBackend{}}}, testEngine)
	if err != nil {
		t.Fatal(err)
	}
	return c, s, dir, job.ID
}

func TestAuditEnqueueDoesNotWaitForWriter(t *testing.T) {
	c, s, _, id := auditControl(t)
	entered, release := make(chan struct{}), make(chan struct{})
	writerDone := make(chan error, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	go func() {
		_, err := s.Update(id, func(*backend.Job) error { close(entered); <-release; return nil })
		writerDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("writer barrier not reached")
	}
	returned := make(chan error, 1)
	go func() { returned <- c.RecordAccess(testActor(), "logs", id, "ended") }()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("audit enqueue waited for database writer")
	}
	select {
	case <-writerDone:
		t.Fatal("writer released before enqueue proof")
	default:
	}
	if len(c.access) != 1 {
		t.Fatal("event not queued")
	}
	unblock()
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
}

func TestAuditQueueOverflowRemainsVisible(t *testing.T) {
	c, _, _, _ := auditControl(t)
	for i := 0; i < maxPendingAccess; i++ {
		if err := c.RecordAccess(testActor(), "list", "", "allowed"); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.RecordAccess(testActor(), "list", "", "allowed"); !errors.Is(err, errAuditQueueFull) {
		t.Fatalf("overflow: %v", err)
	}
	if len(c.access) != maxPendingAccess || c.AuditHealthy() {
		t.Fatal("overflow not bounded and visible")
	}
	c.drainAccess(context.Background())
	if len(c.access) != maxPendingAccess-accessDrainQuota || c.AuditHealthy() {
		t.Fatal("drain erased lost-event warning")
	}
	if _, _, err := c.Plan(testActor(), backend.Task{Argv: []string{"work"}}); err != nil || !c.Healthy() {
		t.Fatalf("audit overflow affected admission/health: %v", err)
	}
}

func TestAuditDrainQuotaAndPersistenceTime(t *testing.T) {
	c, s, dir, _ := auditControl(t)
	for i := 0; i < accessDrainQuota+2; i++ {
		if err := c.RecordAccess(testActor(), "list", "", "allowed"); err != nil {
			t.Fatal(err)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	c.drainAccess(canceled)
	if len(c.access) != accessDrainQuota+2 {
		t.Fatal("canceled drain consumed pending diagnostic")
	}
	before := time.Now()
	c.drainAccess(context.Background())
	if len(c.access) != 2 {
		t.Fatal("drain exceeded quota")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("access_audit"))
		if b.Stats().KeyN != accessDrainQuota {
			t.Error("drained metadata not persisted")
		}
		return b.ForEach(func(_, raw []byte) error {
			var event struct {
				Time time.Time `json:"time"`
			}
			if err := json.Unmarshal(raw, &event); err != nil {
				return err
			}
			if event.Time.Before(before) {
				t.Error("timestamp did not describe persistence attempt")
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
}

func TestAuditPersistenceFailureRecoversWithoutAdmissionChange(t *testing.T) {
	c, s, dir, _ := auditControl(t)
	if err := c.RecordAccess(testActor(), "list", "", "allowed"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	c.drainAccess(context.Background())
	if c.AuditHealthy() || !c.Healthy() {
		t.Fatal("persistence fault changed wrong health")
	}
	if _, _, err := c.Plan(testActor(), backend.Task{Argv: []string{"work"}}); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	c.store = reopened
	if err := c.RecordAccess(testActor(), "list", "", "allowed"); err != nil {
		t.Fatal(err)
	}
	if c.AuditHealthy() {
		t.Fatal("enqueue erased persistence failure")
	}
	c.drainAccess(context.Background())
	if !c.AuditHealthy() {
		t.Fatal("successful persistence did not recover warning")
	}
}
