package server

import (
	"errors"
	"net/http"
	"sync"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/store"
)

// InstallArtifacts associates already initialized Files with a stopped Store.
// The caller owns Files and its root, must stop all servers and prior writers,
// and must retain them through this call. Files.Initialize is a separate explicit
// empty-root operation; neither this function nor Run adopts or initializes files.
// Physical fact collection completes before the Store installation transaction.
func InstallArtifacts(directory string, files *artifact.Files) error {
	if directory == "" || files == nil {
		return errors.New("artifact directory and initialized files required")
	}
	facts, err := files.RootFacts()
	if err != nil {
		return err
	}
	_, err = store.InstallArtifacts(directory, store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume})
	return err
}

// artifactHTTPDrain counts original synchronous HTTP owners. Shutdown deadlines
// and srv.Close cannot manufacture their completion. Wait has no background
// waiter and never drops a handler whose actual I/O or write has not returned.
// This HTTP join does not settle Control's durable downloads or uncertain FDs.
type artifactHTTPDrain struct {
	mu      sync.Mutex
	handler http.Handler
	stopped bool
	active  int
	done    chan struct{}
}

func newArtifactHTTPDrain(handler http.Handler) *artifactHTTPDrain {
	return &artifactHTTPDrain{handler: handler, done: make(chan struct{})}
}

func (d *artifactHTTPDrain) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	d.active++
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.active--
		if d.stopped && d.active == 0 {
			close(d.done)
		}
	}()
	d.handler.ServeHTTP(w, r)
}

func (d *artifactHTTPDrain) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		return
	}
	d.stopped = true
	if d.active == 0 {
		close(d.done)
	}
}

func (d *artifactHTTPDrain) Wait() {
	<-d.done
}
