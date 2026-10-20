package server

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// The blocked handler is simulated transport ownership, not a physical I/O or
// scheduler deadline claim. Stop cannot manufacture the original HTTP return.
func TestArtifactHTTPDrainJoinsOriginalHandler(t *testing.T) {
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	drain := newArtifactHTTPDrain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	go func() {
		defer close(returned)
		drain.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/", nil))
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("original handler not entered")
	}
	drain.Stop()
	drain.Stop()
	rejected := httptest.NewRecorder()
	drain.ServeHTTP(rejected, httptest.NewRequest("POST", "/", nil))
	if rejected.Code != http.StatusServiceUnavailable {
		t.Fatalf("stopped admission=%d", rejected.Code)
	}
	select {
	case <-drain.done:
		t.Fatal("original handler declared joined while blocked")
	default:
	}
	unblock()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("original handler not returned")
	}
	drain.Wait()
}

func TestArtifactHTTPDrainEmpty(t *testing.T) {
	drain := newArtifactHTTPDrain(http.NotFoundHandler())
	drain.Stop()
	drain.Stop()
	drain.Wait()
}
