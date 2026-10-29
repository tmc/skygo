package serving

import (
	"net/http"
	"testing"
)

// AUTHORED_NOT_RUN.
func TestForwardHeader(t *testing.T) {
	in := http.Header{}
	for _, k := range []string{
		"Authorization", "Proxy-Authorization", "Cookie",
		"Impersonate-User", "Impersonate-Uid", "Impersonate-Group", "Impersonate-Extra-Scopes",
		"X-Skygo-Session", "X-Skygo-Login", "Skygo-Audit",
		"Accept", "Content-Type", "X-Request-Id",
	} {
		in.Set(k, "v")
	}
	// A non-canonical key, as a handler may build one, is still removed.
	in["authorization"] = []string{"lower"}
	out := ForwardHeader(in)
	want := map[string]bool{"Accept": true, "Content-Type": true, "X-Request-Id": true}
	if len(out) != len(want) {
		t.Fatalf("kept %v", out)
	}
	for k := range out {
		if !want[k] {
			t.Errorf("kept %s", k)
		}
	}
	if in.Get("Authorization") == "" {
		t.Fatal("ForwardHeader changed its argument")
	}
}
