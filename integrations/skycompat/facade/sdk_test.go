package facade

import (
	"bytes"
	"context"
	"fmt"
	"github.com/tmc/skygo/integrations/skycompat/wire"
	"github.com/tmc/skygo/skycompat/taskyaml"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type traceBody struct {
	io.ReadCloser
	buffer bytes.Buffer
}

func (b *traceBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.buffer.Write(p[:n])
	return n, err
}

type traceWriter struct {
	http.ResponseWriter
	status int
}

func (w *traceWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *traceWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func TestPinnedSDK(t *testing.T) {
	python, oracle := os.Getenv("SKYCOMPAT_SDK_PYTHON"), os.Getenv("SKYCOMPAT_SDK_ORACLE")
	reference := os.Getenv("SKYCOMPAT_SDK_SOURCE")
	if python == "" || oracle == "" || reference == "" {
		t.Skip("set SKYCOMPAT_SDK_PYTHON, SKYCOMPAT_SDK_ORACLE and SKYCOMPAT_SDK_SOURCE for unmodified SDK qualification")
	}
	f := newFixture(t)
	var mu sync.Mutex
	posts, secretPosts := 0, 0
	f.facade.Close()
	f.facade = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := &traceBody{ReadCloser: r.Body}
		r.Body = body
		response := &traceWriter{ResponseWriter: w}
		f.h.ServeHTTP(response, r)
		secretBody := bytes.Contains(body.buffer.Bytes(), []byte(f.alice))
		t.Logf("SDK HTTP %s %s status=%d api=%q version=%q body_bytes=%d body_credential=%t", r.Method, r.URL.Path, response.status, w.Header().Get(wire.APIVersionHeader), w.Header().Get(wire.VersionHeader), body.buffer.Len(), secretBody)
		if r.Method == "POST" {
			decoded, decodeErr := wire.DecodeValidate(bytes.NewReader(body.buffer.Bytes()))
			if decodeErr != nil {
				t.Logf("SDK request wire diagnostic: %v", decodeErr)
			} else {
				doc, parseErr := taskyaml.ParseDAG(strings.NewReader(decoded.DAG), taskyaml.Options{})
				if parseErr != nil {
					t.Logf("SDK task parse diagnostic: %v", parseErr)
				} else if !supported(doc) {
					t.Log("SDK task outside facade subset")
				}
			}
			mu.Lock()
			posts++
			if bytes.Contains(body.buffer.Bytes(), []byte(f.alice)) {
				secretPosts++
			}
			mu.Unlock()
		}
	}))
	t.Cleanup(f.facade.Close)
	config := filepath.Join(f.dir, "sdk-config.yaml")
	if err := os.WriteFile(config, []byte(fmt.Sprintf("api_server:\n  endpoint: %s\n  service_account_token: %s\n", f.facade.URL, f.alice)), 0600); err != nil {
		t.Fatal(err)
	}
	env := []string{}
	for _, item := range os.Environ() {
		key := strings.SplitN(item, "=", 2)[0]
		if !strings.HasPrefix(key, "SKYPILOT_") && !strings.HasPrefix(key, "AWS_") && !strings.HasPrefix(key, "AZURE_") && !strings.HasPrefix(key, "GOOGLE_") && !strings.HasPrefix(key, "OPENAI_") && !strings.HasPrefix(key, "ANTHROPIC_") && key != "PYTHONPATH" && key != "KUBECONFIG" && key != "VIRTUAL_ENV" {
			env = append(env, item)
		}
	}
	env = append(env, "PYTHONPATH="+reference, "PYTHONDONTWRITEBYTECODE=1", "SKYPILOT_CONFIG="+config, "SKYPILOT_DISABLE_USAGE_COLLECTION=1", "SKY_RUNTIME_DIR="+filepath.Join(f.dir, "runtime"))
	for _, mode := range []string{"config", "env-negative"} {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		cmd := exec.CommandContext(ctx, python, oracle)
		cmd.Env = append(append([]string{}, env...), "E0_ORACLE_MODE="+mode)
		if mode == "env-negative" {
			cmd.Env = append(cmd.Env, "SKYPILOT_SERVICE_ACCOUNT_TOKEN="+f.alice)
		}
		output, err := cmd.CombinedOutput()
		cancel()
		if bytes.Contains(output, []byte(f.alice)) {
			t.Fatal("SDK oracle exposed credential")
		}
		if err != nil {
			t.Fatalf("unmodified SDK %s: %v\n%s", mode, err, output)
		}
		t.Logf("unmodified SDK %s: %s", mode, output)
		if mode == "config" {
			mu.Lock()
			n, leaks := posts, secretPosts
			mu.Unlock()
			if n != 2 || leaks != 0 {
				t.Fatalf("config SDK posts %d credential bodies %d", n, leaks)
			}
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if posts != 3 || secretPosts != 1 {
		t.Fatalf("SDK body profile: posts=%d secret-negative=%d", posts, secretPosts)
	}
}
