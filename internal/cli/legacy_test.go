package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"rsc.io/script"
	"rsc.io/script/scripttest"
)

func TestDiscoveryWorkload(t *testing.T) {
	marker := os.Getenv("SKYGO_DISCOVERY_MARKER")
	if marker == "" {
		return
	}
	if err := os.WriteFile(marker, []byte("executed\n"), 0600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("legacy-compatible workload")
	os.Exit(0)
}

// TestLegacyCompatibility requires an external git-archive fixture so the old
// generated client and daemon cannot silently be rebuilt from the new schema.
// Ordinary test runs explicitly skip this opt-in historical-binary gate.
func TestLegacyCompatibility(t *testing.T) {
	dir := os.Getenv("SKYGO_LEGACY_FIXTURE")
	if dir == "" {
		t.Skip("set SKYGO_LEGACY_FIXTURE to the pinned 9135253 legacy fixture")
	}
	data, err := os.ReadFile(filepath.Join(dir, "fixture.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Commit string `json:"source_commit"`
		SHA256 string `json:"binary_sha256"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Commit != "9135253d0edd6a49cdc7e409b7fc1ebfbd7e1845" {
		t.Fatalf("unexpected legacy source: %q", fixture.Commit)
	}
	legacy, err := filepath.Abs(filepath.Join(dir, "skygo-legacy"))
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(legacy)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(binary)
	if got := hex.EncodeToString(sum[:]); got != fixture.SHA256 {
		t.Fatalf("legacy binary SHA256 = %s; want %s", got, fixture.SHA256)
	}
	t.Logf("legacy source %s; binary SHA256 %s", fixture.Commit, fixture.SHA256)
	exe := filepath.Join(t.TempDir(), "skygo")
	build := exec.Command("go", "build", "-o", exe, "../../cmd/skygo")
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOWORK=off")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build current CLI: %v\n%s", err, output)
	}
	for _, tc := range []struct{ name, server string }{{"old_server", legacy}, {"old_client", exe}} {
		t.Run(tc.name, func(t *testing.T) {
			runLegacyScript(t, exe, legacy, tc.server, "testdata/legacy/"+tc.name+".txt")
		})
	}
}

func runLegacyScript(t *testing.T, current, legacy, daemon, file string) {
	t.Helper()
	address := freeAddress(t)
	process := &serverProcess{exe: daemon, address: address, data: t.TempDir()}
	t.Cleanup(process.cleanup)
	target := &url.URL{Scheme: "http", Host: address}
	proxy := httputil.NewSingleHostReverseProxy(target)
	var mu sync.Mutex
	calls := make(map[string]int)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls[r.URL.Path]++
		mu.Unlock()
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	e := executableScriptEngine(current, nil, nil)
	e.Cmds["legacy"] = executableScriptEngine(legacy, nil, nil).Cmds["skygo"]
	e.Cmds["server"] = process.command()
	e.Cmds["job-id"] = jobID()
	e.Cmds["calls"] = script.Command(script.CmdUsage{Summary: "check calls forwarded to the actual daemon", Args: "path count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		want, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, err
		}
		mu.Lock()
		got := calls[args[0]]
		mu.Unlock()
		if got != want {
			return nil, fmt.Errorf("%s calls = %d; want %d", args[0], got, want)
		}
		return nil, nil
	})
	cover := coverDir(t)
	e.Cmds["taskfile"] = script.Command(script.CmdUsage{Summary: "write a task whose execution creates a marker", Args: "file marker"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		exe, err := os.Executable()
		if err != nil {
			return nil, err
		}
		env := map[string]string{"SKYGO_DISCOVERY_MARKER": s.Path(args[1])}
		if cover != "" {
			env["GOCOVERDIR"] = cover
		}
		task := map[string]any{
			"argv": []string{exe, "-test.run=^TestDiscoveryWorkload$"},
			"env":  env,
		}
		data, err := json.Marshal(task)
		if err != nil {
			return nil, err
		}
		return nil, os.WriteFile(s.Path(args[0]), data, 0600)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	scripttest.Test(t, ctx, e, []string{"SKYGO_ENDPOINT=" + srv.URL, "SKYGO_TOKEN=coverage-token"}, file)
}
