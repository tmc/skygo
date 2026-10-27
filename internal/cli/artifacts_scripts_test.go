package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestArtifactWorkload is an owned real program, not an API/backend emulator.
// Setup creates a file in its actual cwd; run requires it and the uploaded seed.
// A second native run refuses its exclusive external marker, exposing replay.
func TestArtifactWorkload(t *testing.T) {
	mode := os.Getenv("SKYGO_ARTIFACT_WORKLOAD")
	if mode == "" {
		return
	}
	fail := func(err error) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(99)
	}
	if mode == "baseline" {
		if err := os.WriteFile(os.Getenv("SKYGO_ARTIFACT_MARKER"), []byte("baseline"), 0600); err != nil {
			fail(err)
		}
		fmt.Println("old default")
		os.Exit(0)
	}
	if len(os.Args) > 0 && os.Args[len(os.Args)-1] == "setup" {
		if err := os.WriteFile("prepared", []byte("prepared\n"), 0600); err != nil {
			fail(err)
		}
		os.Exit(0)
	}
	seed, err := os.ReadFile("seed.txt")
	if err != nil {
		fail(err)
	}
	prepared, err := os.ReadFile("prepared")
	if err != nil {
		fail(err)
	}
	marker, err := os.OpenFile(os.Getenv("SKYGO_ARTIFACT_MARKER"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		fail(err)
	}
	if err := marker.Close(); err != nil {
		fail(err)
	}
	if err := os.Mkdir("out", 0700); err != nil {
		fail(err)
	}
	if err := os.WriteFile("out/result.txt", append(seed, prepared...), 0600); err != nil {
		fail(err)
	}
	fmt.Println("workspace complete")
	os.Exit(0)
}

// All script peers below are actual production daemon processes. The proxy
// changes transport responses only, after a real handler completed. It cannot
// create grants, forge rows, publish archives or implement workspace behavior.
func TestArtifactScripts(t *testing.T) {
	exe := artifactScriptFixture(t, "SKYGO_ARTIFACT_BINARY")
	for _, name := range []string{"validation", "roundtrip", "empty_many", "resume", "download_refusal", "workspace"} {
		t.Run(name, func(t *testing.T) { runArtifactScript(t, exe, exe, "testdata/artifacts/"+name+".txt", true) })
	}
}

func TestArtifactOldServerScripts(t *testing.T) {
	exe := artifactScriptFixture(t, "SKYGO_ARTIFACT_BINARY")
	old := artifactScriptFixture(t, "SKYGO_S05_BINARY")
	runArtifactScript(t, exe, old, "testdata/artifacts/old_server.txt", false)
}

func artifactScriptFixture(t *testing.T, name string) string {
	t.Helper()
	path, want := os.Getenv(name), os.Getenv(name+"_SHA256")
	if path == "" && want == "" && os.Getenv("SKYGO_REQUIRE_FIXTURES") == "" {
		t.Skipf("requires %s and %s_SHA256 (set SKYGO_REQUIRE_FIXTURES=1 to fail instead)", name, name)
	}
	if path == "" || want == "" {
		t.Fatalf("mandatory source-qualified fixture requires %s and %s_SHA256", name, name)
	}
	if !filepath.IsAbs(path) {
		t.Fatalf("%s must be an absolute path", name)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		t.Fatalf("%s is not an executable regular file", name)
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != want {
		t.Fatalf("%s SHA256=%s want=%s", name, got, want)
	}
	t.Logf("%s exact executable SHA256 %s; source provenance is a separate parent receipt", name, want)
	return path
}

func runArtifactScript(t *testing.T, exe, daemon, file string, enableArtifacts bool) {
	t.Helper()
	var env []string
	// Do not infer coverage from the parent test binary for an externally
	// provided executable. Only its explicit, separately qualified mode opts in.
	if mode := os.Getenv("SKYGO_ARTIFACT_BINARY_COVERMODE"); mode != "" {
		if mode != testing.CoverMode() || (mode != "set" && mode != "count" && mode != "atomic") {
			t.Fatal("artifact fixture coverage mode does not match the instrumented test binary")
		}
		if dir := coverDir(t); dir != "" {
			env = append(env, "GOCOVERDIR="+dir)
		}
	}
	var daemonEnv []string
	if daemon == exe {
		daemonEnv = env
	}
	process := &serverProcess{exe: daemon, address: freeAddress(t), data: t.TempDir(), env: daemonEnv}
	if err := os.Chmod(process.data, 0700); err != nil {
		t.Fatal(err)
	}
	if enableArtifacts {
		// The current daemon opens only an installed root. Initialize once while
		// stopped, then preserve the same state/root across every script restart.
		root := filepath.Join(process.data, "artifacts")
		if err := os.Mkdir(root, 0700); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		initialize := exec.CommandContext(ctx, exe, "state", "artifacts", "init", "--data-dir", process.data, "--artifact-dir", root, "--ack-writers-stopped")
		initialize.Env = append(os.Environ(), env...)
		out, err := initialize.CombinedOutput()
		if err != nil {
			t.Fatalf("initialize artifact script fixture: %v\n%s", err, out)
		}
		if string(out) != "enabled: true\npolicy: default-v1\nrestart_required: true\n" {
			t.Fatalf("unexpected artifact initialization readback: %q", out)
		}
		// Explicit args reuse the existing opt-in serverProcess API. Its nil
		// mode remains unchanged for coverage, workflow and predecessor scripts.
		process.args = []string{"--token", "coverage-token", "--artifact-dir", root}
		process.env = append(append([]string(nil), daemonEnv...), "SKYGO_TOKEN=")
	}
	t.Cleanup(process.cleanup)
	target := &url.URL{Scheme: "http", Host: process.address}
	proxy := httputil.NewSingleHostReverseProxy(target)
	direct := proxy.Director
	proxy.Director = func(r *http.Request) { direct(r); r.Header.Del("Accept-Encoding") }
	var mu sync.Mutex
	calls := make(map[string]int)
	fault := ""
	proxy.ModifyResponse = func(r *http.Response) error {
		method := r.Request.URL.Path[strings.LastIndex(r.Request.URL.Path, "/")+1:]
		mu.Lock()
		if r.StatusCode == http.StatusOK {
			calls["ok:"+method]++
		}
		active := fault
		matches := (active == "write-lost" && method == "WriteArtifactChunk") || (active == "finalize-lost" && method == "FinalizeArtifactUpload") || ((active == "download-offset" || active == "download-bytes") && method == "ReadArtifactChunk")
		if matches && r.StatusCode == http.StatusOK {
			fault = ""
		}
		mu.Unlock()
		if !matches || r.StatusCode != http.StatusOK {
			return nil
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 128<<10))
		if err != nil {
			return err
		}
		if err := r.Body.Close(); err != nil {
			return err
		}
		if active == "write-lost" || active == "finalize-lost" {
			r.StatusCode = http.StatusServiceUnavailable
			r.Status = "503 Service Unavailable"
			r.Header.Set("Content-Type", "application/json")
			body = []byte(`{"code":"unavailable","message":"test discarded a completed handler response"}`)
		} else {
			var chunk v1.ReadArtifactChunkResponse
			if err := proto.Unmarshal(body, &chunk); err != nil {
				return err
			}
			if active == "download-offset" {
				chunk.NextOffset--
			} else {
				if len(chunk.Data) == 0 {
					return errors.New("no actual chunk to corrupt")
				}
				chunk.Data[len(chunk.Data)-1] ^= 1
			}
			body, err = proto.Marshal(&chunk)
			if err != nil {
				return err
			}
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		r.Header.Set("Content-Length", strconv.Itoa(len(body)))
		return nil
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		mu.Lock()
		calls[method]++
		mu.Unlock()
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	e := executableScriptEngine(exe, nil, env)
	e.Cmds["server"] = process.command()
	e.Cmds["calls"] = script.Command(script.CmdUsage{Summary: "check actual forwarded method count", Args: "METHOD COUNT"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		want, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, err
		}
		mu.Lock()
		got := calls[args[0]]
		if args[0] == "ALL" {
			got = 0
			for method, n := range calls {
				if !strings.HasPrefix(method, "ok:") {
					got += n
				}
			}
		}
		mu.Unlock()
		if got != want {
			return nil, fmt.Errorf("%s calls=%d want=%d", args[0], got, want)
		}
		return nil, nil
	})
	e.Cmds["fault"] = script.Command(script.CmdUsage{Summary: "discard or corrupt one real daemon response", Args: "MODE"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		switch args[0] {
		case "write-lost", "finalize-lost", "download-offset", "download-bytes":
		default:
			return nil, script.ErrUsage
		}
		mu.Lock()
		defer mu.Unlock()
		if fault != "" {
			return nil, errors.New("previous transport fault was not consumed")
		}
		fault = args[0]
		return nil, nil
	})
	e.Cmds["upload-id"] = script.Command(script.CmdUsage{Summary: "read actual upload recovery hint", Args: "ENV"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		for _, line := range strings.Split(s.Stderr(), "\n") {
			if strings.HasPrefix(line, "upload-id: ") {
				id := strings.TrimPrefix(line, "upload-id: ")
				if !validHexID(id) {
					return nil, errors.New("invalid recovery hint")
				}
				return nil, s.Setenv(args[0], id)
			}
		}
		return nil, errors.New("no upload recovery hint")
	})
	e.Cmds["artifact-task"] = artifactScriptTask(t)
	e.Cmds["artifact-result"] = script.Command(script.CmdUsage{Summary: "extract an actual fixed-attempt available result reference", Args: "JOB_JSON REF_JSON"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		data, err := os.ReadFile(s.Path(args[0]))
		if err != nil {
			return nil, err
		}
		var job v1.Job
		if err := protojson.Unmarshal(data, &job); err != nil {
			return nil, err
		}
		result := job.Result
		if result == nil || result.State != v1.ArtifactResultState_ARTIFACT_RESULT_STATE_AVAILABLE || result.AttemptId == "" {
			return nil, errors.New("job has no available fixed-attempt result")
		}
		if len(job.Attempts) == 0 {
			if result.AttemptId != job.Id {
				return nil, errors.New("default result attempt changed")
			}
		} else {
			last := job.Attempts[len(job.Attempts)-1]
			if last.Id != result.AttemptId || !proto.Equal(last.Result, result) {
				return nil, errors.New("job and fixed attempt result differ")
			}
		}
		if _, err := checkedArtifactRef(result.Reference); err != nil {
			return nil, err
		}
		b, err := protojson.Marshal(result.Reference)
		if err != nil {
			return nil, err
		}
		return nil, os.WriteFile(s.Path(args[1]), b, 0600)
	})
	e.Cmds["private-mode"] = script.Command(script.CmdUsage{Summary: "check extracted private directory and regular file permissions", Args: "DIR FILE"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		for i, name := range args {
			info, err := os.Lstat(s.Path(name))
			if err != nil {
				return nil, err
			}
			want := os.FileMode(0600)
			if i == 0 {
				want = 0700
				if !info.IsDir() {
					return nil, errors.New("extracted root is not a directory")
				}
			} else if !info.Mode().IsRegular() {
				return nil, errors.New("extracted entry is not regular")
			}
			if info.Mode().Perm() != want {
				return nil, fmt.Errorf("%s mode=%04o want=%04o", name, info.Mode().Perm(), want)
			}
		}
		return nil, nil
	})
	e.Cmds["empty-directory"] = script.Command(script.CmdUsage{Summary: "check a complete empty directory", Args: "DIR"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		f, err := os.Open(s.Path(args[0]))
		if err != nil {
			return nil, err
		}
		defer f.Close()
		entries, err := f.ReadDir(1)
		if len(entries) != 0 || !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("downloaded empty snapshot has entries or read error: count=%d err=%v", len(entries), err)
		}
		return nil, nil
	})
	e.Cmds["many-inputs"] = script.Command(script.CmdUsage{Summary: "create 17 actual sibling inputs plus an empty directory", Args: "DIR"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		for i := 0; i < 17; i++ {
			if err := os.WriteFile(s.Path(filepath.Join(args[0], fmt.Sprintf("file%02d", i))), []byte(strconv.Itoa(i)), 0600); err != nil {
				return nil, err
			}
		}
		return nil, os.Mkdir(s.Path(filepath.Join(args[0], "empty")), 0700)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	t.Cleanup(func() {
		mu.Lock()
		remaining := fault
		mu.Unlock()
		if remaining != "" {
			t.Fatalf("transport fault was never reached: %s", remaining)
		}
	})
	scripttest.Test(t, ctx, e, []string{"SKYGO_ENDPOINT=" + server.URL, "SKYGO_TOKEN=coverage-token", "DATA=" + process.data}, file)
}

func artifactScriptTask(t *testing.T) script.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cover := coverDir(t)
	return script.Command(script.CmdUsage{Summary: "write a real owned workload task", Args: "REF_JSON|default TASK_JSON"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		env := map[string]string{"SKYGO_ARTIFACT_WORKLOAD": "workspace", "SKYGO_ARTIFACT_MARKER": s.Path("native-marker")}
		if cover != "" {
			env["GOCOVERDIR"] = cover
		}
		argv := []string{exe, "-test.run=^TestArtifactWorkload$"}
		task := map[string]any{"argv": argv, "env": env}
		if args[0] == "default" {
			env["SKYGO_ARTIFACT_WORKLOAD"] = "baseline"
		} else {
			ref, err := readArtifactRef(s.Path(args[0]))
			if err != nil {
				return nil, err
			}
			task["workdir"] = ref
			task["setup"] = append(append([]string(nil), argv...), "--", "setup")
			task["outputs"] = []string{"out"}
		}
		b, err := json.Marshal(task)
		if err != nil {
			return nil, err
		}
		return nil, os.WriteFile(s.Path(args[1]), b, 0600)
	})
}
