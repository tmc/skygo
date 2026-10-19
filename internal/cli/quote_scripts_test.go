package cli

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestQuoteScripts runs real executables/daemon assembly. The TLS Kubernetes API
// is a request counter, not a scheduler; no script may call it. Quote computation,
// constraints, facts parsing and serialization are production code throughout.
func TestQuoteScripts(t *testing.T) {
	core := quoteScriptBinary(t, "SKYGO_QUOTE_CORE_BINARY", "../..", "./cmd/skygo")
	kube := quoteScriptBinary(t, "SKYGO_QUOTE_KUBE_BINARY", "../../integrations/kubernetes", "./cmd/skygo-kubernetes")
	files, err := filepath.Glob("testdata/quotes/*.txt")
	if err != nil || len(files) == 0 {
		t.Fatalf("quote scripts: %v", err)
	}
	for _, file := range files {
		t.Run(strings.TrimSuffix(filepath.Base(file), ".txt"), func(t *testing.T) {
			old := ""
			if filepath.Base(file) == "old_server.txt" {
				old = os.Getenv("SKYGO_QUOTE_OLD_BINARY")
				if old == "" {
					if os.Getenv("SKYGO_QUOTE_REQUIRE_LEGACY") == "1" {
						t.Fatal("pinned pre-A daemon required")
					}
					t.Skip("set pinned SKYGO_QUOTE_OLD_BINARY/SHA256/SOURCE; final Gate A requires SKYGO_QUOTE_REQUIRE_LEGACY=1")
				}
				sum := os.Getenv("SKYGO_QUOTE_OLD_SHA256")
				source := os.Getenv("SKYGO_QUOTE_OLD_SOURCE")
				if len(sum) != 64 || source != "db1c8bbf7d8325bd5502c2c71ef0ec9d6c4676f3" {
					t.Fatal("old daemon needs exact SHA256 and accepted S05 source commit")
				}
				data, err := os.ReadFile(old)
				if err != nil {
					t.Fatal(err)
				}
				got := sha256.Sum256(data)
				if hex.EncodeToString(got[:]) != sum {
					t.Fatal("old daemon SHA256 differs")
				}
				t.Logf("actual pre-A source %s artifact %s", source, sum)
			}
			f := newQuoteScriptFixture(t, core, kube, old)
			e := executableScriptEngine(core, []string{"--token-file", f.tokenFile}, nil)
			e.Cmds["planner"] = f.planner("kube")
			e.Cmds["coreplanner"] = f.planner("core")
			e.Cmds["oldplanner"] = f.planner("old")
			e.Cmds["validatefacts"] = f.validateFacts()
			e.Cmds["apicalls"] = f.apiCalls()
			e.Cmds["rpccalls"] = f.rpcCalls()
			e.Cmds["httpcalls"] = f.httpCalls()
			e.Cmds["quotejson"] = f.quoteJSON()
			e.Cmds["taskunchanged"] = f.taskUnchanged()
			e.Cmds["wirebounds"] = f.wireBounds()
			e.Cmds["bounded"] = f.bounded()
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			t.Cleanup(cancel)
			scripttest.Test(t, ctx, e, []string{"SKYGO_TOKEN="}, file)
		})
	}
}

func quoteScriptBinary(t *testing.T, env, dir, pkg string) string {
	t.Helper()
	if exe := os.Getenv(env); exe != "" {
		p, err := filepath.Abs(exe)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	exe := filepath.Join(t.TempDir(), "skygo")
	cmd := exec.Command("go", "build", "-p=2", "-mod=readonly", "-o", exe, pkg)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build quote executable: %v\n%s", err, out)
	}
	return exe
}

type quoteScriptFixture struct {
	t                          *testing.T
	core, kube, old, tokenFile string
	native                     *httptest.Server
	api                        atomic.Int32
	process                    *serverProcess
	proxy                      *httptest.Server
	mu                         sync.Mutex
	calls                      map[string]int
	request                    *v1.QuoteTaskRequest
	response                   []byte
}

func newQuoteScriptFixture(t *testing.T, core, kube, old string) *quoteScriptFixture {
	f := &quoteScriptFixture{t: t, core: core, kube: kube, old: old, tokenFile: filepath.Join(t.TempDir(), "token"), calls: make(map[string]int)}
	if err := os.WriteFile(f.tokenFile, []byte("quote-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	f.native = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.api.Add(1)
		http.Error(w, "unexpected native API call", 503)
	}))
	t.Cleanup(func() { f.stop(); f.native.Close() })
	return f
}

func (f *quoteScriptFixture) stop() {
	if f.proxy != nil {
		f.proxy.Close()
		f.proxy = nil
	}
	if f.process != nil {
		f.process.cleanup()
		f.process = nil
	}
}

func quoteRender(data []byte) []byte {
	now := time.Now().UTC()
	r := strings.NewReplacer("$FRESH_START", now.Add(-time.Minute).Format(time.RFC3339Nano), "$FRESH_END", now.Add(10*time.Minute).Format(time.RFC3339Nano), "$STALE_START", now.Add(-20*time.Minute).Format(time.RFC3339Nano), "$STALE_END", now.Add(-10*time.Minute).Format(time.RFC3339Nano), "$FUTURE_START", now.Add(10*time.Minute).Format(time.RFC3339Nano), "$FUTURE_END", now.Add(20*time.Minute).Format(time.RFC3339Nano))
	return []byte(r.Replace(string(data)))
}

func (f *quoteScriptFixture) daemonArgs(s *script.State, mode, facts string) (string, []string, error) {
	args := []string{"--token-file", f.tokenFile}
	if mode == "old" {
		return f.old, args, nil
	}
	data, err := os.ReadFile(s.Path(facts))
	if err != nil {
		return "", nil, err
	}
	file := filepath.Join(f.t.TempDir(), "facts.yaml")
	if err = os.WriteFile(file, quoteRender(data), 0600); err != nil {
		return "", nil, err
	}
	args = append(args, "--target-facts", file)
	if mode == "core" {
		return f.core, args, nil
	}
	dir := f.t.TempDir()
	kubeconfig := filepath.Join(dir, "kubeconfig")
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.native.Certificate().Raw})
	config := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters:\n- name: fixture\n  cluster:\n    server: %s\n    certificate-authority-data: %s\nusers:\n- name: fixture\n  user: {token: native-token}\ncontexts:\n- name: fixture\n  context: {cluster: fixture, user: fixture}\ncurrent-context: fixture\n", f.native.URL, base64.StdEncoding.EncodeToString(ca))
	if err = os.WriteFile(kubeconfig, []byte(config), 0600); err != nil {
		return "", nil, err
	}
	targets := fmt.Sprintf("version: 1\ntargets:\n- name: a\n  kubeconfig: %q\n  context: fixture\n  namespace: quotes-a\n  labels: {pool: batch}\n  resources: {max_cpu_millis: 2000, max_memory_bytes: 1073741824}\n- name: b\n  kubeconfig: %q\n  context: fixture\n  namespace: quotes-b\n  labels: {pool: batch}\n  resources: {max_cpu_millis: 2000, max_memory_bytes: 1073741824}\n", kubeconfig, kubeconfig)
	targetFile := filepath.Join(dir, "targets.yaml")
	if err = os.WriteFile(targetFile, []byte(targets), 0600); err != nil {
		return "", nil, err
	}
	return f.kube, append(args, "--targets", targetFile, "--disable-local"), nil
}

func (f *quoteScriptFixture) planner(mode string) script.Cmd {
	return script.Command(script.CmdUsage{Summary: "start actual configured daemon", Args: "facts.yaml"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if mode == "old" && len(args) != 0 || mode != "old" && len(args) != 1 {
			return nil, script.ErrUsage
		}
		f.stop()
		facts := ""
		if len(args) > 0 {
			facts = args[0]
		}
		exe, flags, err := f.daemonArgs(s, mode, facts)
		if err != nil {
			return nil, err
		}
		f.mu.Lock()
		f.request = nil
		f.response = nil
		f.mu.Unlock()
		p := &serverProcess{exe: exe, address: freeAddress(f.t), data: f.t.TempDir(), args: flags, env: []string{"SKYGO_TOKEN="}}
		f.process = p
		if err = p.start(s); err != nil {
			return nil, err
		}
		target, _ := url.Parse("http://" + p.address)
		proxy := httputil.NewSingleHostReverseProxy(target)
		proxy.ModifyResponse = func(r *http.Response) error {
			if strings.HasSuffix(r.Request.URL.Path, "/QuoteTask") && r.StatusCode == 200 {
				data, err := io.ReadAll(io.LimitReader(r.Body, (65<<10)+1))
				r.Body.Close()
				if err != nil {
					return err
				}
				if len(data) > 65<<10 {
					return errors.New("quote compressed response exceeds fixture bound")
				}
				r.Body = io.NopCloser(bytes.NewReader(data))
				if !strings.Contains(r.Header.Get("Content-Type"), "json") {
					decoded := data
					if r.Header.Get("Content-Encoding") == "gzip" {
						reader, err := gzip.NewReader(bytes.NewReader(data))
						if err != nil {
							return err
						}
						decoded, err = io.ReadAll(io.LimitReader(reader, (64<<10)+1))
						reader.Close()
						if err != nil {
							return err
						}
					}
					if len(decoded) > 64<<10 {
						return errors.New("quote protobuf response exceeds 64 KiB")
					}
					f.mu.Lock()
					f.response = append([]byte(nil), decoded...)
					f.mu.Unlock()
				}
			}
			return nil
		}
		f.proxy = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			f.mu.Lock()
			f.calls[method]++
			f.mu.Unlock()
			if method == "QuoteTask" {
				data, err := io.ReadAll(io.LimitReader(r.Body, (256<<10)+4096))
				if err != nil {
					http.Error(w, "fixture request read", 500)
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(data))
				request := &v1.QuoteTaskRequest{}
				if strings.Contains(r.Header.Get("Content-Type"), "json") {
					err = protojson.Unmarshal(data, request)
				} else {
					err = proto.Unmarshal(data, request)
				}
				if err != nil {
					http.Error(w, "fixture request decode", 500)
					return
				}
				f.mu.Lock()
				f.request = proto.Clone(request).(*v1.QuoteTaskRequest)
				f.mu.Unlock()
			}
			proxy.ServeHTTP(w, r)
		}))
		return nil, s.Setenv("SKYGO_ENDPOINT", f.proxy.URL)
	})
}

func (f *quoteScriptFixture) validateFacts() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "require actual daemon facts startup failure", Args: "file pattern"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		exe, flags, err := f.daemonArgs(s, "kube", args[0])
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(s.Context(), 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, exe, append([]string{"server", "--listen", freeAddress(f.t), "--data-dir", f.t.TempDir()}, flags...)...)
		cmd.Env = append(s.Environ(), "SKYGO_TOKEN=")
		output, err := cmd.CombinedOutput()
		if ctx.Err() != nil {
			return nil, fmt.Errorf("facts did not fail startup: %w", ctx.Err())
		}
		if err == nil {
			return nil, errors.New("invalid facts were accepted")
		}
		ok, matchErr := regexp.Match(args[1], output)
		if matchErr != nil {
			return nil, matchErr
		}
		if !ok {
			return nil, fmt.Errorf("startup output %q does not match %q", output, args[1])
		}
		f.t.Logf("actual rejected facts startup: %s", output)
		return nil, nil
	})
}

func (f *quoteScriptFixture) apiCalls() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "check native API calls", Args: "count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		want, err := strconv.Atoi(args[0])
		if err != nil {
			return nil, err
		}
		if got := int(f.api.Load()); got != want {
			return nil, fmt.Errorf("native API calls %d want %d", got, want)
		}
		return nil, nil
	})
}
func (f *quoteScriptFixture) rpcCalls() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "check actual forwarded RPC count", Args: "method count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		want, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, err
		}
		f.mu.Lock()
		got := f.calls[args[0]]
		f.mu.Unlock()
		if got != want {
			return nil, fmt.Errorf("%s calls %d want %d", args[0], got, want)
		}
		return nil, nil
	})
}
func (f *quoteScriptFixture) taskUnchanged() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "compare captured Task with independent protobuf JSON fixture", Args: "file"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		data, err := os.ReadFile(s.Path(args[0]))
		if err != nil {
			return nil, err
		}
		want := &v1.Task{}
		if err = protojson.Unmarshal(data, want); err != nil {
			return nil, err
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.request == nil || !proto.Equal(f.request.Task, want) {
			return nil, fmt.Errorf("quoted Task changed: %v; want %v", f.request, want)
		}
		return nil, nil
	})
}

func (f *quoteScriptFixture) wireBounds() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "verify actual protobuf and JSON wire response bounds"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		f.mu.Lock()
		if f.request == nil {
			f.mu.Unlock()
			return nil, errors.New("no captured quote")
		}
		request := proto.Clone(f.request)
		raw := append([]byte(nil), f.response...)
		f.mu.Unlock()
		response := &v1.QuoteTaskResponse{}
		if err := proto.Unmarshal(raw, response); err != nil {
			return nil, err
		}
		if len(raw) > 64<<10 || proto.Size(response) > 64<<10 {
			return nil, errors.New("protobuf response exceeds 64 KiB")
		}
		data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(request)
		if err != nil {
			return nil, err
		}
		endpoint, _ := s.LookupEnv("SKYGO_ENDPOINT")
		req, err := http.NewRequestWithContext(s.Context(), http.MethodPost, endpoint+"/skygo.v1.ControlService/QuoteTask", bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Connect-Protocol-Version", "1")
		req.Header.Set("Authorization", "Bearer quote-token")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		data, err = io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != 200 || len(data) > 64<<10 {
			return nil, fmt.Errorf("JSON wire status/size %d/%d", resp.StatusCode, len(data))
		}
		if err = protojson.Unmarshal(data, response); err != nil {
			return nil, err
		}
		return nil, nil
	})
}

func (f *quoteScriptFixture) bounded() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "bound actual CLI process lifetime", Args: "duration skygo args..."}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) < 2 || args[1] != "skygo" {
			return nil, script.ErrUsage
		}
		duration, err := time.ParseDuration(args[0])
		if err != nil || duration <= 0 {
			return nil, script.ErrUsage
		}
		ctx, cancel := context.WithTimeout(s.Context(), duration)
		cmd := exec.CommandContext(ctx, f.core, append([]string{"--token-file", f.tokenFile}, args[2:]...)...)
		cmd.Dir = s.Getwd()
		cmd.Env = s.Environ()
		var out, diag bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &diag
		if err = cmd.Start(); err != nil {
			cancel()
			return nil, err
		}
		return func(*script.State) (string, string, error) {
			defer cancel()
			err := cmd.Wait()
			if ctx.Err() != nil {
				err = fmt.Errorf("bounded CLI deadline: %w", ctx.Err())
			}
			return out.String(), diag.String(), err
		}, nil
	})
}

// quoteJSON inspects the actual CLI JSON and selected nested estimate; it never
// computes an expected quote or infers a missing optional amount as zero.
func (f *quoteScriptFixture) quoteJSON() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "parse actual quote JSON/outcome and optional selected estimate", Args: "outcome [selected-estimate-micros]"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) < 1 || len(args) > 2 {
			return nil, script.ErrUsage
		}
		outcome, ok := v1.QuoteOutcome_value[args[0]]
		if !ok {
			return nil, script.ErrUsage
		}
		response := &v1.QuoteTaskResponse{}
		if err := protojson.Unmarshal([]byte(s.Stdout()), response); err != nil {
			return nil, fmt.Errorf("actual CLI quote JSON: %w", err)
		}
		if response.Outcome != v1.QuoteOutcome(outcome) {
			return nil, fmt.Errorf("outcome %s want %s", response.Outcome, args[0])
		}
		code, _ := s.LookupEnv("SKYGO_EXIT_CODE")
		wantCode := "1"
		if response.Outcome == v1.QuoteOutcome_QUOTE_OUTCOME_SELECTED {
			wantCode = "0"
		}
		if code != wantCode {
			return nil, fmt.Errorf("actual CLI exit %q want %s", code, wantCode)
		}
		if response.Outcome != v1.QuoteOutcome_QUOTE_OUTCOME_SELECTED {
			if response.SelectedTarget != "" || len(args) != 1 {
				return nil, errors.New("no-selection JSON contains selected target/estimate assertion")
			}
			return nil, nil
		}
		if len(args) == 1 {
			return nil, nil
		}
		want, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return nil, err
		}
		for _, candidate := range response.Candidates {
			if candidate.Target == response.SelectedTarget {
				estimate := candidate.EstimatedComputeCost
				if candidate.EstimateStatus != v1.EstimateStatus_ESTIMATE_STATUS_AVAILABLE || estimate == nil || estimate.AmountMicros == nil || *estimate.AmountMicros != want {
					return nil, fmt.Errorf("selected candidate estimate %v status %s want present %d", estimate, candidate.EstimateStatus, want)
				}
				return nil, nil
			}
		}
		return nil, errors.New("selected target has no candidate")
	})
}

func (f *quoteScriptFixture) httpCalls() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "check total actual HTTP requests through RPC endpoint", Args: "count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		want, err := strconv.Atoi(args[0])
		if err != nil {
			return nil, err
		}
		f.mu.Lock()
		got := 0
		for _, n := range f.calls {
			got += n
		}
		f.mu.Unlock()
		if got != want {
			return nil, fmt.Errorf("total HTTP calls %d want %d", got, want)
		}
		return nil, nil
	})
}
