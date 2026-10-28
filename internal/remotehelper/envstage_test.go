//go:build linux || darwin

package remotehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// These tests run the helper's env-put, env-release and env-wait on a local
// directory. The instance file stands in for the kubelet's downward API
// volume; that the kubelet writes the Pod's own metadata.uid there, and
// starts the workspace container only after env-wait exits 0, are native
// facts the tests do not establish.

const envTestUID = "8c0c7d4e-1b2a-4c3d-9e8f-0a1b2c3d4e5f"

func envTestPaths(t *testing.T, uid string) paths {
	t.Helper()
	root := t.TempDir()
	p := paths{instance: filepath.Join(root, "instance"), artifacts: filepath.Join(root, "artifacts")}
	for _, d := range []string{p.instance, p.artifacts} {
		if err := os.Mkdir(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if uid != "" {
		if err := os.WriteFile(filepath.Join(p.instance, "uid"), []byte(uid), 0444); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func envTestArgs(uid, name, body string) []string {
	sum := sha256.Sum256([]byte(body))
	return []string{uid, name, hex.EncodeToString(sum[:]), strconv.Itoa(len(body))}
}

// envTestSpecs returns the manifest operands for bodies a0, a1 and so on.
func envTestSpecs(bodies ...string) []string {
	var specs []string
	for i, b := range bodies {
		a := envTestArgs("", "a"+strconv.Itoa(i), b)
		specs = append(specs, a[1]+":"+a[2]+":"+a[3])
	}
	return specs
}

// envTestManifest records the manifest for bodies as env-wait would.
func envTestManifest(t *testing.T, p paths, bodies ...string) []string {
	t.Helper()
	specs := envTestSpecs(bodies...)
	list, err := parseEnvManifest(specs)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.artifacts, envManifest), manifestBytes(list), 0444); err != nil {
		t.Fatal(err)
	}
	return specs
}

func envTestPut(t *testing.T, p paths, name, body string) {
	t.Helper()
	if err := putArtifact(envTestArgs(envTestUID, name, body), strings.NewReader(body), p); err != nil {
		t.Fatalf("put %s: %v", name, err)
	}
}

// entries returns the names in dir, sorted.
func entries(t *testing.T, dir string) []string {
	t.Helper()
	des, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, de := range des {
		names = append(names, de.Name())
	}
	return names
}

// openLock opens the staging lock as another process would.
func openLock(t *testing.T, p paths) *os.File {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(p.artifacts, envLock), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// pending fails the test if done delivers within d.
func pending(t *testing.T, done <-chan error, d time.Duration, what string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("%s returned %v", what, err)
	case <-time.After(d):
	}
}

func receive(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("did not return")
		return nil
	}
}

// strictReader fails the test if it is read.
type strictReader struct{ t *testing.T }

func (r strictReader) Read([]byte) (int, error) {
	r.t.Error("stdin read before the checks passed")
	return 0, io.EOF
}

func TestPutArtifact(t *testing.T) {
	p := envTestPaths(t, envTestUID)
	envTestManifest(t, p, "hello")
	envTestPut(t, p, "a0", "hello")
	info, err := os.Stat(filepath.Join(p.artifacts, "a0"))
	if err != nil || info.Mode().Perm() != 0444 || info.Size() != 5 {
		t.Fatalf("a0 = %v, %v", info, err)
	}
	if got := entries(t, p.artifacts); strings.Join(got, ",") != ".lock,.manifest,a0" {
		t.Fatalf("entries = %q", got)
	}
	// Again, as after an unknown result: the file is verified, not replaced.
	envTestPut(t, p, "a0", "hello")
	if got := entries(t, p.artifacts); strings.Join(got, ",") != ".lock,.manifest,a0" {
		t.Fatalf("entries after repeat = %q", got)
	}
}

func TestPutArtifactIdentity(t *testing.T) {
	for _, tt := range []struct {
		name     string
		file     string // "" writes no uid file
		expected string
	}{
		{"different uid", "11111111-1b2a-4c3d-9e8f-0a1b2c3d4e5f", envTestUID},
		{"trailing newline", envTestUID + "\n", envTestUID},
		{"missing file", "", envTestUID},
		{"invalid expected", envTestUID, "uid with space"},
		{"empty expected", envTestUID, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := envTestPaths(t, tt.file)
			err := putArtifact(envTestArgs(tt.expected, "a0", "hello"), strictReader{t}, p)
			if !errors.Is(err, errIdentity) {
				t.Fatalf("err = %v, want errIdentity", err)
			}
			if got := entries(t, p.artifacts); len(got) != 0 {
				t.Fatalf("refusal created %q", got)
			}
		})
	}
}

func TestPutArtifactRefused(t *testing.T) {
	for _, tt := range []struct {
		name  string
		args  []string
		stdin string
	}{
		{"short", envTestArgs(envTestUID, "a0", "hello"), "hell"},
		{"long", envTestArgs(envTestUID, "a0", "hello"), "hello!"},
		{"digest", envTestArgs(envTestUID, "a0", "hello"), "jello"},
		{"name", envTestArgs(envTestUID, "a8", "hello"), "hello"},
		{"not in manifest", envTestArgs(envTestUID, "a1", "hello"), "hello"},
		{"manifest differs", envTestArgs(envTestUID, "a0", "jello"), "jello"},
		{"noncanonical size", []string{envTestUID, "a0", strings.Repeat("0", 64), "05"}, "hello"},
		{"zero size", envTestArgs(envTestUID, "a0", ""), ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := envTestPaths(t, envTestUID)
			envTestManifest(t, p, "hello")
			if err := putArtifact(tt.args, strings.NewReader(tt.stdin), p); err == nil || errors.Is(err, errIdentity) {
				t.Fatalf("err = %v, want a failure", err)
			}
			if got := entries(t, p.artifacts); strings.Join(got, ",") != ".lock,.manifest" &&
				strings.Join(got, ",") != ".manifest" {
				t.Errorf("entries = %q", got)
			}
		})
	}
}

// TestPutArtifactNoManifest is a put that arrives before env-wait recorded
// the manifest: it fails and creates no artifact.
func TestPutArtifactNoManifest(t *testing.T) {
	p := envTestPaths(t, envTestUID)
	if err := putArtifact(envTestArgs(envTestUID, "a0", "hello"), strictReader{t}, p); err == nil {
		t.Fatal("put without a manifest succeeded")
	}
	if got := entries(t, p.artifacts); strings.Join(got, ",") != ".lock" {
		t.Fatalf("entries = %q", got)
	}
}

func TestPutArtifactExistingDiffers(t *testing.T) {
	p := envTestPaths(t, envTestUID)
	envTestManifest(t, p, "hello")
	if err := os.WriteFile(filepath.Join(p.artifacts, "a0"), []byte("jello"), 0444); err != nil {
		t.Fatal(err)
	}
	if err := putArtifact(envTestArgs(envTestUID, "a0", "hello"), strings.NewReader("hello"), p); err == nil {
		t.Fatal("accepted a different existing file")
	}
	if data, _ := os.ReadFile(filepath.Join(p.artifacts, "a0")); string(data) != "jello" {
		t.Fatalf("existing file replaced: %q", data)
	}
}

// TestPutArtifactStalePartial is a retry after a put that was killed
// mid-copy: its temporary file stays, the retry succeeds and so does the
// release. The stale file's space is not reclaimed.
func TestPutArtifactStalePartial(t *testing.T) {
	p := envTestPaths(t, envTestUID)
	specs := envTestManifest(t, p, "hello")
	stale := filepath.Join(p.artifacts, ".a0.0011223344556677.part")
	if err := os.WriteFile(stale, []byte("hel"), 0600); err != nil {
		t.Fatal(err)
	}
	envTestPut(t, p, "a0", "hello")
	if err := releaseArtifacts(context.Background(), envTestUID, specs, p); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); err != nil {
		t.Fatalf("stale partial: %v", err)
	}
}

// TestPutArtifactSealed is a put that starts after the release: it refuses
// with errSealed before reading stdin and changes no file.
func TestPutArtifactSealed(t *testing.T) {
	p := envTestPaths(t, envTestUID)
	specs := envTestManifest(t, p, "hello", "x")
	envTestPut(t, p, "a0", "hello")
	envTestPut(t, p, "a1", "x")
	if err := releaseArtifacts(context.Background(), envTestUID, specs, p); err != nil {
		t.Fatal(err)
	}
	before := entries(t, p.artifacts)
	err := putArtifact(envTestArgs(envTestUID, "a0", "hello"), strictReader{t}, p)
	if !errors.Is(err, errSealed) {
		t.Fatalf("err = %v, want errSealed", err)
	}
	if got := entries(t, p.artifacts); strings.Join(got, ",") != strings.Join(before, ",") {
		t.Fatalf("entries = %q, want %q", got, before)
	}
}

// TestPutArtifactQueuedSealed is a put that was waiting for the lock while
// the release held it: once the lock is free it finds the marker and
// refuses.
func TestPutArtifactQueuedSealed(t *testing.T) {
	p := envTestPaths(t, envTestUID)
	specs := envTestManifest(t, p, "hello")
	envTestPut(t, p, "a0", "hello")
	lock := openLock(t, p)
	if locked, err := tryLockExclusive(lock); err != nil || !locked {
		t.Fatalf("lock: %v, %v", locked, err)
	}
	done := make(chan error, 1)
	go func() { done <- putArtifact(envTestArgs(envTestUID, "a0", "hello"), strictReader{t}, p) }()
	pending(t, done, time.Second, "queued put")
	// The release, as env-release records it under the exclusive lock.
	list, _ := parseEnvManifest(specs)
	marker := append([]byte(envTestUID+"\n"), manifestBytes(list)...)
	if err := os.WriteFile(filepath.Join(p.artifacts, envReleased), marker, 0444); err != nil {
		t.Fatal(err)
	}
	lock.Close()
	if err := receive(t, done); !errors.Is(err, errSealed) {
		t.Fatalf("err = %v, want errSealed", err)
	}
}

func TestReleaseArtifacts(t *testing.T) {
	p := envTestPaths(t, envTestUID)
	specs := envTestManifest(t, p, "hello", "x")
	envTestPut(t, p, "a0", "hello")
	envTestPut(t, p, "a1", "x")
	if err := releaseArtifacts(context.Background(), envTestUID, specs, p); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(p.artifacts, envReleased))
	if err != nil {
		t.Fatal(err)
	}
	want := envTestUID + "\n" + strings.Join(specs, "\n") + "\n"
	if string(data) != want {
		t.Fatalf("marker = %q, want %q", data, want)
	}
	// Again, as after an unknown result: the same marker is success.
	if err := releaseArtifacts(context.Background(), envTestUID, specs, p); err != nil {
		t.Fatalf("repeat: %v", err)
	}
	// Another manifest is not.
	if err := releaseArtifacts(context.Background(), envTestUID, specs[:1], p); err == nil {
		t.Fatal("release with another manifest succeeded")
	}
}

// TestReleaseArtifactsMarkerSync fails the sync of the staging directory
// after the release linked the marker. The marker is visible, but a repeat
// that finds it is not success until a sync of that same directory succeeds.
// No repeat replaces the marker or an artifact. Only the sync is modeled;
// the files and their checks are real.
func TestReleaseArtifactsMarkerSync(t *testing.T) {
	ctx := context.Background()
	p := envTestPaths(t, envTestUID)
	specs := envTestManifest(t, p, "hello")
	envTestPut(t, p, "a0", "hello")
	staging, err := os.Stat(p.artifacts)
	if err != nil {
		t.Fatal(err)
	}
	errSync := errors.New("injected directory sync failure")
	var failed, confirmed int
	sameDir := func(dir *os.File) {
		t.Helper()
		if info, err := dir.Stat(); err != nil || !os.SameFile(info, staging) {
			t.Errorf("synced %v, %v; want the staging directory", info, err)
		}
	}
	fail := func(dir *os.File) error {
		failed++
		sameDir(dir)
		return errSync
	}
	confirm := func(dir *os.File) error {
		confirmed++
		sameDir(dir)
		return dir.Sync()
	}

	if err := releaseArtifactsWith(ctx, envTestUID, specs, p, fail); !errors.Is(err, errSync) {
		t.Fatalf("release = %v, want the sync failure", err)
	}
	if failed != 1 {
		t.Fatalf("syncs = %d, want 1", failed)
	}
	markerPath, a0Path := filepath.Join(p.artifacts, envReleased), filepath.Join(p.artifacts, "a0")
	marker, err := os.Stat(markerPath)
	if err != nil {
		t.Fatalf("marker not linked before the failed sync: %v", err)
	}
	a0, err := os.Stat(a0Path)
	if err != nil {
		t.Fatal(err)
	}
	want := envTestUID + "\n" + strings.Join(specs, "\n") + "\n"
	if got := entries(t, p.artifacts); strings.Join(got, ",") != ".lock,.manifest,.released,a0" {
		t.Fatalf("entries = %q", got)
	}
	unchanged := func(what string) {
		t.Helper()
		if got := entries(t, p.artifacts); strings.Join(got, ",") != ".lock,.manifest,.released,a0" {
			t.Fatalf("%s: entries = %q", what, got)
		}
		info, err := os.Stat(markerPath)
		if err != nil || !os.SameFile(info, marker) || info.Mode().Perm() != 0444 {
			t.Fatalf("%s: marker = %v, %v; want the first one, read-only", what, info, err)
		}
		if data, err := os.ReadFile(markerPath); err != nil || string(data) != want {
			t.Fatalf("%s: marker = %q, %v; want %q", what, data, err, want)
		}
		if info, err := os.Stat(a0Path); err != nil || !os.SameFile(info, a0) {
			t.Fatalf("%s: a0 = %v, %v; want the staged file", what, info, err)
		}
	}
	unchanged("failed release")

	// Repeats, as after an unknown result, fail while the sync fails.
	for i := range 2 {
		if err := releaseArtifactsWith(ctx, envTestUID, specs, p, fail); !errors.Is(err, errSync) {
			t.Fatalf("repeat %d = %v, want the sync failure", i, err)
		}
		unchanged("repeat")
	}
	if failed != 3 || confirmed != 0 {
		t.Fatalf("failed syncs %d, confirmed %d; want 3 and 0", failed, confirmed)
	}

	// A repeat whose sync succeeds confirms the marker.
	if err := releaseArtifactsWith(ctx, envTestUID, specs, p, confirm); err != nil {
		t.Fatalf("confirming repeat = %v, want nil", err)
	}
	if confirmed != 1 {
		t.Fatalf("confirmed syncs = %d, want 1", confirmed)
	}
	unchanged("confirming repeat")
	if err := releaseArtifacts(ctx, envTestUID, specs, p); err != nil {
		t.Fatalf("release = %v, want nil", err)
	}
	unchanged("release")
}

// TestReleaseArtifactsMarkerDiffersNoSync finds a marker that records another
// manifest: the release fails without syncing the directory.
func TestReleaseArtifactsMarkerDiffersNoSync(t *testing.T) {
	p := envTestPaths(t, envTestUID)
	specs := envTestManifest(t, p, "hello")
	envTestPut(t, p, "a0", "hello")
	if err := os.WriteFile(filepath.Join(p.artifacts, envReleased), []byte(envTestUID+"\n"), 0444); err != nil {
		t.Fatal(err)
	}
	syncs := 0
	err := releaseArtifactsWith(context.Background(), envTestUID, specs, p, func(*os.File) error {
		syncs++
		return nil
	})
	if err == nil || syncs != 0 {
		t.Fatalf("release = %v after %d syncs, want a failure and none", err, syncs)
	}
}

func TestReleaseArtifactsRefused(t *testing.T) {
	for _, tt := range []struct {
		name  string
		uid   string
		puts  []string // bodies put as a0, a1 and so on
		specs []string // nil uses the manifest
		ident bool
	}{
		{"different uid", "11111111-1b2a-4c3d-9e8f-0a1b2c3d4e5f", []string{"hello", "x"}, nil, true},
		{"missing artifact", envTestUID, []string{"hello"}, nil, false},
		{"manifest differs", envTestUID, []string{"hello", "x"}, envTestSpecs("hello", "y"), false},
		{"manifest shorter", envTestUID, []string{"hello", "x"}, envTestSpecs("hello"), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := envTestPaths(t, envTestUID)
			specs := envTestManifest(t, p, "hello", "x")
			for i, b := range tt.puts {
				envTestPut(t, p, "a"+strconv.Itoa(i), b)
			}
			if tt.specs != nil {
				specs = tt.specs
			}
			err := releaseArtifacts(context.Background(), tt.uid, specs, p)
			if err == nil || errors.Is(err, errIdentity) != tt.ident {
				t.Fatalf("err = %v, want failure, errIdentity %v", err, tt.ident)
			}
			if _, err := os.Stat(filepath.Join(p.artifacts, envReleased)); err == nil {
				t.Fatal("refusal recorded the marker")
			}
		})
	}
}

// TestReleaseArtifactsWaitsForPut is a release while a put, perhaps one
// whose client was canceled, holds the lock shared: it records nothing
// until the put ends.
func TestReleaseArtifactsWaitsForPut(t *testing.T) {
	p := envTestPaths(t, envTestUID)
	specs := envTestManifest(t, p, "hello")
	envTestPut(t, p, "a0", "hello")
	lock := openLock(t, p)
	if err := lockShared(lock); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- releaseArtifacts(context.Background(), envTestUID, specs, p) }()
	pending(t, done, 3*time.Second, "release during a put")
	if _, err := os.Stat(filepath.Join(p.artifacts, envReleased)); err == nil {
		t.Fatal("marker recorded during a put")
	}
	lock.Close()
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}

// TestWaitArtifacts is the barrier: every artifact staged by a successful
// put, with no put running, does not release it; only the release does.
func TestWaitArtifacts(t *testing.T) {
	p := envTestPaths(t, envTestUID)
	specs := envTestSpecs("hello", "x")
	done := make(chan error, 1)
	go func() { done <- waitArtifacts(context.Background(), specs, p) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(p.artifacts, envManifest)); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("wait recorded no manifest")
		}
		time.Sleep(10 * time.Millisecond)
	}
	envTestPut(t, p, "a0", "hello")
	envTestPut(t, p, "a1", "x")
	pending(t, done, 3*time.Second, "wait without release")
	if err := releaseArtifacts(context.Background(), envTestUID, specs, p); err != nil {
		t.Fatal(err)
	}
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestWaitArtifactsMarker(t *testing.T) {
	list, _ := parseEnvManifest(envTestSpecs("hello"))
	for _, tt := range []struct {
		name   string
		marker string
	}{
		{"other uid", "11111111-1b2a-4c3d-9e8f-0a1b2c3d4e5f\n" + string(manifestBytes(list))},
		{"other manifest", envTestUID + "\n" + envTestSpecs("jello")[0] + "\n"},
		{"truncated", envTestUID + "\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := envTestPaths(t, envTestUID)
			specs := envTestManifest(t, p, "hello")
			envTestPut(t, p, "a0", "hello")
			if err := os.WriteFile(filepath.Join(p.artifacts, envReleased), []byte(tt.marker), 0444); err != nil {
				t.Fatal(err)
			}
			if err := waitArtifacts(context.Background(), specs, p); err == nil {
				t.Fatal("wait accepted the marker")
			}
		})
	}
}

func TestWaitArtifactsVerifies(t *testing.T) {
	p := envTestPaths(t, envTestUID)
	specs := envTestManifest(t, p, "hello")
	if err := os.WriteFile(filepath.Join(p.artifacts, "a0"), []byte("jello"), 0444); err != nil {
		t.Fatal(err)
	}
	list, _ := parseEnvManifest(specs)
	marker := append([]byte(envTestUID+"\n"), manifestBytes(list)...)
	if err := os.WriteFile(filepath.Join(p.artifacts, envReleased), marker, 0444); err != nil {
		t.Fatal(err)
	}
	if err := waitArtifacts(context.Background(), specs, p); err == nil {
		t.Fatal("wait accepted a file that differs")
	}
}

func TestWaitArtifactsManifest(t *testing.T) {
	p := envTestPaths(t, envTestUID)
	a := envTestArgs("", "a0", "hello")
	// Names must be a0, a1 and so on in order, with a size and a lowercase
	// digest.
	for _, specs := range [][]string{
		{"a1:" + a[2] + ":" + a[3]},
		{"a0:" + a[2]},
		{"a0:" + strings.ToUpper(a[2]) + ":5"},
	} {
		if err := waitArtifacts(context.Background(), specs, p); !errors.Is(err, errArguments) {
			t.Errorf("%q: err = %v, want errArguments", specs, err)
		}
	}
	// A restarted wait requires the recorded manifest.
	envTestManifest(t, p, "hello")
	if err := waitArtifacts(context.Background(), envTestSpecs("jello"), p); err == nil {
		t.Error("wait accepted another manifest")
	}
}

func TestEnvGrammar(t *testing.T) {
	for _, tt := range []struct {
		args []string
		ok   bool
	}{
		{[]string{"env-put", "u", "a0", "s", "1"}, true},
		{[]string{"env-put", "u", "a0", "s"}, false},
		{[]string{"env-wait", "--", "a0:s:1"}, true},
		{[]string{"env-wait", "--"}, false},
		{[]string{"env-wait", "a0:s:1", "--"}, false},
		{append([]string{"env-wait", "--"}, make([]string, maxEnvArtifacts+1)...), false},
		{[]string{"env-release", "u", "--", "a0:s:1"}, true},
		{[]string{"env-release", "u", "--"}, false},
		{[]string{"env-release", "--", "a0:s:1"}, false},
		{[]string{"env-release", "u", "a0:s:1", "--"}, false},
		{append([]string{"env-release", "u", "--"}, make([]string, maxEnvArtifacts+1)...), false},
	} {
		if err := grammar(tt.args); (err == nil) != tt.ok {
			t.Errorf("grammar(%q) = %v", tt.args, err)
		}
	}
}

// TestRunEnvIdentity runs the commands with the production paths, where no
// instance file exists outside a Pod: they exit identityExit.
func TestRunEnvIdentity(t *testing.T) {
	if _, err := os.Stat(productionPaths.instance); err == nil {
		t.Skip("production instance path exists")
	}
	t.Cleanup(func() {
		processFailure.Lock()
		processFailure.err = nil
		processFailure.Unlock()
	})
	for _, args := range [][]string{
		append([]string{"env-put"}, envTestArgs(envTestUID, "a0", "x")...),
		append([]string{"env-release", envTestUID, "--"}, envTestSpecs("x")...),
	} {
		// The failure is retained for the process, as in production; each
		// run starts without it.
		processFailure.Lock()
		processFailure.err = nil
		processFailure.Unlock()
		var stdout, stderr bytes.Buffer
		if code := Run(context.Background(), args, nil, &stdout, &stderr); code != identityExit {
			t.Fatalf("%s: exit = %d, stderr %q", args[0], code, stderr.String())
		}
		if stderr.String() != "skygo-data: instance identity differs\n" {
			t.Fatalf("%s: stderr = %q", args[0], stderr.String())
		}
	}
}
