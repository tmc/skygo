package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// The security policy rejects shared writable ancestors such as /tmp. Use a
// private cache fixture and resolve the platform cache's own symlink spelling.
func workspaceTestDirectory(t *testing.T) string {
	t.Helper()
	if err := workspacePlatform(); err != nil {
		t.Skip(err)
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(cache, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(cache, "skygo-workspace-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	real, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return real
}

func workspaceTestIntent(dir string) workspaceIntent {
	return workspaceIntent{Endpoint: "https://example.invalid", CredentialSHA256: strings.Repeat("a", 64), RequestID: "fixed", YAML: []byte("run: true\nworkdir: .\n"), Shell: "/bin/sh", Workdir: dir}
}
func workspaceTestPrepared(t *testing.T) (*workspaceRun, workspaceIntent) {
	t.Helper()
	dir := workspaceTestDirectory(t)
	source := filepath.Join(dir, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "input"), []byte("frozen input"), 0600); err != nil {
		t.Fatal(err)
	}
	intent := workspaceTestIntent(source)
	r, err := openWorkspaceRun(filepath.Join(dir, "journal"), intent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !r.closed {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	archive, _, err := snapshotWorkspace(source, r.dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.saveArchive(archive); err != nil {
		t.Fatal(err)
	}
	return r, intent
}
func workspaceTestResume(t *testing.T, r *workspaceRun) *workspaceRun {
	t.Helper()
	intent := workspaceIntent{Endpoint: r.intent.Endpoint, CredentialSHA256: r.intent.CredentialSHA256}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	resumed, err := openWorkspaceRun(r.dir, intent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := resumed.Close(); err != nil {
			t.Error(err)
		}
	})
	return resumed
}

func TestWorkspaceJournalFrozenResume(t *testing.T) {
	r, _ := workspaceTestPrepared(t)
	archive := bytes.Clone(r.archive)
	if err := os.RemoveAll(r.intent.Workdir); err != nil {
		t.Fatal(err)
	}
	resumed := workspaceTestResume(t, r)
	if !bytes.Equal(archive, resumed.archive) || resumed.intent.RequestID != "fixed" {
		t.Fatal("resume changed immutable input")
	}
	if err := resumed.saveArchive(append(archive, 'x')); err == nil {
		t.Fatal("replaced frozen archive")
	}
}

func TestWorkspaceJournalIntentMismatch(t *testing.T) {
	for _, field := range []string{"yaml", "credential", "endpoint", "workdir", "env", "binding", "request", "domain"} {
		t.Run(field, func(t *testing.T) {
			r, in := workspaceTestPrepared(t)
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "yaml":
				in.YAML = []byte("run: false\nworkdir: .\n")
			case "credential":
				in.CredentialSHA256 = strings.Repeat("b", 64)
			case "endpoint":
				in.Endpoint = "https://other.invalid"
			case "workdir":
				in.Workdir = filepath.Dir(in.Workdir)
			case "env":
				in.Env = []string{"A=B"}
			case "binding":
				in.Infra = "kubernetes"
				in.BindingTarget = "other"
			case "request":
				in.RequestID = "other"
			case "domain":
				in.RequestedDomain = "other"
			}
			failed, err := openWorkspaceRun(r.dir, in)
			if failed != nil {
				_ = failed.Close()
			}
			if err == nil {
				t.Fatal("accepted changed intent")
			}
		})
	}
}

func TestWorkspaceJournalPhaseBinding(t *testing.T) {
	r, _ := workspaceTestPrepared(t)
	next := r.state
	next.Phase = "begin"
	next.Domain = &v1.RequestDomain{Lineage: strings.Repeat("b", 32), Epoch: 1}
	if err := r.saveState(next); err != nil {
		t.Fatal(err)
	}
	// The durable copy must not alias the caller's mutable protobuf.
	next.Domain.Epoch = 2
	if r.state.Domain.Epoch != 1 {
		t.Fatal("state aliases caller")
	}
	next = r.state
	next.Phase = "upload"
	next.Upload = &v1.ArtifactRef{Id: strings.Repeat("c", 32), Sha256: r.state.ArchiveSHA256, Size: r.state.ArchiveSize}
	if err := r.saveState(next); err != nil {
		t.Fatal(err)
	}
	next = r.state
	next.Phase = "finalize"
	if err := r.saveState(next); err != nil {
		t.Fatal(err)
	}
	task, err := protojson.Marshal(&v1.Task{Argv: []string{"/bin/true"}, Workdir: r.state.Upload})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.saveTask(task); err != nil {
		t.Fatal(err)
	}
	if r.state.Phase != "sealed" || r.state.TaskSHA256 != workspaceHash(task) {
		t.Fatal("task publication is not sealed")
	}
	for _, change := range []string{"regress", "domain", "upload", "archive"} {
		next = r.state
		switch change {
		case "regress":
			next.Phase = "upload"
		case "domain":
			next.Domain = &v1.RequestDomain{Lineage: strings.Repeat("d", 32), Epoch: 1}
		case "upload":
			next.Upload = &v1.ArtifactRef{Id: strings.Repeat("d", 32), Sha256: next.ArchiveSHA256, Size: next.ArchiveSize}
		case "archive":
			next.ArchiveSHA256 = strings.Repeat("d", 64)
		}
		if err := r.saveState(next); err == nil {
			t.Fatalf("accepted %s", change)
		}
	}
	resumed := workspaceTestResume(t, r)
	if !bytes.Equal(task, resumed.task) || resumed.state.Phase != "sealed" {
		t.Fatal("lost exact sealed task")
	}
}

func TestWorkspaceJournalStrictFiles(t *testing.T) {
	for _, kind := range []string{"mode", "link", "symlink", "unknown", "staging", "oversize", "unknown-json", "duplicate-json", "missing-archive", "archive-change"} {
		t.Run(kind, func(t *testing.T) {
			r, _ := workspaceTestPrepared(t)
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(r.dir, "state.json")
			var err error
			switch kind {
			case "mode":
				err = os.Chmod(path, 0644)
			case "link":
				err = os.Link(path, filepath.Join(filepath.Dir(r.dir), "second"))
			case "symlink":
				err = os.Remove(path)
				if err == nil {
					err = os.Symlink("intent.json", path)
				}
			case "unknown":
				err = os.WriteFile(filepath.Join(r.dir, "extra"), nil, 0600)
			case "staging":
				err = os.WriteFile(filepath.Join(r.dir, "state.json.next"), []byte("partial"), 0600)
			case "oversize":
				f, e := os.OpenFile(path, os.O_WRONLY, 0600)
				err = e
				if e == nil {
					err = f.Truncate(workspaceStateLimit + 1)
					err = errors.Join(err, f.Close())
				}
			case "unknown-json":
				var b []byte
				b, err = os.ReadFile(path)
				if err == nil {
					b = append([]byte(`{"Unknown":1,`), b[1:]...)
					err = os.WriteFile(path, b, 0600)
				}
			case "duplicate-json":
				var b []byte
				b, err = os.ReadFile(path)
				if err == nil {
					b = append([]byte(`{"Version":1,`), b[1:]...)
					err = os.WriteFile(path, b, 0600)
				}
			case "missing-archive":
				err = os.Remove(filepath.Join(r.dir, "archive.tar"))
			case "archive-change":
				err = os.WriteFile(filepath.Join(r.dir, "archive.tar"), []byte("invalid"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(r.dir, "intent.json"))
			if err != nil {
				t.Fatal(err)
			}
			failed, err := openWorkspaceRun(r.dir, workspaceIntent{Endpoint: r.intent.Endpoint, CredentialSHA256: r.intent.CredentialSHA256})
			if failed != nil {
				_ = failed.Close()
			}
			if err == nil {
				t.Fatalf("accepted %s", kind)
			}
			after, err := os.ReadFile(filepath.Join(r.dir, "intent.json"))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("refusal changed immutable input")
			}
		})
	}
}

func TestWorkspaceJournalIncomplete(t *testing.T) {
	dir := workspaceTestDirectory(t)
	r, err := openWorkspaceRun(filepath.Join(dir, "journal"), workspaceTestIntent(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	failed, err := openWorkspaceRun(r.dir, workspaceIntent{Endpoint: r.intent.Endpoint, CredentialSHA256: r.intent.CredentialSHA256})
	if failed != nil {
		_ = failed.Close()
	}
	if err == nil {
		t.Fatal("resumed incomplete intent")
	}
}

func TestWorkspaceJournalExclusiveStageHolds(t *testing.T) {
	r, _ := workspaceTestPrepared(t)
	stage := filepath.Join(r.dir, "state.json.next")
	evidence := []byte("uncommitted")
	if err := os.WriteFile(stage, evidence, 0600); err != nil {
		t.Fatal(err)
	}
	next := r.state
	next.Phase = "begin"
	next.Domain = &v1.RequestDomain{Lineage: strings.Repeat("b", 32), Epoch: 1}
	if err := r.saveState(next); err == nil {
		t.Fatal("adopted existing staging")
	}
	if err := r.saveTask([]byte(`{}`)); err == nil {
		t.Fatal("continued after failed publication")
	}
	got, err := os.ReadFile(stage)
	if err != nil || !bytes.Equal(got, evidence) {
		t.Fatal("changed uncommitted staging evidence")
	}
	// Expected poison is retained on Close too; do not make the helper cleanup
	// classify this actual EEXIST refusal as an unexpected close failure.
	_ = r.Close()
}

func TestWorkspaceJSONStrict(t *testing.T) {
	for _, s := range []string{`{"Version":1,"Version":1}`, `{"version":1,"Version":1}`, `{"Verſion":1,"Version":1}`, `{"Version":1} {}`, `{"Unknown":1}`, `null`} {
		var out workspaceState
		err := decodeWorkspaceJSON([]byte(s), &out)
		if err == nil {
			t.Errorf("accepted %s", s)
		}
	}
	b, _ := json.Marshal(workspaceState{Version: 1})
	var out workspaceState
	if err := decodeWorkspaceJSON(b, &out); err != nil || out.Version != 1 {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{append([]byte(" "), b...), append(bytes.Clone(b), '\n'), bytes.Replace(b, []byte("Version"), []byte("Verſion"), 1)} {
		if err := decodeWorkspaceJSON(bad, &out); err == nil {
			t.Errorf("accepted noncanonical encoding")
		}
	}
}

func TestWorkspaceJournalExistingLockOnly(t *testing.T) {
	dir := workspaceTestDirectory(t)
	journal := filepath.Join(dir, "journal")
	if err := os.Mkdir(journal, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(journal, "lock"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	r, err := openWorkspaceRun(journal, workspaceTestIntent(dir))
	if r != nil {
		_ = r.Close()
	}
	if err == nil {
		t.Fatal("reinitialized released lock-only intent")
	}
	entries, err := os.ReadDir(journal)
	if err != nil || len(entries) != 1 || entries[0].Name() != "lock" {
		t.Fatal("incomplete intent changed", entries, err)
	}
}

func TestWorkspaceJournalImpossiblePhaseRecords(t *testing.T) {
	for _, kind := range []string{"prepared-domain", "prepared-upload", "begin-upload", "begin-task", "sealed-missing-task", "submit-job"} {
		t.Run(kind, func(t *testing.T) {
			r, _ := workspaceTestPrepared(t)
			next := r.state
			domain := &v1.RequestDomain{Lineage: strings.Repeat("b", 32), Epoch: 1}
			upload := &v1.ArtifactRef{Id: strings.Repeat("c", 32), Sha256: next.ArchiveSHA256, Size: next.ArchiveSize}
			switch kind {
			case "prepared-domain":
				next.Domain = domain
			case "prepared-upload":
				next.Upload = upload
			case "begin-upload":
				next.Phase = "begin"
				next.Domain = domain
				next.Upload = upload
			case "begin-task":
				next.Phase = "begin"
				next.Domain = domain
				next.TaskSHA256 = strings.Repeat("d", 64)
			case "sealed-missing-task":
				next.Phase = "sealed"
				next.Domain = domain
				next.Upload = upload
			case "submit-job":
				next.Phase = "submit"
				next.Domain = domain
				next.Upload = upload
				next.TaskSHA256 = strings.Repeat("d", 64)
				next.JobID = strings.Repeat("e", 32)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			checksum, err := workspaceStateChecksum(next)
			if err != nil {
				t.Fatal(err)
			}
			next.Checksum = checksum
			b, err := json.Marshal(next)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(r.dir, "state.json"), b, 0600); err != nil {
				t.Fatal(err)
			}
			failed, err := openWorkspaceRun(r.dir, workspaceIntent{Endpoint: r.intent.Endpoint, CredentialSHA256: r.intent.CredentialSHA256})
			if failed != nil {
				_ = failed.Close()
			}
			if err == nil {
				t.Fatal("accepted impossible phase record")
			}
		})
	}
}

func TestWorkspaceJournalIntegrityRefusals(t *testing.T) {
	for _, kind := range []string{"request-id", "requested-domain", "domain", "reference", "phase", "checksum", "malformed-checksum", "missing-checksum", "intent-digest", "missing-intent-digest"} {
		t.Run(kind, func(t *testing.T) {
			r, _ := workspaceTestPrepared(t)
			next := r.state
			next.Phase = "begin"
			next.Domain = &v1.RequestDomain{Lineage: strings.Repeat("b", 32), Epoch: 1}
			if err := r.saveState(next); err != nil {
				t.Fatal(err)
			}
			next = r.state
			next.Phase = "upload"
			next.Upload = &v1.ArtifactRef{Id: strings.Repeat("c", 32), Sha256: next.ArchiveSHA256, Size: next.ArchiveSize}
			if err := r.saveState(next); err != nil {
				t.Fatal(err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(r.dir, "state.json")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var state workspaceState
			if err := decodeWorkspaceJSON(data, &state); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "request-id", "requested-domain":
				path = filepath.Join(r.dir, "intent.json")
				data, err = os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var intent workspaceIntent
				if err := decodeWorkspaceJSON(data, &intent); err != nil {
					t.Fatal(err)
				}
				if kind == "request-id" {
					intent.RequestID = "another-valid-id"
				} else {
					intent.RequestedDomain = strings.Repeat("b", 32) + ":1"
				}
				data, err = json.Marshal(intent)
			default:
				switch kind {
				case "domain":
					state.Domain.Epoch = 2
				case "reference":
					state.Upload.Id = strings.Repeat("d", 32)
				case "phase":
					state.Phase = "finalize"
				case "checksum":
					state.Checksum = strings.Repeat("f", 64)
				case "malformed-checksum":
					state.Checksum = "bad"
				case "missing-checksum":
					state.Checksum = ""
				case "intent-digest":
					state.IntentSHA256 = strings.Repeat("f", 64)
				case "missing-intent-digest":
					state.IntentSHA256 = ""
				}
				// Deliberately retain the old checksum: a valid-character payload change
				// must be detected without relying on a field's semantic invalidity.
				data, err = json.Marshal(state)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			before := workspaceTestFiles(t, r.dir)
			failed, err := openWorkspaceRun(r.dir, workspaceIntent{Endpoint: r.intent.Endpoint, CredentialSHA256: r.intent.CredentialSHA256})
			if failed != nil {
				_ = failed.Close()
			}
			if err == nil {
				t.Fatal("accepted integrity corruption")
			}
			after := workspaceTestFiles(t, r.dir)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("integrity refusal modified journal")
			}
		})
	}
}

func workspaceTestFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := make(map[string]string, len(entries))
	for _, entry := range entries {
		b, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[entry.Name()] = workspaceHash(b)
	}
	return files
}

func TestWorkspaceJournalIntentDigestImmutable(t *testing.T) {
	r, _ := workspaceTestPrepared(t)
	before := workspaceTestFiles(t, r.dir)
	r.intent.RequestID = "changed-in-memory"
	next := r.state
	next.Phase = "begin"
	next.Domain = &v1.RequestDomain{Lineage: strings.Repeat("b", 32), Epoch: 1}
	if err := r.saveState(next); err == nil {
		t.Fatal("rebound state to changed intent")
	}
	if !reflect.DeepEqual(before, workspaceTestFiles(t, r.dir)) {
		t.Fatal("refused save modified journal")
	}
}

// MODELED directory-Sync failure, with actual mkdir/reopen/publication paths.
// The test never treats a failed injected call as a durability receipt.
func TestWorkspaceJournalFreshRetryRequiresParentSync(t *testing.T) {
	dir := workspaceTestDirectory(t)
	journal := filepath.Join(dir, "intent")
	intent := workspaceTestIntent(dir)
	failure := errors.New("modeled parent sync failure")
	for range 2 {
		calls := 0
		r, err := openWorkspaceRunWithSync(journal, intent, func(parent string) error {
			calls++
			if parent != dir {
				t.Fatal("sync escaped selected managed chain")
			}
			entries, err := os.ReadDir(journal)
			if err != nil || len(entries) != 0 {
				t.Fatal("published before parent Sync", entries, err)
			}
			return failure
		})
		if r != nil {
			_ = r.Close()
		}
		if !errors.Is(err, failure) || calls != 1 {
			t.Fatal("fresh attempt skipped failed parent Sync", calls, err)
		}
		entries, err := os.ReadDir(journal)
		if err != nil || len(entries) != 0 {
			t.Fatal("failed Sync seeded intent", entries, err)
		}
	}
	durable := false
	r, err := openWorkspaceRunWithSync(journal, intent, func(parent string) error {
		if durable {
			t.Fatal("unexpected second selected parent")
		}
		entries, err := os.ReadDir(journal)
		if err != nil || len(entries) != 0 {
			t.Fatal("publication preceded positive Sync", entries, err)
		}
		if err := syncWorkspaceDirectory(parent); err != nil {
			return err
		}
		durable = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !durable || r.intent.RequestID != intent.RequestID {
		t.Fatal("intent published without positive durability or changed identity")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceJournalRefusesMissingOuterAnchor(t *testing.T) {
	dir := workspaceTestDirectory(t)
	parent := filepath.Join(dir, "missing-parent")
	r, err := openWorkspaceRun(filepath.Join(parent, "intent"), workspaceTestIntent(dir))
	if r != nil {
		_ = r.Close()
	}
	if err == nil {
		t.Fatal("created an ancestor outside the managed boundary")
	}
	if _, err := os.Lstat(parent); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refusal created outer ancestor", err)
	}
}

func TestWorkspaceJournalSyncsSelectedManagedChain(t *testing.T) {
	dir := workspaceTestDirectory(t)
	t.Setenv("HOME", dir)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(dir, "cache"))
	cache, err := os.UserCacheDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cache, 0700); err != nil {
		t.Fatal(err)
	}
	var synced []string
	r, err := openWorkspaceRunWithSync("", workspaceTestIntent(dir), func(parent string) error {
		synced = append(synced, parent)
		return syncWorkspaceDirectory(parent)
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Dir(cache), cache, filepath.Join(cache, "skygo"), filepath.Join(cache, "skygo", "workspaces")}
	if !reflect.DeepEqual(synced, want) {
		t.Fatal("wrong durability scope", synced, want)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func workspaceTestMissingCache(t *testing.T) (string, workspaceIntent) {
	t.Helper()
	dir := workspaceTestDirectory(t)
	t.Setenv("HOME", dir)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(dir, "cache"))
	cache, err := os.UserCacheDir()
	if err != nil {
		t.Fatal(err)
	}
	// Darwin's cache parent is Library; Linux's is the fixture root itself.
	if err := os.MkdirAll(filepath.Dir(cache), 0700); err != nil {
		t.Fatal(err)
	}
	return cache, workspaceTestIntent(dir)
}

func TestWorkspaceJournalDefaultFirstUse(t *testing.T) {
	cache, in := workspaceTestMissingCache(t)
	if _, err := os.Lstat(cache); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("fixture cache unexpectedly exists", err)
	}
	r, err := openWorkspaceRun("", in)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(cache)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatal("new cache is not private", err)
	}
	// A preexisting secure0755 cache remains valid; managed children stay0700.
	if err := os.Chmod(cache, 0755); err != nil {
		t.Fatal(err)
	}
	in.RequestID = "next-intent"
	r, err = openWorkspaceRun("", in)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	info, err = os.Stat(filepath.Join(cache, "skygo"))
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatal("managed cache child is not private", err)
	}
}

// MODELED cache-parent Sync failure across fresh invocation owners. No failed
// injected call is credited as a receipt; the successful attempt Syncs real dirs.
func TestWorkspaceJournalCacheParentRetry(t *testing.T) {
	cache, in := workspaceTestMissingCache(t)
	failure := errors.New("modeled cache-parent sync failure")
	for range 2 {
		calls := 0
		r, err := openWorkspaceRunWithSync("", in, func(parent string) error {
			calls++
			if parent != filepath.Dir(cache) {
				t.Fatal("cache parent was not first")
			}
			entries, err := os.ReadDir(cache)
			if err != nil || len(entries) != 0 {
				t.Fatal("seeded managed identity before cache durability", entries, err)
			}
			return failure
		})
		if r != nil {
			_ = r.Close()
		}
		if !errors.Is(err, failure) || calls != 1 {
			t.Fatal("fresh retry skipped cache-parent durability", calls, err)
		}
	}
	var parents []string
	r, err := openWorkspaceRunWithSync("", in, func(parent string) error {
		matches, err := filepath.Glob(filepath.Join(cache, "skygo", "workspaces", "*", "intent.json"))
		if err != nil || len(matches) != 0 {
			t.Fatal("published identity before positive ancestor Syncs", matches, err)
		}
		if err := syncWorkspaceDirectory(parent); err != nil {
			return err
		}
		parents = append(parents, parent)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != 4 || parents[0] != filepath.Dir(cache) || r.intent.RequestID != in.RequestID {
		t.Fatal("durability chain incomplete or identity changed", parents)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceJournalDefaultRefusesMissingCacheParents(t *testing.T) {
	cache, in := workspaceTestMissingCache(t)
	parent := filepath.Join(filepath.Dir(cache), "missing", "deep")
	t.Setenv("HOME", parent)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(parent, "cache"))
	r, err := openWorkspaceRun("", in)
	if r != nil {
		_ = r.Close()
	}
	if err == nil {
		t.Fatal("recursively created missing outer cache parents")
	}
	if _, err := os.Lstat(filepath.Dir(parent)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("created an outer cache ancestor", err)
	}
}
