//go:build linux || darwin

package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// installationEntry is one path's mode, device, inode, size and content digest.
type installationEntry struct {
	mode          fs.FileMode
	device, inode uint64
	size          int64
	sum           [32]byte
}

// installationTree records every path below root: state, auth file, artifact
// root and bystanders, so a refusal is shown to leave all of them exact.
func installationTree(t *testing.T, root string) map[string]installationEntry {
	t.Helper()
	out := make(map[string]installationEntry)
	err := filepath.WalkDir(root, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		st := info.Sys().(*syscall.Stat_t)
		e := installationEntry{mode: info.Mode(), device: uint64(st.Dev), inode: uint64(st.Ino), size: info.Size()}
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			e.sum = sha256.Sum256(data)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out[rel] = e
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// corruptMarkerField is a MODELED corruption, not a genuine historical writer:
// it rewrites one field of the root marker written by the actual initializer,
// in place and keeping the canonical field order, so only the actual
// artifact.Open/RootFacts parser and the server comparator judge it.
func corruptMarkerField(t *testing.T, root, key string, next func(old string) string) {
	t.Helper()
	marker := filepath.Join(root, ".skygo-artifact-root")
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	old, ok := fields[key]
	if !ok {
		t.Fatalf("marker has no %q field", key)
	}
	from := fmt.Sprintf("%q:%s", key, old)
	to := fmt.Sprintf("%q:%s", key, next(string(old)))
	if strings.Count(string(data), from) != 1 || from == to {
		t.Fatalf("marker field %s is not uniquely replaceable", from)
	}
	if err := os.WriteFile(marker, []byte(strings.Replace(string(data), from, to, 1)), 0600); err != nil {
		t.Fatal(err)
	}
}

// bumpMarkerNumber returns a different decimal marker value.
func bumpMarkerNumber(t *testing.T, double bool) func(string) string {
	return func(old string) string {
		n, err := strconv.ParseUint(old, 10, 64)
		if err != nil || n == 0 {
			t.Fatalf("marker number %q", old)
		}
		if double {
			return strconv.FormatUint(2*n, 10)
		}
		return strconv.FormatUint(n+1, 10)
	}
}

type existingServer struct {
	*lifetimeServerFixture
	cfg            Config
	owner, token   string
	installs, adds int
}

// existingServerFixture writes an actual installation with the ordinary
// Store, auth and artifact initializers: installed artifacts, an auth file
// bound to the owner and, if scoped, durably activated scoped auth.
func existingServerFixture(t *testing.T, scoped bool) *existingServer {
	t.Helper()
	f := &existingServer{lifetimeServerFixture: lifetimeFixture(t)}
	root := filepath.Join(f.directory, "artifacts")
	authDir := filepath.Join(f.directory, "auth")
	for _, name := range []string{root, authDir} {
		if err := os.Mkdir(name, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := InitializeArtifacts(f.data, root); err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	s, err := store.Open(f.data)
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	f.opened = s
	f.owner = s.Owner()
	authFile, tokenFile := filepath.Join(authDir, "auth.json"), filepath.Join(authDir, "legacy.token")
	if _, err := auth.Initialize(authFile, tokenFile, f.owner); err != nil {
		t.Fatal(err)
	}
	if scoped {
		if err := s.ActivateScoped(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	f.opened = nil
	raw, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	f.token = strings.TrimSpace(string(raw))
	if err := os.WriteFile(filepath.Join(f.directory, "bystander"), []byte("bystander\n"), 0600); err != nil {
		t.Fatal(err)
	}
	f.cfg = Config{DataDir: f.data, Listen: "127.0.0.1:0", AuthFile: authFile, ArtifactDir: root, ExistingInstallation: true}
	f.cfg.Install = func(*store.Store, *auth.Engine) (Integration, error) { f.installs++; return nil, nil }
	f.cfg.Targets = func(string) ([]backend.Target, error) { f.adds++; return nil, nil }
	return f
}

func TestServerExistingInstallationStarts(t *testing.T) {
	for _, existing := range []bool{true, false} {
		t.Run(fmt.Sprint("existing=", existing), func(t *testing.T) {
			f := existingServerFixture(t, true)
			f.cfg.ExistingInstallation = existing
			frame := &serverRun{}
			f.frame = frame
			if err := frame.start(context.Background(), f.cfg); err != nil {
				f.hold(t, err)
				t.Fatal(err)
			}
			if frame.store.Owner() != f.owner || !frame.store.Scoped() {
				t.Fatal("started a different installation")
			}
			// The same auth file Engine, bound to the installation owner,
			// authenticates; the same root and Files configure the workspace.
			actor, err := frame.engine.Authenticate("Bearer " + f.token)
			if err != nil || actor.Principal() != "legacy" {
				t.Fatalf("Authenticate = %q, %v", actor.Principal(), err)
			}
			if frame.root == nil || frame.files == nil || !frame.ownedFiles || frame.localBackend == nil {
				t.Fatal("artifact root, Files or local workspace backend not retained")
			}
			if f.installs != 1 || f.adds != 1 {
				t.Fatalf("callbacks: install %d, targets %d", f.installs, f.adds)
			}
			if err := frame.shutdown(nil); err != nil {
				f.hold(t, err)
				t.Fatal(err)
			}
			f.frame = nil
			if _, err := store.InspectExisting(f.data); err != nil {
				t.Fatalf("installation no longer current after serving: %v", err)
			}
		})
	}
}

func TestServerExistingInstallationRefuses(t *testing.T) {
	for _, tt := range []struct {
		name   string
		scoped bool
		setup  func(t *testing.T, f *existingServer)
		want   error
	}{
		{"wrong auth owner", true, func(t *testing.T, f *existingServer) {
			dir := filepath.Join(f.directory, "other-auth")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			f.cfg.AuthFile = filepath.Join(dir, "auth.json")
			if _, err := auth.Initialize(f.cfg.AuthFile, filepath.Join(dir, "legacy.token"), strings.Repeat("ab", 16)); err != nil {
				t.Fatal(err)
			}
		}, auth.ErrInvalid},
		{"other installed root", true, func(t *testing.T, f *existingServer) {
			other := filepath.Join(f.directory, "other-root")
			if err := os.Mkdir(other, 0700); err != nil {
				t.Fatal(err)
			}
			if err := InitializeArtifacts(filepath.Join(f.directory, "other-state"), other); err != nil {
				f.hold(t, err)
				t.Fatal(err)
			}
			f.cfg.ArtifactDir = other
		}, artifact.ErrFileConflict},
		// The marker cases are MODELED corruptions of the actual initializer's
		// marker, one field at a time. A well-formed foreign ID passes the
		// parser and is refused by the comparator against the Store's tuple;
		// unit and volume are refused by RootFacts against the live root.
		{"wrong marker id", true, func(t *testing.T, f *existingServer) {
			corruptMarkerField(t, f.cfg.ArtifactDir, "id", func(old string) string {
				id := strings.Repeat("ab", 16)
				if old == strconv.Quote(id) {
					id = strings.Repeat("cd", 16)
				}
				return strconv.Quote(id)
			})
		}, artifact.ErrFileConflict},
		{"wrong marker unit", true, func(t *testing.T, f *existingServer) {
			corruptMarkerField(t, f.cfg.ArtifactDir, "unit", bumpMarkerNumber(t, true))
		}, artifact.ErrFileConflict},
		{"wrong marker volume", true, func(t *testing.T, f *existingServer) {
			corruptMarkerField(t, f.cfg.ArtifactDir, "volume", bumpMarkerNumber(t, false))
		}, artifact.ErrFileConflict},
		{"missing marker", true, func(t *testing.T, f *existingServer) {
			empty := filepath.Join(f.directory, "empty-root")
			if err := os.Mkdir(empty, 0700); err != nil {
				t.Fatal(err)
			}
			f.cfg.ArtifactDir = empty
		}, nil},
		{"nondirectory root", true, func(t *testing.T, f *existingServer) {
			file := filepath.Join(f.directory, "not-a-root")
			if err := os.WriteFile(file, nil, 0600); err != nil {
				t.Fatal(err)
			}
			f.cfg.ArtifactDir = file
		}, artifact.ErrFileConflict},
		{"unscoped state", false, nil, store.ErrNotCurrent},
		{"missing state", true, func(t *testing.T, f *existingServer) {
			f.cfg.DataDir = filepath.Join(f.directory, "absent")
		}, fs.ErrNotExist},
		{"borrowed files", true, func(t *testing.T, f *existingServer) {
			f.cfg.Artifacts = new(artifact.Files)
		}, nil},
		{"reader recovery", true, func(t *testing.T, f *existingServer) {
			f.cfg.RecoverStoppedArtifactReaders = true
		}, nil},
		{"no auth file", true, func(t *testing.T, f *existingServer) {
			f.cfg.AuthFile = ""
		}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := existingServerFixture(t, tt.scoped)
			if tt.setup != nil {
				tt.setup(t, f)
			}
			before := installationTree(t, f.directory)
			frame := &serverRun{}
			f.frame = frame
			startErr := frame.start(context.Background(), f.cfg)
			if startErr == nil {
				t.Fatal("started a refused installation")
			}
			if tt.want != nil && !errors.Is(startErr, tt.want) {
				t.Errorf("start = %v, want %v", startErr, tt.want)
			}
			if frame.listener != nil || frame.runDone != nil || f.installs != 0 || f.adds != 0 {
				f.hold(t, startErr)
				t.Fatal("refusal reached callbacks or serving")
			}
			// Every refusal here precedes store.OpenExisting: no writer.
			if frame.store != nil {
				f.hold(t, startErr)
				t.Fatal("refusal acquired the installation writer")
			}
			result := frame.closeStartup(startErr)
			var held *RunError
			if errors.As(result, &held) {
				f.hold(t, result)
				t.Fatal("ordinary refusal held its startup frame")
			}
			if !errors.Is(result, startErr) || frame.store != nil || frame.root != nil || frame.files != nil {
				f.hold(t, result)
				t.Fatal("refusal lost its cause or kept resources")
			}
			f.frame = nil
			if !maps.Equal(before, installationTree(t, f.directory)) {
				t.Fatal("refusal changed state, auth, artifacts or a bystander")
			}
		})
	}
}

// TestServerStartupHoldsCloseUncertainty checks closeStartup's classification
// of the Store and auth close sentinels. The causes are MODELED: they wrap the
// sentinels, while the actual typed errors are exercised by the store and
// auth tests. The held frame keeps its real Store and root, as a HOLD must.
func TestServerStartupHoldsCloseUncertainty(t *testing.T) {
	for _, sentinel := range []error{store.ErrCloseUncertain, auth.ErrCloseUncertain} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			f := lifetimeFixture(t)
			s, err := store.Open(f.data)
			if err != nil {
				f.hold(t, err)
				t.Fatal(err)
			}
			f.opened = s
			root, err := os.OpenRoot(f.directory)
			if err != nil {
				f.hold(t, err)
				t.Fatal(err)
			}
			frame := &serverRun{store: s, root: root}
			f.frame = frame
			result := frame.closeStartup(fmt.Errorf("modeled startup: %w", sentinel))
			var held *RunError
			if !errors.As(result, &held) || held.frame != frame || !errors.Is(result, sentinel) {
				f.hold(t, result)
				t.Fatal("close uncertainty was not held", result)
			}
			if frame.store != s || frame.root != root {
				f.hold(t, result)
				t.Fatal("held frame cleared its originals")
			}
			if _, err := s.ArtifactInstallation(); err != nil {
				f.hold(t, result)
				t.Fatal("held frame closed its Store", err)
			}
			if _, err := root.Lstat("."); err != nil {
				f.hold(t, result)
				t.Fatal("held frame closed its root", err)
			}
			// Keep the real originals until process exit; nothing reopens or
			// removes them.
			f.expectedHeld = sentinel
			f.hold(t, result)
		})
	}
}
