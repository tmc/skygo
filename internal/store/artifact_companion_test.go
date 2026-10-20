//go:build linux || darwin

package store

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	bolt "go.etcd.io/bbolt"
)

// These tests create real filesystem owners and archive receipts. They do not
// model an execution, native shutdown, workspace completion or cleanup witness.
type companionFixture struct {
	data, artifacts, owner string
	ref                    ArtifactRef
	object                 artifactObject
	archive                []byte
	s                      *Store
	root                   *os.Root
	files                  *artifact.Files
	closed                 bool
	closeErr               error
	constructorErr         error
	held                   error
	keeping                bool
	operations             []error
	restored               *Store
	restoredCloseAttempted bool
	lockedDB               *bolt.DB
	lockCloseAttempted     bool
}

// hold keeps this same fixture and its original returned errors reachable until
// process exit. It is custody only, never a Close retry or native join proof.
func (f *companionFixture) hold(err error) {
	f.held = errors.Join(f.held, err)
	if !f.keeping {
		f.keeping = true
		wait := make(chan struct{})
		go func() { <-wait; runtime.KeepAlive(f) }()
	}
}

func (f *companionFixture) fatal(t *testing.T, err error) {
	t.Helper()
	f.hold(err)
	t.Fatal(err)
}

func (f *companionFixture) operation(err error) error {
	if err == nil {
		return nil
	}
	f.operations = append(f.operations, err)
	var failure *companionError
	if errors.As(err, &failure) {
		o := failure.operation
		// A retained original slot is not proof its OS descriptor survived.
		// Refusals with every slot positively settled may clean their fixture.
		if o.published || o.stagePath != "" || o.root != nil || o.newRoot != nil || o.parentRoot != nil || o.stageRoot != nil || o.files != nil || o.newFiles != nil || o.reader != nil || o.sourceDB.file != nil || o.stageDB.file != nil || o.parent.file != nil || o.data.file != nil || o.sourceObjects.file != nil || o.sourceLogs.file != nil || o.stage.file != nil || o.objects.file != nil || o.logs.file != nil || o.input.file != nil || o.output.file != nil {
			f.hold(err)
		}
	}
	if errors.Is(err, artifact.ErrFileCloseUncertain) {
		f.hold(err)
	}
	return err
}

func (f *companionFixture) closeRestored() error {
	if f.restored == nil || f.restoredCloseAttempted {
		return f.closeErr
	}
	f.restoredCloseAttempted = true
	if err := f.restored.Close(); err != nil {
		f.closeErr = errors.Join(f.closeErr, err)
		f.hold(err) // Keep the exact restored Store, not only its cause.
		return err
	}
	f.restored = nil
	return nil
}

func (f *companionFixture) stop() error {
	if f.closed {
		return f.closeErr
	}
	f.closed = true // Each original Close is attempted once, including on error.
	err := f.constructorErr
	if f.files != nil {
		err = errors.Join(err, f.files.Close())
	}
	// Files borrows Root; do not close its parent after an uncertain child Close.
	if err == nil && f.root != nil {
		err = f.root.Close()
	}
	if f.s != nil {
		err = errors.Join(err, f.s.Close())
	}
	f.closeErr = err
	if err != nil {
		f.hold(err)
	}
	return err
}

func newCompanionFixture(t *testing.T, upload bool) *companionFixture {
	t.Helper()
	base, err := companionExampleBase()
	if err != nil {
		t.Fatal(err)
	}
	f := &companionFixture{data: filepath.Join(base, "state"), artifacts: filepath.Join(base, "artifacts")}
	t.Cleanup(func() {
		if f.lockedDB != nil && !f.lockCloseAttempted {
			f.lockCloseAttempted = true
			if err := f.lockedDB.Close(); err != nil {
				f.hold(err)
				t.Errorf("close original lock owner: %v", err)
			} else {
				f.lockedDB = nil
			}
		}
		if err := f.stop(); err != nil {
			t.Errorf("close fixture (retaining %s): %v", base, err)
			return
		}
		if t.Failed() || f.keeping {
			t.Logf("retaining failed companion fixture: %s", base)
			return
		}
		if err := os.RemoveAll(base); err != nil {
			t.Error(err)
		}
	})
	if err := os.Mkdir(f.artifacts, 0700); err != nil {
		f.fatal(t, err)
	}
	f.root, err = os.OpenRoot(f.artifacts)
	if err != nil {
		f.fatal(t, err)
	}
	info, err := f.root.Stat(".")
	if err != nil {
		f.fatal(t, err)
	}
	_, _, id, err := companionStat(info)
	if err != nil {
		f.fatal(t, err)
	}
	rootID := artifact.FileID{Device: id.Device, Inode: id.Inode}
	facts, err := artifact.Initialize(f.root, rootID, strings.Repeat("c", 32))
	if errors.Is(err, artifact.ErrFileCloseUncertain) {
		f.constructorErr = err
	}
	if err != nil {
		f.fatal(t, err)
	}
	f.files, err = artifact.Open(f.root, rootID)
	if errors.Is(err, artifact.ErrFileCloseUncertain) {
		f.constructorErr = err
	}
	if err != nil {
		f.fatal(t, err)
	}
	f.s, err = Open(f.data)
	if err != nil {
		f.fatal(t, err)
	}
	f.owner = f.s.Owner()
	if _, err := f.s.InstallArtifacts(ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}); err != nil {
		f.fatal(t, err)
	}
	if upload {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		body := []byte("companion preserves exact archive bytes\n")
		if err := tw.WriteHeader(&tar.Header{Name: "input.txt", Mode: 0600, Size: int64(len(body)), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR, ModTime: time.Unix(0, 0).UTC()}); err != nil {
			f.fatal(t, err)
		}
		_, writeErr := tw.Write(body)
		if err := errors.Join(writeErr, tw.Close()); err != nil {
			f.fatal(t, err)
		}
		f.archive = buf.Bytes()
		hash := sha256.Sum256(f.archive)
		actor := testActor()
		u, err := f.s.BeginArtifactUpload(actor, hex.EncodeToString(hash[:]), uint64(len(f.archive)))
		if err != nil {
			f.fatal(t, err)
		}
		ref := artifact.Ref{ID: u.Reference.ID, SHA256: u.Reference.SHA256, Size: int64(u.Reference.Size)}
		part, err := f.files.CreatePart(ref)
		if err != nil {
			f.fatal(t, err)
		}
		file := ArtifactFile{Device: part.Device, Inode: part.Inode}
		u, err = f.s.ArtifactPartCreated(actor, ArtifactCursor{u.Reference.ID, u.Revision}, file)
		if err != nil {
			f.fatal(t, err)
		}
		if _, err := f.files.WritePart(ref, part, 0, 0, f.archive); err != nil {
			f.fatal(t, err)
		}
		u, err = f.s.ArtifactChunkSynced(actor, ArtifactCursor{u.Reference.ID, u.Revision}, file, 0, uint64(len(f.archive)), hex.EncodeToString(hash[:]))
		if err != nil {
			f.fatal(t, err)
		}
		u, err = f.s.BeginArtifactFinalize(actor, ArtifactCursor{u.Reference.ID, u.Revision})
		if err != nil {
			f.fatal(t, err)
		}
		// The tiny real write is covered by the test host's observed space;
		// this is not a live Control physical-cache receipt.
		receipt, err := f.files.SealPart(ref, part, func() error {
			free, err := filesystemFree(f.artifacts)
			if err == nil && free < 2<<30 {
				return errors.New("insufficient fixture publication space")
			}
			return err
		})
		if err != nil {
			f.fatal(t, err)
		}
		manifest := ArtifactManifest{Reference: u.Reference, ExpandedBytes: receipt.Manifest.ExpandedBytes, RegularFiles: receipt.Manifest.RegularFiles, Directories: receipt.Manifest.Directories}
		file = ArtifactFile{Device: receipt.File.Device, Inode: receipt.File.Inode}
		f.ref, err = f.s.CompleteArtifactUpload(actor, ArtifactCursor{u.Reference.ID, u.Revision}, u.Reference, file, manifest)
		if err != nil {
			f.fatal(t, err)
		}
		if err := f.s.db.View(func(tx *bolt.Tx) error {
			st, err := artifactLoad(tx)
			if err == nil {
				f.object = st.Objects[f.ref.ID]
			}
			return err
		}); err != nil {
			f.fatal(t, err)
		}
	}
	if err := f.stop(); err != nil {
		f.fatal(t, err)
	}
	return f
}

type companionObservedFile struct {
	info  os.FileInfo
	bytes []byte
}

func observeCompanionSource(t *testing.T, f *companionFixture) map[string]companionObservedFile {
	t.Helper()
	return observeCompanionPaths(t, f.data, f.artifacts)
}

func observeCompanionPaths(t *testing.T, dirs ...string) map[string]companionObservedFile {
	t.Helper()
	out := make(map[string]companionObservedFile)
	for _, dir := range dirs {
		if err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			var data []byte
			if info.Mode().IsRegular() {
				data, err = os.ReadFile(path)
			}
			out[path] = companionObservedFile{info, data}
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func unchangedCompanionSource(t *testing.T, f *companionFixture, before map[string]companionObservedFile) {
	t.Helper()
	unchangedCompanionPaths(t, before, observeCompanionSource(t, f))
}

func unchangedCompanionPaths(t *testing.T, before, after map[string]companionObservedFile) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("source membership changed: %d -> %d", len(before), len(after))
	}
	for path, old := range before {
		now, ok := after[path]
		if !ok || !os.SameFile(old.info, now.info) || old.info.Mode() != now.info.Mode() || !bytes.Equal(old.bytes, now.bytes) {
			t.Errorf("source changed: %s", path)
		}
	}
}

func TestCompanionRoundTrip(t *testing.T) {
	for _, upload := range []bool{false, true} {
		t.Run(map[bool]string{false: "initialized-empty", true: "unconsumed-upload"}[upload], func(t *testing.T) {
			f := newCompanionFixture(t, upload)
			before := observeCompanionSource(t, f)
			base := filepath.Dir(f.data)
			bundle, restored := filepath.Join(base, "bundle"), filepath.Join(base, "restored")
			if err := f.operation(BackupCompanion(context.Background(), f.data, f.artifacts, bundle)); err != nil {
				f.fatal(t, err)
			}
			if err := f.operation(RestoreCompanion(context.Background(), bundle, restored)); err != nil {
				f.fatal(t, err)
			}
			unchangedCompanionSource(t, f, before)
			s, err := Open(restored)
			if err != nil {
				f.fatal(t, err)
			}
			f.restored = s
			defer func() {
				if err := f.closeRestored(); err != nil {
					t.Error(err)
				}
			}()
			if s.Owner() != f.owner {
				t.Fatal("persistent owner changed")
			}
			if required, err := s.ArtifactRequiredObjects(); err != nil || len(required) != 0 {
				t.Fatalf("unconsumed upload became required: %v, %v", required, err)
			}
			if err := s.db.View(func(tx *bolt.Tx) error {
				st, err := artifactLoad(tx)
				if err != nil {
					return err
				}
				if st.Root.ID != strings.Repeat("c", 32) || len(st.Objects) != map[bool]int{false: 0, true: 1}[upload] {
					t.Fatal("installation or object membership changed")
				}
				if upload {
					o := st.Objects[f.ref.ID]
					old := f.object
					if o.File == old.File {
						t.Fatal("restored object retained source inode identity")
					}
					old.File = o.File
					if o != old || st.Uploads[f.ref.ID].Part != o.File {
						t.Fatal("object history changed or complete upload alias was not rebound")
					}
				}
				return nil
			}); err != nil {
				f.fatal(t, err)
			}
			if upload {
				path := filepath.Join(restored, "artifacts", f.ref.ID+".tar")
				got, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(got, f.archive) {
					t.Fatalf("archive roundtrip: %v", err)
				}
				for _, dir := range []string{bundle, restored, filepath.Join(bundle, "objects"), filepath.Join(restored, "artifacts")} {
					info, err := os.Stat(dir)
					if err != nil || info.Mode().Perm() != 0700 {
						t.Fatalf("private directory %s: %v", dir, err)
					}
				}
				for _, name := range []string{filepath.Join(bundle, "objects", f.ref.ID+".tar"), path} {
					info, err := os.Stat(name)
					if err != nil || info.Mode().Perm() != 0600 || os.SameFile(before[filepath.Join(f.artifacts, f.ref.ID+".tar")].info, info) {
						t.Fatalf("private independent object %s: %v", name, err)
					}
				}
			}
		})
	}
}

func TestCompanionRestoreRefusesDamagedBundle(t *testing.T) {
	for _, damage := range []string{"archive-bytes", "database-bytes", "manifest-bytes", "extra-entry", "extra-object", "missing-object", "symlink-object", "symlink-database", "symlink-manifest", "symlink-objects"} {
		t.Run(damage, func(t *testing.T) {
			f := newCompanionFixture(t, true)
			before := observeCompanionSource(t, f)
			base := filepath.Dir(f.data)
			bundle, restored := filepath.Join(base, "bundle"), filepath.Join(base, "restored")
			if err := f.operation(BackupCompanion(context.Background(), f.data, f.artifacts, bundle)); err != nil {
				f.fatal(t, err)
			}
			object := filepath.Join(bundle, "objects", f.ref.ID+".tar")
			var err error
			switch damage {
			case "archive-bytes":
				data := bytes.Clone(f.archive)
				data[512] ^= 1
				err = os.WriteFile(object, data, 0600)
			case "database-bytes", "manifest-bytes":
				name := "jobs.db"
				if damage == "manifest-bytes" {
					name = "manifest.json"
				}
				err = os.WriteFile(filepath.Join(bundle, name), []byte("invalid companion\n"), 0600)
			case "extra-entry":
				err = os.WriteFile(filepath.Join(bundle, "unknown"), []byte("keep"), 0600)
			case "extra-object":
				err = os.WriteFile(filepath.Join(bundle, "objects", strings.Repeat("f", 32)+".tar"), f.archive, 0600)
			case "missing-object":
				err = os.Remove(object)
			default:
				path, target := object, filepath.Join(f.artifacts, f.ref.ID+".tar")
				switch damage {
				case "symlink-database":
					path, target = filepath.Join(bundle, "jobs.db"), filepath.Join(f.data, "jobs.db")
				case "symlink-manifest":
					path, target = filepath.Join(bundle, "manifest.json"), filepath.Join(base, "saved-manifest")
				case "symlink-objects":
					path, target = filepath.Join(bundle, "objects"), filepath.Join(base, "saved-objects")
				}
				if damage == "symlink-manifest" || damage == "symlink-objects" {
					err = os.Rename(path, target)
				} else {
					err = os.Remove(path)
				}
				if err == nil {
					err = os.Symlink(target, path)
				}
			}
			if err != nil {
				f.fatal(t, err)
			}
			if err := f.operation(RestoreCompanion(context.Background(), bundle, restored)); err == nil {
				t.Fatal("damaged bundle restored")
			}
			if _, err := os.Lstat(restored); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed restore published destination: %v", err)
			}
			unchangedCompanionSource(t, f, before)
		})
	}
}

func TestCompanionNoReplaceAndCanceled(t *testing.T) {
	f := newCompanionFixture(t, false)
	before := observeCompanionSource(t, f)
	base := filepath.Dir(f.data)
	bundle := filepath.Join(base, "bundle")
	if err := f.operation(BackupCompanion(context.Background(), f.data, f.artifacts, bundle)); err != nil {
		f.fatal(t, err)
	}
	for _, restore := range []bool{false, true} {
		for _, existing := range []bool{false, true} {
			name := "backup"
			if restore {
				name = "restore"
			}
			if existing {
				name += "-existing"
			} else {
				name += "-canceled"
			}
			t.Run(name, func(t *testing.T) {
				destination := filepath.Join(base, name)
				ctx := context.Background()
				if existing {
					if err := os.Mkdir(destination, 0700); err != nil {
						f.fatal(t, err)
					}
					if err := os.WriteFile(filepath.Join(destination, "sentinel"), []byte("keep"), 0600); err != nil {
						f.fatal(t, err)
					}
				} else {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				var err error
				if restore {
					err = f.operation(RestoreCompanion(ctx, bundle, destination))
				} else {
					err = f.operation(BackupCompanion(ctx, f.data, f.artifacts, destination))
				}
				if err == nil || !existing && !errors.Is(err, context.Canceled) {
					t.Fatalf("refusal: %v", err)
				}
				if existing {
					entries, err := os.ReadDir(destination)
					if err != nil || len(entries) != 1 || entries[0].Name() != "sentinel" {
						t.Fatalf("existing destination changed: %v", err)
					}
					data, err := os.ReadFile(filepath.Join(destination, "sentinel"))
					if err != nil || string(data) != "keep" {
						t.Fatal("sentinel changed")
					}
				} else if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("canceled operation published: %v", err)
				}
				unchangedCompanionSource(t, f, before)
			})
		}
	}
}

// Refusals below use actual modes, links and bbolt locks. They do not inject
// successful Close results or reconstruct the lock owner's descriptor.
func TestCompanionPrivateOwnershipRefusals(t *testing.T) {
	for _, restore := range []bool{false, true} {
		for _, damage := range []string{"root-mode", "database-mode", "objects-mode", "object-mode", "database-hardlink", "object-hardlink", "database-lock"} {
			name := "source/"
			if restore {
				name = "bundle/"
			}
			t.Run(name+damage, func(t *testing.T) {
				f := newCompanionFixture(t, true)
				base := filepath.Dir(f.data)
				bundle := filepath.Join(base, "bundle")
				if err := f.operation(BackupCompanion(context.Background(), f.data, f.artifacts, bundle)); err != nil {
					f.fatal(t, err)
				}
				data, objects := f.data, f.artifacts
				if restore {
					data, objects = bundle, filepath.Join(bundle, "objects")
				}
				object := filepath.Join(objects, f.ref.ID+".tar")
				var err error
				switch damage {
				case "root-mode":
					err = os.Chmod(data, 0755)
				case "database-mode":
					err = os.Chmod(filepath.Join(data, "jobs.db"), 0644)
				case "objects-mode":
					err = os.Chmod(objects, 0755)
				case "object-mode":
					err = os.Chmod(object, 0644)
				case "database-hardlink":
					err = os.Link(filepath.Join(data, "jobs.db"), filepath.Join(base, "database-link"))
				case "object-hardlink":
					err = os.Link(object, filepath.Join(base, "object-link"))
				case "database-lock":
					f.lockedDB, err = bolt.Open(filepath.Join(data, "jobs.db"), 0600, &bolt.Options{NoFreelistSync: true, Timeout: time.Second})
				}
				if err != nil {
					f.fatal(t, err)
				}
				before := observeCompanionSource(t, f)
				bundleBefore := observeCompanionPaths(t, bundle)
				destination := filepath.Join(base, "refused")
				if restore {
					err = f.operation(RestoreCompanion(context.Background(), bundle, destination))
				} else {
					err = f.operation(BackupCompanion(context.Background(), f.data, f.artifacts, destination))
				}
				if err == nil {
					t.Fatal("non-private or locked input accepted")
				}
				if damage == "database-lock" && !errors.Is(err, bolt.ErrTimeout) {
					f.fatal(t, err)
				}
				if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("refusal published destination: %v", err)
				}
				unchangedCompanionSource(t, f, before)
				unchangedCompanionPaths(t, bundleBefore, observeCompanionPaths(t, bundle))
			})
		}
	}
}
