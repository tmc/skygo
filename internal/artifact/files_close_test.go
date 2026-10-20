package artifact

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
)

// Every injected Close here calls the actual original File.Close first. This
// helper only disposes fixture bookkeeping after assertions; it supplies no
// production recovery, capacity credit or permission to re-close a numeric FD.
func discardTestClosedOwners(t *testing.T, f *Files) {
	t.Helper()
	f.closeMu.Lock()
	defer f.closeMu.Unlock()
	for file := range f.uncertain {
		if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("test original descriptor not actually closed: %v", err)
		}
		delete(f.uncertain, file)
	}
}

func TestFilesTypedOriginalClose(t *testing.T) {
	for _, operation := range []string{"recover", "write", "seal"} {
		t.Run(operation, func(t *testing.T) {
			f, _, _ := filesFixture(t)
			data := testArchive(t, testEntry{name: "file", body: "payload"})
			ref := filesRef(strings.Repeat("0", 32), data)
			id := completePart(t, f, ref, data)
			injected := errors.New("actual close reported uncertainty")
			calls := 0
			f.hooks.close = func(file *os.File) error { calls++; return errors.Join(file.Close(), injected) }
			var err error
			switch operation {
			case "recover":
				err = f.RecoverPart(ref, id, ref.Size)
			case "write":
				_, err = f.WritePart(ref, id, ref.Size, 0, data[:1])
			case "seal":
				_, err = f.SealPart(ref, id, testPublicationGrowth)
			}
			if !errors.Is(err, injected) || !errors.Is(err, ErrFileCloseUncertain) {
				t.Fatalf("untyped uncertainty=%v", err)
			}
			var original *FileCloseError
			if !errors.As(err, &original) || original.owner != f || original.file == nil {
				t.Fatal("lost actual original owner")
			}
			if _, err := original.file.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatal("fixture did not actually close")
			}
			f.hooks = filesIO{}
			if _, err := f.WritePart(ref, id, ref.Size, 0, data[:1]); !errors.Is(err, ErrFilesBusy) || errors.Is(err, ErrFileCloseUncertain) {
				t.Fatalf("replacement admission=%v", err)
			}
			if err := f.closeFile(original.file); err != original || calls != 1 {
				t.Fatal("uncertain original descriptor retried")
			}
			if err := f.Close(); !errors.Is(err, ErrFilesBusy) {
				t.Fatalf("closure protection released=%v", err)
			}
			discardTestClosedOwners(t, f)
		})
	}
}

func TestFilesCleanupCloseClassification(t *testing.T) {
	f, _, _ := filesFixture(t)
	data := testArchive(t, testEntry{name: "file"})
	ref := filesRef(strings.Repeat("1", 32), data)
	id, err := f.CreatePart(ref)
	if err != nil {
		t.Fatal(err)
	}
	// Recover rejects the missing acknowledged prefix only AFTER opening the
	// exact original part, so its actual early cleanup Close must be classified.
	injected := errors.New("cleanup original close uncertain")
	f.hooks.close = func(file *os.File) error { return errors.Join(file.Close(), injected) }
	err = f.RecoverPart(ref, id, 1)
	if !errors.Is(err, ErrFileConflict) || !errors.Is(err, ErrFileCloseUncertain) {
		t.Fatalf("early cleanup=%v", err)
	}
	f.hooks = filesIO{}
	discardTestClosedOwners(t, f)
}

func TestFilesPositiveCloseErrorsRecover(t *testing.T) {
	for _, operation := range []string{"sync", "validation", "write"} {
		t.Run(operation, func(t *testing.T) {
			f, _, _ := filesFixture(t)
			data := testArchive(t, testEntry{name: "file"})
			ref := filesRef(strings.Repeat("2", 32), data)
			id, err := f.CreatePart(ref)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("ordinary positively closed failure")
			switch operation {
			case "sync":
				f.hooks.sync = func(file *os.File) error { return errors.Join(file.Sync(), injected) }
				_, err = f.WritePart(ref, id, 0, 0, data)
			case "validation":
				err = f.RecoverPart(ref, id, 1)
			case "write":
				f.hooks.writeAt = func(file *os.File, p []byte, offset int64) (int, error) {
					n, err := file.WriteAt(p[:1], offset)
					return n, errors.Join(err, injected)
				}
				_, err = f.WritePart(ref, id, 0, 0, data)
			}
			f.hooks = filesIO{}
			if err == nil || errors.Is(err, ErrFileCloseUncertain) || f.hasUncertainClose() {
				t.Fatalf("ordinary failure poisoned owner: %v", err)
			}
			if err := f.RecoverPart(ref, id, 0); err != nil {
				t.Fatal(err)
			}
			if _, err := f.WritePart(ref, id, 0, 0, data); err != nil {
				t.Fatal(err)
			}
			if _, err := f.SealPart(ref, id, testPublicationGrowth); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCandidateDigestAndPublicationRecovery(t *testing.T) {
	f, root, _ := filesFixture(t)
	c, err := f.CreateCandidate(strings.Repeat("3", 32))
	if err != nil {
		t.Fatal(err)
	}
	empty := sha256.Sum256(nil)
	if c.Description().SHA256 != hex.EncodeToString(empty[:]) {
		t.Fatal("missing empty staging digest")
	}
	data := testArchive(t, testEntry{name: "file", body: "payload"})
	if _, err := c.Write(data[:13]); err != nil {
		t.Fatal(err)
	}
	prefix := sha256.Sum256(data[:13])
	if c.Description().SHA256 != hex.EncodeToString(prefix[:]) {
		t.Fatal("counted prefix digest mismatch")
	}
	if _, err := c.Write(data[13:]); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("publication directory sync failed")
	calls := 0
	f.hooks.sync = func(file *os.File) error {
		err := file.Sync()
		if file == f.directory {
			calls++
			if calls == 1 {
				return errors.Join(err, injected)
			}
		}
		return err
	}
	if _, err := c.Seal(testPublicationGrowth); !errors.Is(err, injected) || errors.Is(err, ErrFileCloseUncertain) {
		t.Fatalf("closed publication failure=%v", err)
	}
	f.hooks = filesIO{}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	desc := c.Description()
	ref := Ref{ID: desc.ID, SHA256: desc.SHA256, Size: desc.Size}
	part, err := root.Lstat(partName(ref))
	if err != nil {
		t.Fatal(err)
	}
	object, err := root.Lstat(objectName(ref))
	if err != nil || !os.SameFile(part, object) {
		t.Fatal("not actual two-link charged prefix")
	}
	if err := f.RemoveCandidate(desc); err == nil {
		t.Fatal("singlepart cleanup accepted two-link prefix")
	}
	// Real assembly commits cleanup intent and retains charge before these
	// exact original-inode operations; FD closure above supplies no such credit.
	if err := f.RemovePart(ref, desc.File); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Lstat(objectName(ref)); err != nil {
		t.Fatal("part-only cleanup erased object protection")
	}
	if err := f.RemoveObject(ref, desc.File); err != nil {
		t.Fatal(err)
	}
}

func TestCandidateCleanupRefusesEveryObjectPrefix(t *testing.T) {
	for _, prefix := range []string{"object-only", "unrelated-object", "missing-both"} {
		t.Run(prefix, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			c, err := f.CreateCandidate(strings.Repeat("4", 32))
			if err != nil {
				t.Fatal(err)
			}
			data := testArchive(t, testEntry{name: "file", body: "payload"})
			if _, err := c.Write(data); err != nil {
				t.Fatal(err)
			}
			desc := c.Description()
			ref := Ref{ID: desc.ID, SHA256: desc.SHA256, Size: desc.Size}
			switch prefix {
			case "object-only":
				injected := errors.New("final publication root sync failed after actual unlink")
				calls := 0
				f.hooks.sync = func(file *os.File) error {
					err := file.Sync()
					if file == f.directory {
						calls++
						if calls == 2 {
							return errors.Join(err, injected)
						}
					}
					return err
				}
				if _, err := c.Seal(testPublicationGrowth); !errors.Is(err, injected) || errors.Is(err, ErrFileCloseUncertain) {
					t.Fatalf("publication fixture=%v", err)
				}
				f.hooks = filesIO{}
				if calls != 2 {
					t.Fatal("did not fail second actual directory sync")
				}
				if _, err := root.Lstat(partName(ref)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("part not actually unlinked")
				}
				if _, err := root.Lstat(objectName(ref)); err != nil {
					t.Fatal("published object missing")
				}
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
				if err := f.RemoveCandidate(desc); !errors.Is(err, ErrFileConflict) {
					t.Fatalf("object-only false absence=%v", err)
				}
				if _, err := root.Lstat(objectName(ref)); err != nil {
					t.Fatal("object-only was erased")
				}
				// Caller holds committed publication cleanup intent for both names.
				if err := f.RemovePart(ref, desc.File); err != nil {
					t.Fatal(err)
				}
				if err := f.RemoveObject(ref, desc.File); err != nil {
					t.Fatal(err)
				}
			case "unrelated-object":
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
				writeFile(t, root, objectName(ref), "foreign published sentinel", 0600)
				if err := f.RemoveCandidate(desc); !errors.Is(err, ErrFileConflict) {
					t.Fatalf("unrelated object bypass=%v", err)
				}
				if string(readFile(t, root, objectName(ref))) != "foreign published sentinel" {
					t.Fatal("foreign object changed")
				}
				if info, err := root.Lstat(partName(ref)); err != nil || info.Size() != ref.Size {
					t.Fatal("owned part erased despite foreign object")
				}
			case "missing-both":
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
				if err := f.RemoveCandidate(desc); err != nil {
					t.Fatal(err)
				}
				injected := errors.New("absence directory sync failed")
				f.hooks.sync = func(file *os.File) error { return errors.Join(file.Sync(), injected) }
				if err := f.RemoveCandidate(desc); !errors.Is(err, injected) {
					t.Fatal("missing-both credited without actual root sync")
				}
				f.hooks = filesIO{}
				if err := f.RemoveCandidate(desc); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
