//go:build linux || darwin

package artifact

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestPublicationGrowthOnlyNewLink(t *testing.T) {
	for _, prefix := range []string{"part", "both", "object"} {
		t.Run(prefix, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			data := testArchive(t, testEntry{name: "file", body: "payload"})
			ref := filesRef(strings.Repeat("e", 32), data)
			id := completePart(t, f, ref, data)
			if prefix != "part" {
				if err := linkFile(int(f.directory.Fd()), partName(ref), objectName(ref)); err != nil {
					t.Fatal(err)
				}
			}
			if prefix == "object" {
				if err := root.Remove(partName(ref)); err != nil {
					t.Fatal(err)
				}
			}
			calls, links := 0, 0
			denied := errors.New("fresh capacity denied")
			f.hooks.link = func(dir int, part, object string) error { links++; return linkFile(dir, part, object) }
			receipt, err := f.SealPart(ref, id, func() error { calls++; return denied })
			if prefix == "part" {
				if !errors.Is(err, denied) || calls != 1 || links != 0 || receipt.File.valid() {
					t.Fatalf("new link bypassed guard: %+v %v calls%d links%d", receipt, err, calls, links)
				}
				if _, err := root.Lstat(objectName(ref)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("object created under pressure: %v", err)
				}
				if !bytes.Equal(readFile(t, root, partName(ref)), data) {
					t.Fatal("denial changed original part")
				}
				if _, err := f.SealPart(ref, id, nil); !errors.Is(err, ErrFilesBusy) {
					t.Fatalf("nil allowed a new link: %v", err)
				}
			} else if err != nil || calls != 0 || links != 0 || receipt.File != id || receipt.Manifest.Reference != ref {
				t.Fatalf("settlement incorrectly guarded: %+v %v calls%d links%d", receipt, err, calls, links)
			}
		})
	}
}

func TestPublicationGrowthAfterValidationAndSync(t *testing.T) {
	f, root, _ := filesFixture(t)
	data := testArchive(t, testEntry{name: "file", body: "payload"})
	ref := filesRef(strings.Repeat("f", 32), data)
	id := completePart(t, f, ref, data)
	pressure := false
	checks, links := 0, 0
	denied := errors.New("capacity became stale during validation")
	// The real file Sync has completed only AFTER full archive validation. Make
	// the cached decision change there; a saved pre-validation bool would fail.
	f.hooks.sync = func(file *os.File) error {
		err := file.Sync()
		info, e := file.Stat()
		if e != nil {
			return errors.Join(err, e)
		}
		if info.Mode().IsRegular() {
			pressure = true
		}
		return err
	}
	f.hooks.link = func(dir int, part, object string) error { links++; return linkFile(dir, part, object) }
	_, err := f.SealPart(ref, id, func() error {
		checks++
		if pressure {
			return denied
		}
		return nil
	})
	if !errors.Is(err, denied) || checks != 1 || links != 0 {
		t.Fatalf("stale preflight accepted: %v checks%d links%d", err, checks, links)
	}
	if _, err := root.Lstat(objectName(ref)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected object: %v", err)
	}
}

func TestCandidatePressureSameOriginalResume(t *testing.T) {
	f, root, _ := filesFixture(t)
	data := testArchive(t, testEntry{name: "file", body: "same original bytes"})
	ref := filesRef(strings.Repeat("a", 32), data)
	candidate, err := f.CreateCandidate(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := candidate.Write(data); err != nil {
		t.Fatal(err)
	}
	original := candidate.file
	description := candidate.Description()
	originalCloses := 0
	originalClosed := false
	allowed := false
	checks := 0
	f.hooks.close = func(file *os.File) error {
		err := file.Close()
		if file == original {
			originalCloses++
			originalClosed = err == nil
		}
		return err
	}
	check := func() error {
		checks++
		if !originalClosed {
			t.Fatal("original writer not positively closed before cached check")
		}
		if !allowed {
			return ErrFilesBusy
		}
		return nil
	}
	first, err := candidate.Seal(check)
	if !errors.Is(err, ErrFilesBusy) || errors.Is(err, ErrFileCloseUncertain) || first.File.valid() || originalCloses != 1 || checks != 1 {
		t.Fatalf("first pressure/closure=%+v %v closes%d checks%d", first, err, originalCloses, checks)
	}
	if _, err := original.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("actual original writer remained open: %v", err)
	}
	if candidate.Description() != description || !candidate.closed || candidate.aborted || candidate.err != nil {
		t.Fatal("ordinary pressure latched collection failure or changed original prefix")
	}
	if _, err := candidate.Write([]byte("new bytes")); !errors.Is(err, ErrFilesClosed) {
		t.Fatalf("closed original accepted new writes: %v", err)
	}
	if _, err := root.Lstat(objectName(ref)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("denied publication created object")
	}
	allowed = true
	sealed, err := candidate.Seal(check)
	if err != nil || sealed.Manifest.Reference != ref || sealed.File != description.File || originalCloses != 1 || checks != 2 {
		t.Fatalf("same-owner resume=%+v %v closes%d checks%d", sealed, err, originalCloses, checks)
	}
	if !bytes.Equal(readFile(t, root, objectName(ref)), data) {
		t.Fatal("resumed publication changed original bytes")
	}
	if _, err := root.Lstat(partName(ref)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("resumed original part not removed")
	}
	f.hooks.close = nil
}

func TestPublicationGuardCannotHideCloseUncertainty(t *testing.T) {
	f, _, _ := filesFixture(t)
	data := testArchive(t, testEntry{name: "file", body: "payload"})
	ref := filesRef(strings.Repeat("b", 32), data)
	id := completePart(t, f, ref, data)
	injected := errors.New("actual closed descriptor reported uncertain")
	f.hooks.close = func(file *os.File) error { return errors.Join(file.Close(), injected) }
	sealed, err := f.SealPart(ref, id, func() error { return ErrFilesBusy })
	if !errors.Is(err, ErrFilesBusy) || !errors.Is(err, ErrFileCloseUncertain) || !errors.Is(err, injected) || sealed.File.valid() {
		t.Fatalf("uncertain prefix hidden behind pressure: %+v %v", sealed, err)
	}
	discardTestClosedOwners(t, f)
	f.hooks.close = nil
}

func TestPublicationIdentityFailureBeforeGrowthCheck(t *testing.T) {
	for _, kind := range []string{"inode", "archive", "foreign-object"} {
		t.Run(kind, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			data := testArchive(t, testEntry{name: "file", body: "payload"})
			if kind == "archive" {
				data = []byte("not a valid archive")
			}
			ref := filesRef(strings.Repeat("c", 32), data)
			id := completePart(t, f, ref, data)
			if kind == "inode" {
				id.Inode++
			}
			if kind == "foreign-object" {
				writeFile(t, root, objectName(ref), "foreign sentinel", 0600)
			}
			checks := 0
			_, err := f.SealPart(ref, id, func() error { checks++; return nil })
			if err == nil || checks != 0 {
				t.Fatalf("capacity approved invalid actual prefix: %v checks%d", err, checks)
			}
		})
	}
}
