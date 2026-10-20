//go:build linux || darwin

package artifact

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// These are concrete Files-only fixtures. They observe actual Candidate writes,
// closes, inodes and links; they do not fabricate publication Ref/Store intent,
// native writer closure, physical reclamation or metadata reservation receipts.
func cleanupCandidate(t *testing.T, f *Files, payload []byte) CandidateFile {
	t.Helper()
	candidate, err := f.CreateCandidate(strings.Repeat("a", 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := candidate.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := candidate.Close(); err != nil {
		t.Fatal(err)
	}
	return candidate.Description()
}

func linkCleanupCandidate(t *testing.T, f *Files, c CandidateFile, name string) {
	t.Helper()
	if err := linkFile(int(f.directory.Fd()), c.ID+".part", name); err != nil {
		t.Fatal(err)
	}
	if err := f.syncRoot(); err != nil {
		t.Fatal(err)
	}
}

func requireCleanupAbsent(t *testing.T, root *os.Root, c CandidateFile) {
	t.Helper()
	for _, name := range []string{c.ID + ".part", c.ID + ".tar"} {
		if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s remains or absence failed: %v", name, err)
		}
	}
}

func TestRemoveCandidateNamesPrefixes(t *testing.T) {
	for _, tc := range []struct {
		name         string
		payload      []byte
		part, object bool
	}{
		{"empty", nil, true, false},
		{"empty_both", nil, true, true},
		{"malformed_part", []byte("not a tar archive"), true, false},
		{"malformed_both", []byte("unfinished"), true, true},
		{"object_only", []byte("unfinished"), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			c := cleanupCandidate(t, f, tc.payload)
			if tc.object {
				linkCleanupCandidate(t, f, c, c.ID+".tar")
			}
			if !tc.part {
				if err := root.Remove(c.ID + ".part"); err != nil {
					t.Fatal(err)
				}
			}
			if tc.object {
				if err := f.RemoveCandidate(c); !errors.Is(err, ErrFileConflict) {
					t.Fatalf("existing single-name API changed: %v", err)
				}
			}
			if err := f.RemoveCandidateNames(c); err != nil {
				t.Fatal(err)
			}
			requireCleanupAbsent(t, root, c)
		})
	}
}

func TestRemoveCandidateNamesRefusesBeforeUnlink(t *testing.T) {
	for _, kind := range []string{"foreign_inode", "wrong_size", "third_link", "single_foreign_link", "replacement"} {
		t.Run(kind, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			c := cleanupCandidate(t, f, []byte("original prefix"))
			if kind != "single_foreign_link" {
				linkCleanupCandidate(t, f, c, c.ID+".tar")
			}
			subject := c
			switch kind {
			case "foreign_inode":
				subject.File.Inode++
			case "wrong_size":
				subject.Size++
			case "third_link", "single_foreign_link":
				linkCleanupCandidate(t, f, c, "unrelated-link")
			case "replacement":
				if err := root.Remove(c.ID + ".tar"); err != nil {
					t.Fatal(err)
				}
				writeFile(t, root, c.ID+".tar", "foreign sentinel", 0600)
			}
			removes := 0
			f.hooks.remove = func(root *os.Root, name string) error {
				removes++
				return root.Remove(name)
			}
			err := f.RemoveCandidateNames(subject)
			f.hooks = filesIO{}
			if !errors.Is(err, ErrFileConflict) || removes != 0 {
				t.Fatalf("refusal=%v removals=%d", err, removes)
			}
			if got := string(readFile(t, root, c.ID+".part")); got != "original prefix" {
				t.Fatalf("owned part changed: %q", got)
			}
			if kind == "replacement" {
				if got := string(readFile(t, root, c.ID+".tar")); got != "foreign sentinel" {
					t.Fatalf("foreign object changed: %q", got)
				}
			}
		})
	}
}

func TestRemoveCandidateNamesObservedReplacementAfterClose(t *testing.T) {
	f, root, _ := filesFixture(t)
	c := cleanupCandidate(t, f, []byte("original prefix"))
	linkCleanupCandidate(t, f, c, c.ID+".tar")
	replaced := false
	f.hooks.close = func(file *os.File) error {
		info, err := file.Stat()
		if err != nil {
			return errors.Join(err, file.Close())
		}
		m, err := fileMetadata(info)
		if err == nil && fileID(m) == c.File && !replaced {
			replaced = true
			err = root.Remove(c.ID + ".tar")
			if err == nil {
				var replacement *os.File
				replacement, err = root.OpenFile(c.ID+".tar", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
				if err == nil {
					_, writeErr := io.WriteString(replacement, "replacement sentinel")
					err = errors.Join(writeErr, replacement.Close())
				}
			}
		}
		return errors.Join(err, file.Close())
	}
	removes := 0
	f.hooks.remove = func(root *os.Root, name string) error {
		removes++
		return root.Remove(name)
	}
	err := f.RemoveCandidateNames(c)
	f.hooks = filesIO{}
	if !replaced || !errors.Is(err, ErrFileConflict) || removes != 0 || errors.Is(err, ErrFileCloseUncertain) {
		t.Fatalf("observed replacement=%v err=%v removals=%d", replaced, err, removes)
	}
	if string(readFile(t, root, c.ID+".part")) != "original prefix" || string(readFile(t, root, c.ID+".tar")) != "replacement sentinel" {
		t.Fatal("observed foreign replacement was deleted")
	}
}

func TestRemoveCandidateNamesPartialProgress(t *testing.T) {
	for _, stage := range []string{"first_after_unlink", "second_before_unlink", "second_after_unlink", "root_sync"} {
		t.Run(stage, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			c := cleanupCandidate(t, f, []byte("incomplete archive"))
			linkCleanupCandidate(t, f, c, c.ID+".tar")
			injected := errors.New("actual cleanup progress reported failure")
			removes := 0
			f.hooks.remove = func(root *os.Root, name string) error {
				removes++
				if stage == "second_before_unlink" && removes == 2 {
					return injected
				}
				err := root.Remove(name)
				if stage == "first_after_unlink" && removes == 1 || stage == "second_after_unlink" && removes == 2 {
					err = errors.Join(err, injected)
				}
				return err
			}
			f.hooks.sync = func(file *os.File) error {
				err := file.Sync()
				if stage == "root_sync" && file == f.directory {
					return errors.Join(err, injected)
				}
				return err
			}
			err := f.RemoveCandidateNames(c)
			f.hooks = filesIO{}
			if !errors.Is(err, injected) || errors.Is(err, ErrFileCloseUncertain) {
				t.Fatalf("partial progress error=%v", err)
			}
			if _, err := root.Lstat(c.ID + ".part"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("first actual unlink missing: %v", err)
			}
			if stage == "first_after_unlink" || stage == "second_before_unlink" {
				info, err := root.Lstat(c.ID + ".tar")
				if err != nil {
					t.Fatal(err)
				}
				if err := f.checkOwnedFile(info, c.File, 1, MaxSize); err != nil || info.Size() != c.Size {
					t.Fatalf("remaining exact single-link prefix: %v", err)
				}
			} else {
				requireCleanupAbsent(t, root, c)
			}
			// Caller keeps its committed intent and every charge after the error.
			// This Files-only retry claims no Store/counter retirement.
			if err := f.RemoveCandidateNames(c); err != nil {
				t.Fatalf("exact remaining-prefix retry: %v", err)
			}
			requireCleanupAbsent(t, root, c)
		})
	}
}

func TestRemoveCandidateNamesAbsenceRequiresSync(t *testing.T) {
	f, root, _ := filesFixture(t)
	c := cleanupCandidate(t, f, nil)
	if err := f.RemoveCandidateNames(c); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("actual absent-root sync failed")
	syncs := 0
	f.hooks.sync = func(file *os.File) error {
		err := file.Sync()
		if file == f.directory {
			syncs++
			return errors.Join(err, injected)
		}
		return err
	}
	err := f.RemoveCandidateNames(c)
	f.hooks = filesIO{}
	if !errors.Is(err, injected) || syncs != 1 {
		t.Fatalf("both-absent error=%v actual root syncs=%d", err, syncs)
	}
	requireCleanupAbsent(t, root, c)
	if err := f.RemoveCandidateNames(c); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveCandidateNamesOriginalCloseRetained(t *testing.T) {
	f, root, _ := filesFixture(t)
	c := cleanupCandidate(t, f, []byte("closed prefix"))
	linkCleanupCandidate(t, f, c, c.ID+".tar")
	injected := errors.New("original candidate cleanup close uncertain")
	candidateCloses := 0
	f.hooks.close = func(file *os.File) error {
		info, err := file.Stat()
		if err != nil {
			return errors.Join(err, file.Close())
		}
		m, err := fileMetadata(info)
		closeErr := file.Close()
		if err == nil && fileID(m) == c.File {
			candidateCloses++
			return errors.Join(closeErr, injected)
		}
		return errors.Join(err, closeErr)
	}
	err := f.RemoveCandidateNames(c)
	if !errors.Is(err, ErrFileCloseUncertain) || !errors.Is(err, injected) {
		t.Fatalf("lost typed original close: %v", err)
	}
	var original *FileCloseError
	if !errors.As(err, &original) || original.owner != f || original.file == nil {
		t.Fatal("exact original FD/Files owner not retained")
	}
	if _, err := original.file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("test hook did not actually close the original file")
	}
	for _, name := range []string{c.ID + ".part", c.ID + ".tar"} {
		if _, err := root.Lstat(name); err != nil {
			t.Fatalf("name deleted after uncertain Close: %s: %v", name, err)
		}
	}
	if err := f.RemoveCandidateNames(c); !errors.Is(err, ErrFilesBusy) || candidateCloses != 1 {
		t.Fatalf("replacement/FD retry after uncertain Close: %v closes=%d", err, candidateCloses)
	}
	if err := f.Close(); !errors.Is(err, ErrFilesBusy) {
		t.Fatalf("uncertain cleanup owner released: %v", err)
	}
	f.hooks = filesIO{}
	discardTestClosedOwners(t, f)
}

func TestRemoveCandidateNamesBusyOwners(t *testing.T) {
	t.Run("candidate", func(t *testing.T) {
		f, root, _ := filesFixture(t)
		candidate, err := f.CreateCandidate(strings.Repeat("a", 32))
		if err != nil {
			t.Fatal(err)
		}
		if err := f.RemoveCandidateNames(candidate.Description()); !errors.Is(err, ErrFilesBusy) {
			t.Fatalf("active candidate removed: %v", err)
		}
		if _, err := root.Lstat(candidate.Description().ID + ".part"); err != nil {
			t.Fatal(err)
		}
		if err := candidate.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("reader", func(t *testing.T) {
		f, root, _ := filesFixture(t)
		candidate, err := f.CreateCandidate(strings.Repeat("a", 32))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := candidate.Write(testArchive(t, testEntry{name: "file", body: "payload"})); err != nil {
			t.Fatal(err)
		}
		c := candidate.Description()
		sealed, err := candidate.Seal(testPublicationGrowth)
		if err != nil {
			t.Fatal(err)
		}
		reader, err := f.OpenObject(sealed.Manifest.Reference, sealed.File)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.RemoveCandidateNames(c); !errors.Is(err, ErrFilesBusy) {
			t.Fatalf("active Reader removed: %v", err)
		}
		if _, err := root.Lstat(c.ID + ".tar"); err != nil {
			t.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRemoveCandidateNamesRootIdentity(t *testing.T) {
	f, root, host := filesFixture(t)
	c := cleanupCandidate(t, f, []byte("owned prefix"))
	other, _ := testRoot(t)
	writeFile(t, other, c.ID+".part", "other root sentinel", 0600)
	f.root = other
	err := f.RemoveCandidateNames(c)
	f.root = root
	if !errors.Is(err, ErrFileConflict) || string(readFile(t, root, c.ID+".part")) != "owned prefix" || string(readFile(t, other, c.ID+".part")) != "other root sentinel" {
		t.Fatalf("observed root replacement not refused: %v", err)
	}
	moved := host + "-moved"
	if err := os.Rename(host, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(host, 0700); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(host, c.ID+".part")
	if err := os.WriteFile(foreign, []byte("host pathname sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.RemoveCandidateNames(c); err != nil {
		t.Fatal(err)
	}
	requireCleanupAbsent(t, root, c)
	got, err := os.ReadFile(foreign)
	if err != nil || string(got) != "host pathname sentinel" {
		t.Fatal("host pathname replacement was adopted")
	}
	// Remove only the known old empty installation after the actual operation.
	// The borrowed original Root remains owned by testRoot's cleanup.
	if err := root.Remove(rootMarkerName); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(moved); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveCandidateNamesSubjectBounds(t *testing.T) {
	for _, kind := range []string{"id", "hash", "file", "negative", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			c := cleanupCandidate(t, f, []byte("owned prefix"))
			subject := c
			switch kind {
			case "id":
				subject.ID = "../" + strings.Repeat("a", 29)
			case "hash":
				subject.SHA256 = strings.Repeat("A", 64)
			case "file":
				subject.File = FileID{}
			case "negative":
				subject.Size = -1
			case "oversize":
				subject.Size = MaxSize + 1
			}
			closes := 0
			f.hooks.close = func(file *os.File) error {
				closes++
				return file.Close()
			}
			err := f.RemoveCandidateNames(subject)
			f.hooks = filesIO{}
			if !errors.Is(err, ErrFileConflict) || closes != 0 {
				t.Fatalf("invalid subject reached descriptor IO: %v closes=%d", err, closes)
			}
			if string(readFile(t, root, c.ID+".part")) != "owned prefix" {
				t.Fatal("invalid subject changed original prefix")
			}
		})
	}
}

func TestRemoveCandidateNamesMarkerRefusal(t *testing.T) {
	f, root, _ := filesFixture(t)
	c := cleanupCandidate(t, f, []byte("owned prefix"))
	marker, err := root.OpenFile(rootMarkerName, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := io.WriteString(marker, "invalid installation marker\n")
	if err := errors.Join(writeErr, marker.Close()); err != nil {
		t.Fatal(err)
	}
	removes := 0
	f.hooks.remove = func(root *os.Root, name string) error {
		removes++
		return root.Remove(name)
	}
	err = f.RemoveCandidateNames(c)
	f.hooks = filesIO{}
	if err == nil || removes != 0 {
		t.Fatalf("invalid original marker allowed cleanup: %v removes=%d", err, removes)
	}
	if string(readFile(t, root, c.ID+".part")) != "owned prefix" {
		t.Fatal("prefix removed through invalid installation marker")
	}
}
