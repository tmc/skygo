package artifact

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCandidateSeal(t *testing.T) {
	f, root, _ := filesFixture(t)
	data := testArchive(t, testEntry{name: "file", body: "payload"})
	c, err := f.CreateCandidate(strings.Repeat("a", 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(c, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	desc := c.Description()
	if !desc.File.valid() || desc.Size != int64(len(data)) {
		t.Fatalf("description=%+v", desc)
	}
	if err := f.RemoveCandidate(desc); !errors.Is(err, ErrFilesBusy) {
		t.Fatalf("active removal=%v", err)
	}
	ref := filesRef(desc.ID, data)
	if _, err := f.WritePart(ref, desc.File, desc.Size, 0, data[:1]); !errors.Is(err, ErrFilesBusy) {
		t.Fatalf("upload mutation=%v", err)
	}
	r, err := c.Seal(testPublicationGrowth)
	if err != nil {
		t.Fatal(err)
	}
	if r.File != desc.File || r.Manifest.Reference != ref || r.Manifest.RegularFiles != 1 || r.Manifest.ExpandedBytes != 7 {
		t.Fatalf("receipt=%+v", r)
	}
	if _, err := root.Lstat(desc.ID + ".part"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	reader, err := f.OpenObject(ref, desc.File)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(data))
	n, err := reader.ReadAt(got, 0)
	if err != nil || n != len(data) || !bytes.Equal(got, data) {
		t.Fatalf("read=%d %v", n, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCandidateAbortAndBounds(t *testing.T) {
	for _, kind := range []string{"empty", "invalid", "oversize", "collect-error"} {
		t.Run(kind, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			c, err := f.CreateCandidate(strings.Repeat("b", 32))
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "empty":
				if c.Description().Size != 0 {
					t.Fatal("nonzero empty staging")
				}
			case "invalid":
				if _, err := c.Write([]byte("not an archive")); err != nil {
					t.Fatal(err)
				}
				if _, err := c.Seal(testPublicationGrowth); err == nil {
					t.Fatal("invalid archive sealed")
				}
			case "oversize":
				block := make([]byte, chunkSize)
				for i := int64(0); i < MaxSize; i += int64(len(block)) {
					if _, err := c.Write(block); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := c.Write([]byte{1}); err == nil {
					t.Fatal("over limit accepted")
				}
			case "collect-error":
				// A collector can fail a root aftercheck after writing a complete tar.
				// The actual caller must abort on that extrinsic error, not call Seal.
				input, _ := testRoot(t)
				writeFile(t, input, "file", "safe", 0600)
				out := &callbackWriter{callback: func() { writeFile(t, input, "added", "late entry", 0600) }}
				if err := Snapshot(input, io.MultiWriter(c, out)); err == nil {
					t.Fatal("actual root aftercheck did not fail")
				}
				if _, err := Validate(bytes.NewReader(out.Bytes()), int64(out.Len())); err != nil {
					t.Fatalf("fixture did not leave complete valid tar: %v", err)
				}
			}
			if err := c.Close(); err != nil && kind != "invalid" && kind != "oversize" {
				t.Fatal(err)
			}
			if _, err := c.Seal(testPublicationGrowth); err == nil {
				t.Fatal("aborted collection published")
			}
			if _, err := root.Lstat(c.Description().ID + ".tar"); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("object published after abort")
			}
			if err := f.RemoveCandidate(c.Description()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCandidateOwnersAndNoAdoption(t *testing.T) {
	f, _, _ := filesFixture(t)
	var owners []*Candidate
	defer func() {
		for _, c := range owners {
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	for _, id := range []string{"1", "2", "3", "4"} {
		c, err := f.CreateCandidate(strings.Repeat(id, 32))
		if err != nil {
			t.Fatal(err)
		}
		owners = append(owners, c)
	}
	if _, err := f.CreateCandidate(strings.Repeat("5", 32)); !errors.Is(err, ErrFilesBusy) {
		t.Fatalf("fifth owner=%v", err)
	}
	if err := owners[0].Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.CreateCandidate(strings.Repeat("1", 32)); err == nil {
		t.Fatal("existing prefix adopted")
	}
	wrong := owners[0].Description()
	wrong.File.Inode++
	if err := f.RemoveCandidate(wrong); err == nil {
		t.Fatal("foreign inode removed")
	}
	if err := f.Close(); !errors.Is(err, ErrFilesBusy) {
		t.Fatalf("close credited active owners=%v", err)
	}
}

func TestCandidateUncertainClosureHeld(t *testing.T) {
	for _, stage := range []string{"sync", "close"} {
		t.Run(stage, func(t *testing.T) {
			f, _, _ := filesFixture(t)
			c, err := f.CreateCandidate(strings.Repeat("c", 32))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Write(testArchive(t, testEntry{name: "file"})); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("owned operation reported uncertainty")
			if stage == "sync" {
				f.hooks.sync = func(file *os.File) error { return errors.Join(file.Sync(), injected) }
			} else {
				f.hooks.close = func(file *os.File) error { return errors.Join(file.Close(), injected) }
			}
			if r, err := c.Seal(testPublicationGrowth); !errors.Is(err, injected) || r.File.valid() {
				t.Fatalf("false seal=%+v %v", r, err)
			}
			f.hooks = filesIO{}
			if stage == "sync" {
				if err := c.Close(); !errors.Is(err, injected) || errors.Is(err, ErrFileCloseUncertain) {
					t.Fatalf("positively closed sync failure=%v", err)
				}
				if err := f.RemoveCandidate(c.Description()); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err := f.RemoveCandidate(c.Description()); !errors.Is(err, ErrFilesBusy) {
				t.Fatalf("uncertain owner forgotten=%v", err)
			}
			if err := f.Close(); !errors.Is(err, ErrFilesBusy) {
				t.Fatalf("uncertain close credited=%v", err)
			}
			// Both hooks performed the actual FD close before reporting failure.
			// Test-only disposal is not production reconciliation or proof credit.
			f.mu.Lock()
			delete(f.candidates, c)
			f.mu.Unlock()
			discardTestClosedOwners(t, f)
		})
	}
}

func TestCandidateCloseJoinsWrite(t *testing.T) {
	f, _, _ := filesFixture(t)
	c, err := f.CreateCandidate(strings.Repeat("d", 32))
	if err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	f.hooks.writeAt = func(file *os.File, p []byte, offset int64) (int, error) {
		close(entered)
		<-resume
		return file.WriteAt(p, offset)
	}
	written, closed := make(chan error, 1), make(chan error, 1)
	writeFinished, closeFinished := make(chan struct{}), make(chan struct{})
	closeStarted := false
	defer func() {
		release()
		select {
		case <-writeFinished:
		case <-time.After(3 * time.Second):
			t.Error("owned writer did not join")
		}
		if closeStarted {
			select {
			case <-closeFinished:
			case <-time.After(3 * time.Second):
				t.Error("owned closer did not join")
			}
		} else {
			c.Close()
		}
	}()
	go func() { defer close(writeFinished); _, err := c.Write([]byte("owned bytes")); written <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		release()
		t.Fatal("write did not enter")
	}
	closeStarted = true
	go func() { defer close(closeFinished); closed <- c.Close() }()
	select {
	case err := <-closed:
		release()
		t.Fatalf("close before actual write completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	release()
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("write did not join")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("close did not join")
	}
}

func TestCandidatePartialWriteAndIdentity(t *testing.T) {
	f, root, _ := filesFixture(t)
	for _, id := range []string{"", strings.Repeat("A", 32), strings.Repeat("0", 31), "../" + strings.Repeat("0", 29)} {
		if _, err := f.CreateCandidate(id); err == nil {
			t.Fatalf("invalid ID accepted: %q", id)
		}
	}
	c, err := f.CreateCandidate(strings.Repeat("e", 32))
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("partial actual write")
	f.hooks.writeAt = func(file *os.File, p []byte, offset int64) (int, error) {
		n, err := file.WriteAt(p[:2], offset)
		return n, errors.Join(err, injected)
	}
	if n, err := c.Write([]byte("four")); n != 2 || !errors.Is(err, injected) {
		t.Fatalf("partial=%d %v", n, err)
	}
	f.hooks = filesIO{}
	if c.Description().Size != 2 {
		t.Fatal("actual accepted prefix lost")
	}
	if _, err := c.Write([]byte("again")); !errors.Is(err, injected) {
		t.Fatal("write error was not sticky")
	}
	if err := c.Close(); !errors.Is(err, injected) {
		t.Fatal("abort lost original write error")
	}
	wrong := c.Description()
	wrong.Size++
	if err := f.RemoveCandidate(wrong); err == nil {
		t.Fatal("wrong prefix size deleted")
	}
	if got := string(readFile(t, root, c.Description().ID+".part")); got != "fo" {
		t.Fatalf("prefix=%q", got)
	}
	if err := f.RemoveCandidate(c.Description()); err != nil {
		t.Fatal(err)
	}
	if err := f.RemoveCandidate(c.Description()); err != nil {
		t.Fatal("absence did not sync")
	}
}

func TestCandidateCreationSyncClosed(t *testing.T) {
	f, root, _ := filesFixture(t)
	injected := errors.New("creation sync failed")
	f.hooks.sync = func(file *os.File) error { return errors.Join(file.Sync(), injected) }
	c, err := f.CreateCandidate(strings.Repeat("f", 32))
	if c == nil || !errors.Is(err, injected) || !c.Description().File.valid() {
		t.Fatalf("lost exclusive creation owner: %v %v", c, err)
	}
	f.hooks = filesIO{}
	if err := c.Close(); !errors.Is(err, injected) {
		t.Fatal("sync uncertainty erased by actual close")
	}
	if _, err := root.Lstat(c.Description().ID + ".part"); err != nil {
		t.Fatal("sync failure credited storage before deletion")
	}
	if err := f.RemoveCandidate(c.Description()); err != nil {
		t.Fatal(err)
	}
}
