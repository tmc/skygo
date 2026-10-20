//go:build linux || darwin

package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func finishPhysical(t *testing.T, s *PhysicalScan) PhysicalSample {
	t.Helper()
	for step := 0; step < 32; step++ {
		sample, err := s.Step(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if sample.Status == PhysicalIntervalComplete {
			return sample
		}
	}
	t.Fatal("finite actual fixture never reached EOF")
	return PhysicalSample{}
}
func beginPhysical(t *testing.T, f *Files) *PhysicalScan {
	t.Helper()
	s, err := f.BeginPhysicalSample()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestPhysicalIntervalActualInodes(t *testing.T) {
	f, root, name := filesFixture(t)
	writeFile(t, root, "data", "actual allocated payload", 0600)
	if err := os.Link(filepath.Join(name, "data"), filepath.Join(name, "alias")); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "never-open")
	if err := os.WriteFile(outside, make([]byte, 1<<20), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(name, "symlink")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(name, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := root.Mkdir("directory", 0700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, root, "directory/child", "child", 0600)
	var expected uint64
	seen := make(map[[2]uint64]bool)
	for _, entry := range []string{".", rootMarkerName, "data", "alias", "symlink", "fifo", "directory", "directory/child"} {
		info, err := os.Lstat(filepath.Join(name, entry))
		if err != nil {
			t.Fatal(err)
		}
		stat := info.Sys().(*syscall.Stat_t)
		id := [2]uint64{uint64(stat.Dev), uint64(stat.Ino)}
		if !seen[id] {
			seen[id] = true
			expected += uint64(stat.Blocks) * 512
		}
	}
	opens := []string{}
	f.physicalIO.openDir = func(parent *os.File, name string) (*os.File, error) {
		opens = append(opens, name)
		if name != "." && name != "directory" {
			t.Fatalf("non-directory opened: %q", name)
		}
		return openPhysicalDirectory(parent, name)
	}
	s := beginPhysical(t, f)
	sample := finishPhysical(t, s)
	if sample.AllocatedBytes == nil || *sample.AllocatedBytes != expected || sample.Inodes == nil || *sample.Inodes != uint64(len(seen)) || sample.Names != 7 {
		t.Fatalf("actual census=%+v expected blocks=%d inodes=%d", sample, expected, len(seen))
	}
	if len(opens) != 2 || sample.EndedAt.IsZero() || sample.OldestObservedAt.Before(sample.StartedAt) || sample.NewestObservedAt.After(sample.EndedAt) || sample.FreeBytes == nil || sample.FreeObservedAt.IsZero() {
		t.Fatalf("interval/time facts=%+v opens=%v", sample, opens)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// The borrowed directory stream was not consumed by the scan.
	entries, err := f.directory.ReadDir(64)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if len(entries) != 6 {
		t.Fatalf("borrowed ReadDir offset consumed: %d", len(entries))
	}
}

func TestPhysical4097Cursor(t *testing.T) {
	f, root, _ := filesFixture(t)
	if err := root.Mkdir("bulk", 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4097; i++ {
		writeFile(t, root, fmt.Sprintf("bulk/file-%04d", i), "", 0600)
	}
	reads, maxBatch := 0, 0
	f.physicalIO.readDir = func(file *os.File, n int) ([]string, error) {
		reads++
		if n > 64 {
			t.Fatalf("unbounded batch %d", n)
		}
		entries, err := file.Readdirnames(n)
		if len(entries) > maxBatch {
			maxBatch = len(entries)
		}
		return entries, err
	}
	s := beginPhysical(t, f)
	original := s.frames[0].file
	first, err := s.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != PhysicalPartial || first.Visits != 4096 || !first.EndedAt.IsZero() || f.physical != s || s.frames[0].file != original {
		t.Fatalf("lost yielded original cursor: %+v", first)
	}
	if _, err := f.BeginPhysicalSample(); !errors.Is(err, ErrFilesBusy) {
		t.Fatalf("replacement cursor admitted: %v", err)
	}
	second := finishPhysical(t, s)
	if second.Names != 4099 || second.Visits != 4099 || maxBatch > 64 || reads < 65 {
		t.Fatalf("entry omission/restart=%+v reads=%d batch=%d", second, reads, maxBatch)
	}
	if f.physical != s {
		t.Fatal("EOF secretly released registration")
	}
}

func TestPhysicalRegistrationBlocksRootClose(t *testing.T) {
	for _, state := range []string{"yielded", "complete", "cancelled"} {
		t.Run(state, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			if state == "yielded" {
				if err := root.Mkdir("bulk", 0700); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 4097; i++ {
					writeFile(t, root, "bulk/"+fmt.Sprint(i), "", 0600)
				}
			}
			s := beginPhysical(t, f)
			ctx := context.Background()
			if state == "cancelled" {
				cancelCtx, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelCtx
			}
			_, err := s.Step(ctx)
			if state == "cancelled" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); !errors.Is(err, ErrFilesBusy) {
				t.Fatalf("registered %s did not block Files.Close: %v", state, err)
			}
			if _, err := f.directory.Stat(); err != nil {
				t.Fatalf("borrowed original directory closed: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := root.Lstat("."); err != nil {
				t.Fatalf("borrowed Root closed: %v", err)
			}
		})
	}
}

func TestPhysicalMarkerBoundsAndUncertainBegin(t *testing.T) {
	for _, size := range []int{512, 513} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			f, root, _ := filesFixture(t)
			file, err := root.OpenFile(rootMarkerName, os.O_WRONLY|os.O_TRUNC, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.Write([]byte(strings.Repeat("x", size))); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			closed := 0
			f.hooks.close = func(file *os.File) error { closed++; return file.Close() }
			s, err := f.BeginPhysicalSample()
			if err == nil || s == nil || f.physical != s {
				t.Fatalf("malformed marker accepted or owner lost: %v", err)
			}
			if size == 512 && closed != 1 || size == 513 && closed != 0 {
				t.Fatalf("marker bound wrong close/read prefix: size%d close%d", size, closed)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			f.hooks.close = nil
		})
	}
	t.Run("original-marker-close", func(t *testing.T) {
		f, _, _ := filesFixture(t)
		injected := errors.New("marker close unproved")
		calls := 0
		f.hooks.close = func(file *os.File) error { calls++; return errors.Join(file.Close(), injected) }
		s, err := f.BeginPhysicalSample()
		if s == nil || !errors.Is(err, ErrFileCloseUncertain) {
			t.Fatalf("lost pre-cursor marker owner: %v", err)
		}
		var actual *FileCloseError
		if !errors.As(err, &actual) || actual.owner != f || actual.file == nil {
			t.Fatal("original marker error missing")
		}
		if !errors.Is(s.Close(), ErrFileCloseUncertain) || calls != 1 {
			t.Fatalf("uncertain marker Close retried: %d", calls)
		}
		if !errors.Is(f.Close(), ErrFilesBusy) {
			t.Fatal("uncertain registration released")
		}
		// Injected failure followed actual Close. Only test disposal may verify
		// that fact and drop bookkeeping; this is not production reconciliation.
		discardTestClosedOwners(t, f)
		f.physical = nil
		f.hooks.close = nil
	})
}

func TestPhysicalCloseUncertaintyRetainsOriginal(t *testing.T) {
	f, _, _ := filesFixture(t)
	s, err := f.BeginPhysicalSample()
	if err != nil {
		t.Fatal(err)
	}
	original := s.frames[0].file
	injected := errors.New("original scan close unproved")
	calls := 0
	f.hooks.close = func(file *os.File) error { calls++; return errors.Join(file.Close(), injected) }
	first := s.Close()
	second := s.Close()
	var actual *FileCloseError
	if !errors.As(first, &actual) || actual.file != original || actual.owner != f || first != second || calls != 1 || f.physical != s {
		t.Fatalf("original Close lost/retried: %v %v calls%d", first, second, calls)
	}
	if _, err := f.BeginPhysicalSample(); !errors.Is(err, ErrFilesBusy) {
		t.Fatalf("replacement after uncertain close: %v", err)
	}
	if !errors.Is(f.Close(), ErrFilesBusy) {
		t.Fatal("root closed across unknown scanner")
	}
	discardTestClosedOwners(t, f)
	f.physical = nil
	f.hooks.close = nil
}

func TestPhysicalDepthAndFDPeak(t *testing.T) {
	for _, depth := range []int{64, 65} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			f, root, _ := filesFixture(t)
			path := ""
			for i := 0; i < depth; i++ {
				if path != "" {
					path += "/"
				}
				path += "d"
				if err := root.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if depth == 64 {
				for i := 0; i < 4097; i++ {
					writeFile(t, root, path+"/"+fmt.Sprint(i), "", 0600)
				}
			}
			owned, peak, markerActive := 0, 0, false
			f.physicalIO.openDir = func(parent *os.File, name string) (*os.File, error) {
				if markerActive {
					t.Fatal("child and marker temporary overlap")
				}
				file, err := openPhysicalDirectory(parent, name)
				if err == nil {
					owned++
					if owned > peak {
						peak = owned
					}
				}
				return file, err
			}
			f.physicalIO.openMarker = func(parent *os.File, name string) (*os.File, error) {
				if markerActive {
					t.Fatal("overlapping marker temporary")
				}
				file, err := openPhysicalMarker(parent, name)
				if err == nil {
					markerActive = true
					if owned+1 > peak {
						peak = owned + 1
					}
				}
				return file, err
			}
			f.hooks.close = func(file *os.File) error {
				info, err := file.Stat()
				if err != nil {
					return err
				}
				if info.IsDir() {
					if markerActive {
						t.Fatal("directory close overlapped marker temporary")
					}
					owned--
				} else if !markerActive {
					t.Fatal("unregistered marker close")
				}
				err = file.Close()
				if !info.IsDir() {
					markerActive = false
				}
				return err
			}
			s := beginPhysical(t, f)
			sample, err := s.Step(context.Background())
			if depth == 64 {
				if err != nil || sample.Status != PhysicalPartial {
					t.Fatalf("deep yielded scan refused: %+v %v", sample, err)
				}
				sample = finishPhysical(t, s)
			} else if err == nil || sample.Status != PhysicalPartial || !sample.EndedAt.IsZero() {
				t.Fatalf("depth65 accepted: %+v %v", sample, err)
			}
			if peak != 66 {
				t.Fatalf("actual scan FD peak%d", peak)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if owned != 0 {
				t.Fatalf("descriptor leak%d", owned)
			}
			f.hooks.close = nil
		})
	}
}

func TestPhysicalObservedHardlinkConflict(t *testing.T) {
	f, root, name := filesFixture(t)
	writeFile(t, root, "first", "original", 0600)
	if err := os.Link(filepath.Join(name, "first"), filepath.Join(name, "second")); err != nil {
		t.Fatal(err)
	}
	seen := false
	f.physicalIO.statAt = func(parent *os.File, name string) (physicalObservation, error) {
		o, err := physicalAt(parent, name)
		if err == nil && (name == "first" || name == "second") {
			if !seen {
				seen = true
				file, e := root.OpenFile(name, os.O_WRONLY|os.O_APPEND, 0)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = file.Write([]byte(" actual later write")); e != nil {
					t.Fatal(e)
				}
				if e = file.Close(); e != nil {
					t.Fatal(e)
				}
			}
		}
		return o, err
	}
	s := beginPhysical(t, f)
	sample, err := s.Step(context.Background())
	if err == nil || sample.Status != PhysicalPartial || !sample.EndedAt.IsZero() {
		t.Fatalf("conflicting actual inode became complete: %+v %v", sample, err)
	}
}

func TestPhysicalPointTimesAndLimits(t *testing.T) {
	for _, kind := range []string{"deadline", "rollback", "quanta", "overflow", "cross-device", "read-error"} {
		t.Run(kind, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			writeFile(t, root, "file", "payload", 0600)
			s := beginPhysical(t, f)
			injected := errors.New("actual read error")
			switch kind {
			case "deadline":
				f.physicalIO.now = func() time.Time { return s.started.Add(physicalDuration + time.Nanosecond) }
			case "rollback":
				f.physicalIO.now = func() time.Time { return s.started.Add(-time.Second) }
			case "quanta":
				s.quanta = physicalTotalQuanta
			case "overflow", "cross-device":
				f.physicalIO.statAt = func(parent *os.File, name string) (physicalObservation, error) {
					o, err := physicalAt(parent, name)
					if name == "file" {
						if kind == "overflow" {
							o.blocks = math.MaxUint64
						} else {
							o.id.Device++
						}
					}
					return o, err
				}
			case "read-error":
				f.physicalIO.readDir = func(file *os.File, n int) ([]string, error) {
					_, err := file.Readdirnames(n)
					return nil, errors.Join(err, injected)
				}
			}
			sample, err := s.Step(context.Background())
			if err == nil || sample.Status != PhysicalPartial || !sample.EndedAt.IsZero() {
				t.Fatalf("%s manufactured complete: %+v %v", kind, sample, err)
			}
			if sample.FreeBytes == nil || sample.FreeObservedAt.IsZero() {
				t.Fatalf("inventory failure erased independent real free point: %+v", sample)
			}
		})
	}
	t.Run("fresh-after-step", func(t *testing.T) {
		f, root, _ := filesFixture(t)
		writeFile(t, root, "file", "payload", 0600)
		clock := time.Now()
		f.physicalIO.now = func() time.Time { return clock }
		s := beginPhysical(t, f)
		f.physicalIO.readDir = func(file *os.File, n int) ([]string, error) {
			entries, err := file.Readdirnames(n)
			clock = clock.Add(time.Second)
			return entries, err
		}
		sample := finishPhysical(t, s)
		if !sample.FreeObservedAt.After(sample.StartedAt) || sample.FreeObservedAt.After(sample.EndedAt) || sample.OldestObservedAt.Equal(sample.FreeObservedAt) {
			t.Fatalf("copied point timestamp: %+v", sample)
		}
	})
}

func TestPhysicalNoFollowAtDirectoryOpen(t *testing.T) {
	f, root, name := filesFixture(t)
	if err := root.Mkdir("directory", 0700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "sentinel"), []byte("never inspect target"), 0600); err != nil {
		t.Fatal(err)
	}
	f.physicalIO.openDir = func(parent *os.File, entry string) (*os.File, error) {
		if entry == "directory" {
			if err := root.Remove(entry); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(name, entry)); err != nil {
				t.Fatal(err)
			}
		}
		return openPhysicalDirectory(parent, entry)
	}
	s := beginPhysical(t, f)
	sample, err := s.Step(context.Background())
	if err == nil || sample.Status != PhysicalPartial || sample.Names < 1 || sample.Names > 2 {
		t.Fatalf("substituted symlink followed: %+v %v", sample, err)
	}
}

func TestPhysicalFailedBeginRetainsOpenedCursor(t *testing.T) {
	f, root, _ := filesFixture(t)
	if err := root.Mkdir("different", 0700); err != nil {
		t.Fatal(err)
	}
	var original *os.File
	f.physicalIO.openDir = func(parent *os.File, name string) (*os.File, error) {
		var err error
		original, err = openPhysicalDirectory(parent, "different")
		return original, err
	}
	s, err := f.BeginPhysicalSample()
	if s == nil || !errors.Is(err, ErrFileConflict) || f.physical != s || len(s.frames) != 1 || s.frames[0].file != original {
		t.Fatalf("failed descriptor verification lost original owner: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := original.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("failed-prefix original descriptor not closed: %v", err)
	}
}

func TestPhysicalIntervalMayMissTransientName(t *testing.T) {
	f, root, _ := filesFixture(t)
	transient := false
	f.physicalIO.readDir = func(file *os.File, n int) ([]string, error) {
		if !transient {
			transient = true
			writeFile(t, root, "transient", "a real name between observations", 0600)
			if err := root.Remove("transient"); err != nil {
				t.Fatal(err)
			}
		}
		return file.Readdirnames(n)
	}
	s := beginPhysical(t, f)
	sample := finishPhysical(t, s)
	if !transient || sample.Names != 1 || sample.Status != PhysicalIntervalComplete {
		t.Fatalf("fixture interval=%+v", sample)
	}
	// The actual transient entry was not observed. IntervalComplete is therefore
	// not all-live-name coverage, a simultaneous total, peak usage or hard quota.
}

func TestPhysicalOriginalRootSurvivesHostRename(t *testing.T) {
	f, _, name := filesFixture(t)
	s := beginPhysical(t, f)
	if err := os.Rename(name, name+"-original"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(name, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(name, "foreign"), []byte("new host pathname"), 0600); err != nil {
		t.Fatal(err)
	}
	sample := finishPhysical(t, s)
	if sample.Names != 1 {
		t.Fatalf("adopted new host directory: %+v", sample)
	}
	data, err := os.ReadFile(filepath.Join(name, "foreign"))
	if err != nil || string(data) != "new host pathname" {
		t.Fatalf("foreign path touched: %q %v", data, err)
	}
	// Restore the host pathname only after this original scan positively closes;
	// the pre-existing fixture cleanups then close Files/Root before TempDir removal.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(name, "foreign")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(name+"-original", name); err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalObservedDirectoryReplacement(t *testing.T) {
	f, root, name := filesFixture(t)
	if err := root.Mkdir("directory", 0700); err != nil {
		t.Fatal(err)
	}
	replaced := false
	f.physicalIO.readDir = func(file *os.File, n int) ([]string, error) {
		if file.Name() == "directory" && !replaced {
			replaced = true
			if err := os.Rename(filepath.Join(name, "directory"), filepath.Join(name, "retained-original")); err != nil {
				t.Fatal(err)
			}
			if err := root.Mkdir("directory", 0700); err != nil {
				t.Fatal(err)
			}
		}
		return file.Readdirnames(n)
	}
	s := beginPhysical(t, f)
	sample, err := s.Step(context.Background())
	if !replaced || err == nil || sample.Status != PhysicalPartial || !sample.EndedAt.IsZero() {
		t.Fatalf("observed directory replacement accepted: %+v %v", sample, err)
	}
}

// This real total-visit boundary is deliberately opt-in: marker+bulk directory
// plus65534/65535 names, not a fake DirEntry stream. It needs a separate released large-policy
// filesystem gate; ordinary small fixture tests do not qualify this boundary.
func TestPhysicalMaximumNamePass(t *testing.T) {
	if os.Getenv("SKYGO_PHYSICAL_MAXIMUM") != "1" {
		t.Skip("requires explicit large-policy filesystem gate")
	}
	for _, names := range []int{65534, 65535} {
		t.Run(fmt.Sprint(names), func(t *testing.T) {
			f, root, name := filesFixture(t)
			if err := root.Mkdir("bulk", 0700); err != nil {
				t.Fatal(err)
			}
			// Groups of at most1024 aliases avoid filesystem link-count ceilings;
			// all real names still traverse the original descriptor cursor; the marker
			// and bulk directory add two visits to the independently counted total.
			for i := 0; i < names; i++ {
				entry := fmt.Sprintf("bulk/entry-%05d", i)
				if i%1024 == 0 {
					writeFile(t, root, entry, "shared inode", 0600)
				} else if err := os.Link(filepath.Join(name, fmt.Sprintf("bulk/entry-%05d", i/1024*1024)), filepath.Join(name, entry)); err != nil {
					t.Fatal(err)
				}
			}
			s := beginPhysical(t, f)
			var sample PhysicalSample
			var err error
			for i := 0; i < 32; i++ {
				sample, err = s.Step(context.Background())
				if err != nil || sample.Status == PhysicalIntervalComplete {
					break
				}
			}
			if names == 65534 {
				if err != nil || sample.Status != PhysicalIntervalComplete || sample.Visits != 65536 || *sample.Inodes != uint64(3+(names+1023)/1024) {
					t.Fatalf("exact actual name envelope: %+v %v", sample, err)
				}
			} else if err == nil || sample.Status != PhysicalPartial || !sample.EndedAt.IsZero() {
				t.Fatalf("over-limit actual names complete: %+v %v", sample, err)
			}
		})
	}
}

func TestPhysicalStepUncertainChildNoNewProbe(t *testing.T) {
	f, root, _ := filesFixture(t)
	if err := root.Mkdir("child", 0700); err != nil {
		t.Fatal(err)
	}
	s, err := f.BeginPhysicalSample()
	if err != nil {
		t.Fatal(err)
	}
	markers, childCloses := 0, 0
	var original *os.File
	injected := errors.New("child original close uncertain")
	f.physicalIO.openMarker = func(parent *os.File, name string) (*os.File, error) {
		markers++
		return openPhysicalMarker(parent, name)
	}
	f.hooks.close = func(file *os.File) error {
		if file.Name() == "child" {
			original = file
			childCloses++
			return errors.Join(file.Close(), injected)
		}
		return file.Close()
	}
	sample, err := s.Step(context.Background())
	var actual *FileCloseError
	if !errors.As(err, &actual) || actual.file != original || actual.owner != f || !errors.Is(err, ErrFileCloseUncertain) || sample.Status != PhysicalPartial || !sample.EndedAt.IsZero() || markers != 1 {
		t.Fatalf("uncertain Step lost original or opened later probe: %+v %v markers%d", sample, err, markers)
	}
	if !errors.Is(s.Close(), ErrFileCloseUncertain) || !errors.Is(s.Close(), ErrFileCloseUncertain) || childCloses != 1 || markers != 1 || f.physical != s {
		t.Fatal("uncertain original replaced/retried or registration released")
	}
	discardTestClosedOwners(t, f)
	f.physical = nil
	f.hooks.close = nil
}

func TestPhysicalFreeFailureNeverMeasuredZero(t *testing.T) {
	f, _, _ := filesFixture(t)
	s := beginPhysical(t, f)
	injected := errors.New("original Fstatfs failed")
	f.physicalIO.free = func(file *os.File) (uint64, error) {
		if file != f.directory {
			t.Fatal("statfs used replacement root")
		}
		return 0, injected
	}
	sample, err := s.Step(context.Background())
	if !errors.Is(err, injected) || sample.Status != PhysicalPartial || sample.FreeBytes != nil || !sample.FreeObservedAt.IsZero() || !sample.EndedAt.IsZero() {
		t.Fatalf("failed free probe invented zero/fresh interval: %+v %v", sample, err)
	}
}

func TestPhysicalCanonicalMarkerReplacement(t *testing.T) {
	f, root, name := filesFixture(t)
	bytes := readFile(t, root, rootMarkerName)
	old, err := root.Open(rootMarkerName)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	s := beginPhysical(t, f)
	replaced := false
	f.physicalIO.readDir = func(file *os.File, n int) ([]string, error) {
		if !replaced {
			replaced = true
			writeFile(t, root, "replacement-marker", string(bytes), 0600)
			if err := os.Rename(filepath.Join(name, "replacement-marker"), filepath.Join(name, rootMarkerName)); err != nil {
				t.Fatal(err)
			}
		}
		return file.Readdirnames(n)
	}
	sample, err := s.Step(context.Background())
	if !replaced || err == nil || sample.Status != PhysicalPartial || !sample.EndedAt.IsZero() {
		t.Fatalf("same-bytes marker inode was adopted: %+v %v", sample, err)
	}
}
