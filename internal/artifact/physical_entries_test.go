//go:build linux || darwin

package artifact

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestPhysicalRootEntriesActual(t *testing.T) {
	f, root, name := filesFixture(t)
	writeFile(t, root, "data", "payload", 0600)
	if err := os.Link(filepath.Join(name, "data"), filepath.Join(name, "alias")); err != nil {
		t.Fatal(err)
	}
	if err := root.Mkdir("directory", 0700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, root, "directory/nested", "not first level", 0600)
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("never read"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(name, "symlink")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(name, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	want := map[string]PhysicalEntryType{rootMarkerName: PhysicalEntryRegular, "data": PhysicalEntryRegular, "alias": PhysicalEntryRegular, "directory": PhysicalEntryDirectory, "symlink": PhysicalEntryOther, "fifo": PhysicalEntryOther}
	ids := make(map[string]FileID)
	for entry := range want {
		info, err := os.Lstat(filepath.Join(name, entry))
		if err != nil {
			t.Fatal(err)
		}
		stat := info.Sys().(*syscall.Stat_t)
		ids[entry] = FileID{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}
	}
	directories, markers := 0, 0
	f.physicalIO.openDir = func(parent *os.File, entry string) (*os.File, error) {
		directories++
		if entry != "." && entry != "directory" {
			t.Fatalf("projection opened workload/special %q", entry)
		}
		return openPhysicalDirectory(parent, entry)
	}
	f.physicalIO.openMarker = func(parent *os.File, entry string) (*os.File, error) {
		markers++
		if entry != rootMarkerName {
			t.Fatalf("projection read non-marker %q", entry)
		}
		return openPhysicalMarker(parent, entry)
	}
	s := beginPhysical(t, f)
	sample := finishPhysical(t, s)
	if len(sample.RootEntries) != 6 {
		t.Fatalf("root projection: %+v", sample.RootEntries)
	}
	seen := make(map[string]bool)
	for _, entry := range sample.RootEntries {
		kind, ok := want[entry.Name]
		if !ok || seen[entry.Name] || entry.Type != kind || entry.File != ids[entry.Name] {
			t.Fatalf("unexpected observed root entry %+v", entry)
		}
		seen[entry.Name] = true
	}
	if sample.Names != 7 || directories != 2 || markers != 3 {
		t.Fatalf("projection added opens or omitted nested accounting: names%d directories%d markers%d", sample.Names, directories, markers)
	}
	if ids["data"] != ids["alias"] {
		t.Fatal("fixture hardlink missing")
	}
	// Other is a diagnostic type. Files does not decide which entries a caller's
	// actual cohort explains and grants no adoption/removal authority from it.
}

func TestPhysicalRootEntriesYieldedCopies(t *testing.T) {
	f, root, _ := filesFixture(t)
	if err := root.Mkdir("bulk", 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4097; i++ {
		writeFile(t, root, fmt.Sprintf("bulk/%04d", i), "", 0600)
	}
	s := beginPhysical(t, f)
	first, err := s.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != PhysicalPartial || first.Visits != 4096 || len(first.RootEntries) == 0 || len(first.RootEntries) > 2 {
		t.Fatalf("missing yielded projection: %+v", first)
	}
	old := append([]PhysicalEntry(nil), first.RootEntries...)
	first.RootEntries[0] = PhysicalEntry{Name: "caller-mutated", Type: PhysicalEntryOther}
	second := finishPhysical(t, s)
	if len(second.RootEntries) != 2 || second.Names != 4099 {
		t.Fatalf("missing complete projection: %+v", second)
	}
	for _, entry := range old {
		found := false
		for _, later := range second.RootEntries {
			if entry == later {
				found = true
			}
		}
		if !found {
			t.Fatalf("caller changed retained observation %+v", entry)
		}
	}
	second.RootEntries[0].Name = "second-mutated"
	third, err := s.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if third.Status != PhysicalIntervalComplete || len(third.RootEntries) != 2 {
		t.Fatalf("terminal snapshot: %+v", third)
	}
	for _, entry := range third.RootEntries {
		if entry.Name == "caller-mutated" || entry.Name == "second-mutated" {
			t.Fatal("returned slices alias scan")
		}
	}
}

func TestPhysicalRootEntryCountEnvelope(t *testing.T) {
	// Independent literal policy boundary: marker plus4095/4096 real files,
	// with no synthetic directory entries or lowered total-visit ceilings.
	for _, files := range []int{4095, 4096} {
		t.Run(fmt.Sprint(files), func(t *testing.T) {
			f, root, _ := filesFixture(t)
			for i := 0; i < files; i++ {
				writeFile(t, root, fmt.Sprintf("entry-%04d", i), "", 0600)
			}
			s := beginPhysical(t, f)
			var sample PhysicalSample
			var err error
			for i := 0; i < 3; i++ {
				sample, err = s.Step(context.Background())
				if err != nil || sample.Status == PhysicalIntervalComplete {
					break
				}
			}
			if files == 4095 {
				if err != nil || sample.Status != PhysicalIntervalComplete || len(sample.RootEntries) != 4096 || sample.Visits != 4096 {
					t.Fatalf("exact root envelope %+v %v", sample, err)
				}
			} else {
				if err == nil || sample.Status != PhysicalPartial || !sample.EndedAt.IsZero() || len(sample.RootEntries) != 4096 || sample.Visits != 4097 {
					t.Fatalf("overflow manufactured usable interval %+v %v", sample, err)
				}
				found := false
				for _, condition := range sample.Conditions {
					if condition == "root-entries" {
						found = true
					}
				}
				if !found {
					t.Fatalf("wrong overflow condition %v", sample.Conditions)
				}
			}
		})
	}
}

func TestPhysicalRootEntrySharedPayloadEnvelope(t *testing.T) {
	// Fault-injected logical ledger, not an actual allocation/RSS experiment.
	// Independent literals reserve160 bytes for the new inode record and then
	// exactly name+64 for the root projection in the SAME16MiB pass allowance.
	for _, excess := range []int{0, 1} {
		t.Run(fmt.Sprint(excess), func(t *testing.T) {
			f, _, _ := filesFixture(t)
			s := beginPhysical(t, f)
			seeded := false
			f.physicalIO.statAt = func(parent *os.File, name string) (physicalObservation, error) {
				o, err := physicalAt(parent, name)
				if err == nil && name == rootMarkerName && !seeded {
					seeded = true
					s.payload = (16 << 20) - 160 - len(name) - 64 + excess
				}
				return o, err
			}
			sample, err := s.Step(context.Background())
			if !seeded {
				t.Fatal("did not reach actual root stat")
			}
			if excess == 0 {
				if err != nil || sample.Status != PhysicalIntervalComplete || len(sample.RootEntries) != 1 {
					t.Fatalf("exact shared allowance refused: %+v %v", sample, err)
				}
			} else {
				if err == nil || sample.Status != PhysicalPartial || !sample.EndedAt.IsZero() || len(sample.RootEntries) != 0 {
					t.Fatalf("projection exceeded shared allowance: %+v %v", sample, err)
				}
				if errors.Is(err, ErrFileCloseUncertain) {
					t.Fatal("ledger refusal fabricated Close uncertainty")
				}
			}
		})
	}
}
