package artifact

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func expectedRoot(t *testing.T, root *os.Root) FileID {
	t.Helper()
	info, err := root.Lstat(".")
	if err != nil {
		t.Fatal(err)
	}
	m, err := fileMetadata(info)
	if err != nil {
		t.Fatal(err)
	}
	return fileID(m)
}
func TestFilesInstallationBinding(t *testing.T) {
	root, _ := testRoot(t)
	expected := expectedRoot(t, root)
	if _, err := Open(root, expected); err == nil {
		t.Fatal("missing installation marker accepted")
	}
	facts, err := Initialize(root, expected, strings.Repeat("e", 32))
	if err != nil {
		t.Fatal(err)
	}
	if facts.ID != strings.Repeat("e", 32) || facts.File != expected || facts.Volume != expected.Device || facts.Unit < 512 || facts.Unit > 65536 || facts.Unit&(facts.Unit-1) != 0 || facts.MetadataFiles != 2 || facts.MetadataAllowance != 2*facts.Unit {
		t.Fatalf("wrong observed facts: %+v", facts)
	}
	f, err := Open(root, expected)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	observed, err := f.RootFacts()
	if err != nil || observed != facts {
		t.Fatalf("facts changed: %+v %v", observed, err)
	}
	if _, err := Initialize(root, expected, facts.ID); err == nil {
		t.Fatal("existing marker silently adopted by initialization")
	}
	// A same-volume copied marker cannot bind another directory inode.
	other, _ := testRoot(t)
	data := readFile(t, root, rootMarkerName)
	writeFile(t, other, rootMarkerName, string(data), 0600)
	if _, err := Open(other, expectedRoot(t, other)); err == nil {
		t.Fatal("copied marker rebound another inode")
	}
}
func TestFilesInitializationDoesNotAdopt(t *testing.T) {
	root, _ := testRoot(t)
	writeFile(t, root, "unexplained", "sentinel", 0600)
	if _, err := Initialize(root, expectedRoot(t, root), strings.Repeat("f", 32)); err == nil {
		t.Fatal("populated root adopted")
	}
	if _, err := root.Lstat(rootMarkerName); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("marker created despite nonempty root")
	}
	if string(readFile(t, root, "unexplained")) != "sentinel" {
		t.Fatal("unexplained existing file changed")
	}
}
func TestFilesMarkerPolicy(t *testing.T) {
	for _, kind := range []string{"id", "version", "unit", "volume", "inode", "unknown", "duplicate", "oversized", "hardlink", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			f, root, name := filesFixture(t)
			data := readFile(t, root, rootMarkerName)
			var marker rootMarker
			if err := json.Unmarshal(data, &marker); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "id":
				marker.ID = "invalid"
			case "version":
				marker.Version++
			case "unit":
				marker.Unit *= 2
			case "volume":
				marker.Volume++
			case "inode":
				marker.Inode++
			case "unknown":
				data = bytes.Replace(data, []byte("{\"version\""), []byte("{\"extra\":1,\"version\""), 1)
			case "duplicate":
				data = bytes.Replace(data, []byte("{\"version\""), []byte("{\"version\":1,\"version\""), 1)
			case "oversized":
				data = make([]byte, markerLimit+1)
			case "hardlink":
				if err := os.Link(filepath.Join(name, rootMarkerName), filepath.Join(name, "alias")); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				outside := filepath.Join(t.TempDir(), "marker")
				if err := os.WriteFile(outside, data, 0600); err != nil {
					t.Fatal(err)
				}
				if err := root.Remove(rootMarkerName); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(name, rootMarkerName)); err != nil {
					t.Fatal(err)
				}
			}
			if kind != "hardlink" && kind != "symlink" {
				if kind != "unknown" && kind != "duplicate" && kind != "oversized" {
					var err error
					data, err = json.Marshal(marker)
					if err != nil {
						t.Fatal(err)
					}
					data = append(data, '\n')
				}
				if err := os.WriteFile(filepath.Join(name, rootMarkerName), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.RootFacts(); err == nil {
				t.Fatalf("%s marker accepted", kind)
			}
			if _, err := Open(root, expectedRoot(t, root)); err == nil {
				t.Fatalf("%s marker admitted by constructor", kind)
			}
		})
	}
}
