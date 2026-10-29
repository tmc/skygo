package kube

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// treeImage returns every entry under dir with its mode and content digest,
// so that a refusal can be checked to have written nothing.
func treeImage(t *testing.T, dir string) string {
	t.Helper()
	var b strings.Builder
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		fmt.Fprintf(&b, "%s %v", rel, info.Mode())
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			fmt.Fprintf(&b, " %x", sha256.Sum256(data))
		}
		b.WriteByte('\n')
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func writeDRAFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestDRAStatePreflight(t *testing.T) {
	b, r, _, _ := draUnit(t)
	key := draScopeKey{"owner", b.namespace, b.binding}
	marker := `{"format":"skygo-kubernetes-dra-state-v1","owner":"owner"}`
	scope := draScope(key)
	record := func(f func(*draRecord)) string {
		x := r
		x.Format, x.Generation, x.State = draRecordFormat, 1, draOpen
		x.Owner, x.Namespace, x.Binding = key.owner, key.namespace, key.binding
		if f != nil {
			f(&x)
		}
		data, err := jsonMarshal(&x)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, root string)
		err   string
	}{
		{"unknown file", func(t *testing.T, root string) { writeDRAFile(t, filepath.Join(root, "junk"), "x") }, "unexpected"},
		{"legacy receipt scope", func(t *testing.T, root string) {
			legacy := filepath.Join(root, receiptScope("owner", b.namespace, b.binding))
			os.Mkdir(legacy, 0o700)
			writeDRAFile(t, filepath.Join(legacy, testID+".json"), `{"format":"skygo-kubernetes-execution-receipt-v1"}`)
		}, "unexpected"},
		{"legacy receipt file at root", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, testID+".json"), "{}")
		}, "unexpected"},
		{"scope without marker", func(t *testing.T, root string) { os.Mkdir(filepath.Join(root, scope), 0o700) }, "without its marker"},
		{"other owner", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), `{"format":"skygo-kubernetes-dra-state-v1","owner":"other"}`)
		}, "another installation"},
		// MODELED corruption: an edited header is not a genuine old binary.
		{"other format", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), `{"format":"skygo-kubernetes-dra-state-v0","owner":"owner"}`)
		}, "unsupported"},
		{"noncanonical marker", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), `{"owner":"owner","format":"skygo-kubernetes-dra-state-v1"}`)
		}, "corrupt"},
		{"scope of another binding", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), marker)
			os.Mkdir(filepath.Join(root, draScope(draScopeKey{"owner", b.namespace, "sha256:other"})), 0o700)
		}, "unexpected"},
		{"corrupt record", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), marker)
			os.Mkdir(filepath.Join(root, scope), 0o700)
			writeDRAFile(t, filepath.Join(root, scope, testID+".json"), record(nil)+" ")
		}, "corrupt"},
		{"pod sent before claim pinned", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), marker)
			os.Mkdir(filepath.Join(root, scope), 0o700)
			writeDRAFile(t, filepath.Join(root, scope, testID+".json"), record(func(x *draRecord) { x.Claim.Send, x.Pod.Send = phaseSent, phaseSent }))
		}, "corrupt"},
		{"unknown scope entry", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), marker)
			os.Mkdir(filepath.Join(root, scope), 0o700)
			writeDRAFile(t, filepath.Join(root, scope, "notes.txt"), "x")
		}, "unexpected"},
		{"public marker", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), marker)
			os.Chmod(filepath.Join(root, draMarkerName), 0o644)
		}, "permissions"},
		// Interrupted-write temporaries are never attributed, removed or
		// replaced, whatever they hold.
		{"foreign marker tmp", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName+".tmp"), `{"format":"skygo-kubernetes-dra-state-v1","owner":"other"}`)
		}, "interrupted write"},
		{"corrupt marker tmp", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName+".tmp"), "\x00corrupt")
		}, "interrupted write"},
		{"own marker tmp", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName+".tmp"), marker)
		}, "interrupted write"},
		{"public marker tmp", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName+".tmp"), marker)
			os.Chmod(filepath.Join(root, draMarkerName+".tmp"), 0o644)
		}, "interrupted write"},
		{"hardlinked marker tmp", func(t *testing.T, root string) {
			other := filepath.Join(t.TempDir(), "other")
			writeDRAFile(t, other, marker)
			if err := os.Link(other, filepath.Join(root, draMarkerName+".tmp")); err != nil {
				t.Fatal(err)
			}
		}, "interrupted write"},
		{"foreign record tmp", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), marker)
			os.Mkdir(filepath.Join(root, scope), 0o700)
			writeDRAFile(t, filepath.Join(root, scope, testID+".tmp"), record(func(x *draRecord) { x.Owner = "other" }))
		}, "interrupted write"},
		{"corrupt record tmp", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), marker)
			os.Mkdir(filepath.Join(root, scope), 0o700)
			writeDRAFile(t, filepath.Join(root, scope, testID+".tmp"), "{")
		}, "interrupted write"},
		{"public record tmp", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), marker)
			os.Mkdir(filepath.Join(root, scope), 0o700)
			writeDRAFile(t, filepath.Join(root, scope, testID+".tmp"), record(nil))
			os.Chmod(filepath.Join(root, scope, testID+".tmp"), 0o644)
		}, "interrupted write"},
		{"hardlinked record tmp", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), marker)
			os.Mkdir(filepath.Join(root, scope), 0o700)
			other := filepath.Join(t.TempDir(), "other")
			writeDRAFile(t, other, record(nil))
			if err := os.Link(other, filepath.Join(root, scope, testID+".tmp")); err != nil {
				t.Fatal(err)
			}
		}, "interrupted write"},
		{"public lock", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), marker)
			os.Mkdir(filepath.Join(root, scope), 0o700)
			writeDRAFile(t, filepath.Join(root, scope, journalLockName), "")
			os.Chmod(filepath.Join(root, scope, journalLockName), 0o644)
		}, "permissions"},
		{"nonempty lock", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), marker)
			os.Mkdir(filepath.Join(root, scope), 0o700)
			writeDRAFile(t, filepath.Join(root, scope, journalLockName), "x")
		}, "lock content"},
		{"public operation lock", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), marker)
			os.Mkdir(filepath.Join(root, scope), 0o700)
			writeDRAFile(t, filepath.Join(root, scope, operationLockName), "")
			os.Chmod(filepath.Join(root, scope, operationLockName), 0o644)
		}, "permissions"},
		{"nonempty operation lock", func(t *testing.T, root string) {
			writeDRAFile(t, filepath.Join(root, draMarkerName), marker)
			os.Mkdir(filepath.Join(root, scope), 0o700)
			writeDRAFile(t, filepath.Join(root, scope, operationLockName), "x")
		}, "lock content"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, exec := receiptDir(t), receiptDir(t)
			tc.setup(t, root)
			before := treeImage(t, root)
			_, err := openDRAState(root, exec, "owner", []draScopeKey{key})
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("err = %v, want %q", err, tc.err)
			}
			if after := treeImage(t, root); after != before {
				t.Fatalf("refusal wrote state:\nbefore:\n%s\nafter:\n%s", before, after)
			}
		})
	}
	t.Run("fresh then restart", func(t *testing.T) {
		root, exec := receiptDir(t), receiptDir(t)
		if _, err := openDRAState(root, exec, "owner", []draScopeKey{key}); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(root, draMarkerName)); err != nil {
			t.Fatal(err)
		}
		writeDRAFile(t, filepath.Join(root, scope, testID+".json"), record(nil))
		// Restart accepts the exact same-version root and its records.
		if _, err := openDRAState(root, exec, "owner", []draScopeKey{key}); err != nil {
			t.Fatalf("restart: %v", err)
		}
	})
	t.Run("execution state overlap", func(t *testing.T) {
		root := receiptDir(t)
		before := treeImage(t, root)
		for name, exec := range map[string]string{"same": root, "nested": filepath.Join(root, "exec")} {
			if name == "nested" {
				os.Mkdir(exec, 0o700)
				before = treeImage(t, root)
			}
			if _, err := openDRAState(root, exec, "owner", []draScopeKey{key}); err == nil || !strings.Contains(err.Error(), "separate") {
				t.Errorf("%s: err = %v", name, err)
			}
			if after := treeImage(t, root); after != before {
				t.Errorf("%s: refusal wrote state", name)
			}
		}
	})
	t.Run("symlink root", func(t *testing.T) {
		dir, exec := receiptDir(t), receiptDir(t)
		real := filepath.Join(dir, "real")
		os.Mkdir(real, 0o700)
		link := filepath.Join(dir, "link")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		if _, err := openDRAState(link, exec, "owner", []draScopeKey{key}); err == nil {
			t.Fatal("symlinked root accepted")
		}
		if entries, _ := os.ReadDir(real); len(entries) != 0 {
			t.Fatal("refusal wrote through the symlink")
		}
	})
}

func jsonMarshal(r *draRecord) (string, error) {
	data, err := json.Marshal(r)
	return string(data), err
}

func TestDRARecordTransitions(t *testing.T) {
	b, r, _, _ := draUnit(t)
	key := draScopeKey{"owner", b.namespace, b.binding}
	stores, err := openDRAState(receiptDir(t), receiptDir(t), "owner", []draScopeKey{key})
	if err != nil {
		t.Fatal(err)
	}
	d := stores[key]
	if err := d.create(r); err != nil {
		t.Fatal(err)
	}
	if err := d.create(r); !errors.Is(err, errReceiptExists) {
		t.Fatalf("second create: %v", err)
	}
	// No Pod grant before the claim is pinned.
	if _, err := d.update(testID, consumeDRA(true, r.Pod.Nonce)); err == nil {
		t.Fatal("pod grant consumed before claim pin")
	}
	if _, err := d.update(testID, consumeDRA(false, r.Claim.Nonce)); err != nil {
		t.Fatal(err)
	}
	// A consumed grant is never consumed again, in any process.
	again := &draStore{s: restartedStore(d.s), key: key}
	if _, err := again.update(testID, consumeDRA(false, r.Claim.Nonce)); err == nil {
		t.Fatal("claim grant consumed twice")
	}
	if _, err := d.update(testID, func(x *draRecord) error { return pinDRA(&x.Claim, r.Claim.Nonce, "claim-uid") }); err != nil {
		t.Fatal(err)
	}
	if _, err := d.update(testID, func(x *draRecord) error { return pinDRA(&x.Claim, r.Claim.Nonce, "other-uid") }); !errors.Is(err, errReceiptPinned) {
		t.Fatalf("repin: %v", err)
	}
	// Rollback to an older valid copy is refused once seen.
	path := filepath.Join(d.s.dir, testID+".json")
	old, _ := os.ReadFile(path)
	if _, err := d.update(testID, consumeDRA(true, r.Pod.Nonce)); err != nil {
		t.Fatal(err)
	}
	writeDRAFile(t, path, string(old))
	if _, err := d.load(testID); err == nil || !strings.Contains(err.Error(), "rolled back") {
		t.Fatalf("rollback: %v", err)
	}
}

func TestDRARecordCapacity(t *testing.T) {
	b, r, _, _ := draUnit(t)
	key := draScopeKey{"owner", b.namespace, b.binding}
	stores, err := openDRAState(receiptDir(t), receiptDir(t), "owner", []draScopeKey{key})
	if err != nil {
		t.Fatal(err)
	}
	d := stores[key]
	d.s.limit = 1
	if err := d.create(r); err != nil {
		t.Fatal(err)
	}
	_, r2, _, _ := draUnit(t)
	r2.Attempt = otherID
	r2.Pod.Name = "skygo-" + otherID
	r2.Claim.Name, _ = draClaimName(otherID, r2.Claim.Nonce)
	if err := d.create(r2); !errors.Is(err, errReceiptCapacity) {
		t.Fatalf("capacity: %v", err)
	}
}

func TestDRARecordFault(t *testing.T) {
	b, r, _, _ := draUnit(t)
	key := draScopeKey{"owner", b.namespace, b.binding}
	stores, err := openDRAState(receiptDir(t), receiptDir(t), "owner", []draScopeKey{key})
	if err != nil {
		t.Fatal(err)
	}
	d := stores[key]
	if err := d.create(r); err != nil {
		t.Fatal(err)
	}
	d.fault = func(step string, x *draRecord) error {
		if step == "dirsync" && x.Claim.Send == phaseSent {
			return errors.New("modeled fsync failure")
		}
		return nil
	}
	var uncertain receiptUncertain
	if _, err := d.update(testID, consumeDRA(false, r.Claim.Nonce)); !errors.As(err, &uncertain) {
		t.Fatalf("uncertain write: %v", err)
	}
}
