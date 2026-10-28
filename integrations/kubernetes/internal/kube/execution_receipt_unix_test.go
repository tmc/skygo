//go:build linux || darwin

package kube

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestReceiptStoreRefusesReplacedState replaces the root, the scope
// directory or a state file after the store was opened. Every acquisition
// refuses before reading or writing a record, and none blocks.
func TestReceiptStoreRefusesReplacedState(t *testing.T) {
	journal := []string{"load", "update", "create", "retire"}
	operation := []string{"operate", "exclusive"}
	all := append(append([]string(nil), journal...), operation...)
	for _, tt := range []struct {
		name string
		ops  []string
		// replace changes the state and returns the actual scope directory
		// now holding the original records. A path it renames outside the
		// fixture directory is transferred to the fixture's custody.
		replace func(t *testing.T, f *receiptFixture, s *receiptStore) string
	}{
		{"scope-mode", all, func(t *testing.T, _ *receiptFixture, s *receiptStore) string {
			chmod(t, s.dir, 0o755)
			return s.dir
		}},
		{"root-mode", all, func(t *testing.T, _ *receiptFixture, s *receiptStore) string {
			chmod(t, s.root, 0o755)
			return s.dir
		}},
		{"scope-symlink", all, func(t *testing.T, _ *receiptFixture, s *receiptStore) string {
			moved := s.dir + "-moved"
			rename(t, s.dir, moved)
			symlink(t, moved, s.dir)
			return moved
		}},
		{"scope-replaced", all, func(t *testing.T, _ *receiptFixture, s *receiptStore) string {
			moved := s.dir + "-moved"
			rename(t, s.dir, moved)
			mkdir(t, s.dir)
			return moved
		}},
		{"root-symlink", all, func(t *testing.T, f *receiptFixture, s *receiptStore) string {
			moved := movedRoot(t, f, s)
			symlink(t, moved, s.root)
			return filepath.Join(moved, s.scope)
		}},
		{"root-replaced", all, func(t *testing.T, f *receiptFixture, s *receiptStore) string {
			moved := movedRoot(t, f, s)
			mkdir(t, s.root)
			mkdir(t, s.dir)
			return filepath.Join(moved, s.scope)
		}},
		{"journal-lock-fifo", journal, func(t *testing.T, _ *receiptFixture, s *receiptStore) string {
			fifo(t, filepath.Join(s.dir, journalLockName))
			return s.dir
		}},
		{"operation-lock-fifo", operation, func(t *testing.T, _ *receiptFixture, s *receiptStore) string {
			fifo(t, filepath.Join(s.dir, operationLockName))
			return s.dir
		}},
		{"receipt-fifo", journal, func(t *testing.T, _ *receiptFixture, s *receiptStore) string {
			fifo(t, receiptFile(s, testID))
			return s.dir
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newReceiptFixture(t)
			s, err := openReceiptStore(fixture.dir, "owner", "workloads", testBinding)
			if err != nil {
				t.Fatal(err)
			}
			pinTestReceipt(t, s, testReceipt(testID))
			before, err := os.ReadFile(receiptFile(s, testID))
			if err != nil {
				t.Fatal(err)
			}
			actual := tt.replace(t, fixture, s)
			for _, name := range tt.ops {
				op := fixture.run(name, func() error { return replacedOp(s, name) })
				if err := op.wait(t); err == nil {
					t.Errorf("%s accepted replaced state", name)
				}
			}
			path := filepath.Join(actual, testID+".json")
			if tt.name == "receipt-fifo" {
				// Never open the FIFO here: a blocking open would hang.
				info, err := os.Lstat(path)
				if err != nil || info.Mode()&os.ModeNamedPipe == 0 {
					t.Fatalf("receipt FIFO replaced: %v", err)
				}
			} else if after, err := os.ReadFile(path); err != nil || !bytes.Equal(before, after) {
				t.Fatalf("original receipt changed: %v", err)
			}
			if _, err := os.Lstat(filepath.Join(actual, otherID+".json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("refused create wrote a receipt")
			}
		})
	}
}

func replacedOp(s *receiptStore, name string) error {
	switch name {
	case "load":
		_, err := s.load(testID)
		return err
	case "update":
		_, err := s.update(testID, func(x *executionReceipt) error { x.Cancel = stepIntent; return nil })
		return err
	case "create":
		return s.create(testReceipt(otherID))
	case "retire":
		return s.retire(testID, func(*executionReceipt) error { return nil })
	case "operate":
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		release, err := s.operate(ctx)
		if err == nil {
			release()
		}
		return err
	case "exclusive":
		release, err := s.exclusive()
		if err == nil {
			release()
		}
		return err
	}
	return fmt.Errorf("unknown operation %q", name)
}

// movedRoot renames the store root aside. The renamed root is transferred
// to f's custody first, so that it is removed with f's directory, only after
// every original operation of f has returned, and retained with it
// otherwise.
func movedRoot(t *testing.T, f *receiptFixture, s *receiptStore) string {
	t.Helper()
	moved := s.root + "-moved"
	f.own(moved)
	rename(t, s.root, moved)
	return moved
}

func chmod(t *testing.T, name string, mode os.FileMode) {
	t.Helper()
	if err := os.Chmod(name, mode); err != nil {
		t.Fatal(err)
	}
}

func rename(t *testing.T, from, to string) {
	t.Helper()
	if err := os.Rename(from, to); err != nil {
		t.Fatal(err)
	}
}

func symlink(t *testing.T, target, name string) {
	t.Helper()
	if err := os.Symlink(target, name); err != nil {
		t.Fatal(err)
	}
}

func mkdir(t *testing.T, name string) {
	t.Helper()
	if err := os.Mkdir(name, 0o700); err != nil {
		t.Fatal(err)
	}
}

func fifo(t *testing.T, name string) {
	t.Helper()
	if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(name, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestReceiptBackendRefusesReplacedScope replaces the scope directory of a
// constructed Backend. Start and Observe refuse without API requests.
func TestReceiptBackendRefusesReplacedScope(t *testing.T) {
	ctx := context.Background()
	api, url, root := newReceiptAPI(t)
	b := newReceiptBackend(t, root, url)
	ref := receiptRef(b, "Pod")
	if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	moved := b.receipts.dir + "-moved"
	rename(t, b.receipts.dir, moved)
	symlink(t, moved, b.receipts.dir)
	requests, writes := api.counts()
	next := ref
	next.ID = otherID
	if err := b.Start(ctx, next.ID, next.Task); err == nil {
		t.Fatal("start through a replaced scope")
	}
	if _, err := b.Observe(ctx, ref); err == nil {
		t.Fatal("observe through a replaced scope")
	}
	if n, w := api.counts(); n != requests || w != writes {
		t.Fatal("replaced scope reached the API")
	}
	if _, err := os.Lstat(filepath.Join(moved, otherID+".json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("replaced scope recorded an attempt")
	}
	// Positive twin: restoring the original directory restores operation.
	if err := os.Remove(b.receipts.dir); err != nil {
		t.Fatal(err)
	}
	rename(t, moved, b.receipts.dir)
	if status, err := b.Observe(ctx, ref); err != nil || status.Instance == "" {
		t.Fatalf("restored observe: %+v %v", status, err)
	}
}

// TestReceiptOperationLockProcess holds a shared operation permit while a
// separate process of this test binary attempts cleanup's exclusive permit.
func TestReceiptOperationLockProcess(t *testing.T) {
	if root := os.Getenv("SKYGO_RECEIPT_LOCK_ROOT"); root != "" {
		receiptLockHelper(root)
		return
	}
	s, root := testStore(t)
	release, err := s.operate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	held := true
	defer func() {
		if held {
			release()
		}
	}()
	if out := runReceiptLockHelper(t, root); out != "busy" {
		t.Fatalf("exclusive in another process while shared: %q", out)
	}
	release()
	held = false
	if out := runReceiptLockHelper(t, root); out != "acquired" {
		t.Fatalf("exclusive in another process after release: %q", out)
	}
}

func receiptLockHelper(root string) {
	s, err := openReceiptStore(root, "owner", "workloads", testBinding)
	if err != nil {
		fmt.Println("result: error:", err)
		return
	}
	release, err := s.exclusive()
	switch {
	case errors.Is(err, errOperationBusy):
		fmt.Println("result: busy")
	case err != nil:
		fmt.Println("result: error:", err)
	default:
		release()
		fmt.Println("result: acquired")
	}
}

// runReceiptLockHelper runs the helper process to completion within a bound
// and returns its result line.
func runReceiptLockHelper(t *testing.T, root string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), receiptJoinTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReceiptOperationLockProcess$", "-test.count=1")
	cmd.Env = append(os.Environ(), "SKYGO_RECEIPT_LOCK_ROOT="+root)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("helper: %v\n%s", err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if result, ok := strings.CutPrefix(line, "result: "); ok {
			return result
		}
	}
	t.Fatalf("helper output: %s", out)
	return ""
}
