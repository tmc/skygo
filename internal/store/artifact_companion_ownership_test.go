//go:build linux || darwin

package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
)

// Constructor failures are MODELED reported results at the exact shared error
// handoff used immediately by Initialize/Open. This is not a native close-fault
// test and creates no invented Files/FD receipt. The original Root is real.
func TestCompanionConstructorUncertaintyKeepsBorrowedRoot(t *testing.T) {
	for _, frontier := range []string{"backup-open", "restore-initialize", "restore-open"} {
		t.Run(frontier, func(t *testing.T) {
			base, err := companionExampleBase()
			if err != nil {
				t.Fatal(err)
			}
			root, err := os.OpenRoot(base)
			if err != nil {
				t.Fatal(err)
			}
			op := &companionOperation{ctx: context.Background()}
			destination := frontier != "backup-open"
			if destination {
				op.newRoot = root
			} else {
				op.root = root
			}
			cause := errors.Join(artifact.ErrFileCloseUncertain, errors.New("modeled constructor result"))
			if got := op.artifactConstructorError(cause, destination); got != cause {
				t.Fatal("constructor error identity changed")
			}
			got := op.finish(cause)
			fixture := &companionFixture{}
			fixture.hold(got) // Keeps SAME operation/cause/borrowed root reachable.
			var held *companionError
			if !errors.As(got, &held) || held.operation != op || !errors.Is(got, cause) {
				t.Fatal("lost original constructor frame", got)
			}
			if destination {
				if op.newRoot != root || op.newRootAttempted || op.newFilesAcquisitionErr != cause {
					t.Fatal("closed destination borrowed root beneath failed constructor")
				}
			} else if op.root != root || op.rootAttempted || op.filesAcquisitionErr != cause {
				t.Fatal("closed source borrowed root beneath failed constructor")
			}
			if _, err := root.Lstat("."); err != nil {
				t.Fatal("original borrowed root no longer usable", err)
			}
			if !errors.Is(op.finish(nil), cause) {
				t.Fatal("lost sticky constructor uncertainty")
			}
			t.Logf("retained modeled uncertain-constructor root/case: %s", base)
		})
	}
}

func TestCompanionOrdinaryConstructorFailureClosesBorrowedRoot(t *testing.T) {
	base, err := companionExampleBase()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		t.Fatal(err)
	}
	op := &companionOperation{ctx: context.Background(), root: root}
	cause := errors.New("ordinary constructor validation failure")
	got := op.finish(op.artifactConstructorError(cause, false))
	fixture := &companionFixture{}
	fixture.operation(got)
	if !errors.Is(got, cause) || op.root != nil || !op.rootAttempted || op.filesAcquisitionErr != nil {
		fixture.fatal(t, got)
	}
	if _, err := root.Lstat("."); err == nil {
		t.Fatal("ordinary failure did not close original root")
	}
	if err := os.RemoveAll(base); err != nil {
		t.Fatal(err)
	}
}

// These actual same-user namespace replacements deliberately violate the
// stable-parent premise. Admission must detect them, leave both trees alone,
// and never publish the substituted basename. No cleanup/discovery is adopted.
func TestCompanionRefusesReplacedParentOrStage(t *testing.T) {
	for _, replaceParent := range []bool{true, false} {
		t.Run(map[bool]string{true: "parent", false: "source-stage"}[replaceParent], func(t *testing.T) {
			base, err := companionExampleBase()
			if err != nil {
				t.Fatal(err)
			}
			parent := filepath.Join(base, "parent")
			if err := os.Mkdir(parent, 0700); err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(parent, "published")
			op, err := newCompanion(context.Background(), destination)
			fixture := &companionFixture{}
			if err != nil {
				fixture.fatal(t, op.finish(err))
			}
			if err := op.makeStage("objects"); err != nil {
				fixture.fatal(t, op.finish(err))
			}
			originalParent := parent
			originalStage := op.stagePath
			if replaceParent {
				originalParent = filepath.Join(base, "original-parent")
				if err := os.Rename(parent, originalParent); err != nil {
					fixture.fatal(t, op.finish(err))
				}
				originalStage = filepath.Join(originalParent, op.stageName)
				if err := os.Mkdir(parent, 0700); err != nil {
					fixture.fatal(t, op.finish(err))
				}
			} else {
				originalStage = filepath.Join(parent, "original-stage")
				if err := os.Rename(op.stagePath, originalStage); err != nil {
					fixture.fatal(t, op.finish(err))
				}
			}
			replacement := filepath.Join(parent, op.stageName)
			if err := os.Mkdir(replacement, 0700); err != nil {
				fixture.fatal(t, op.finish(err))
			}
			if err := os.WriteFile(filepath.Join(replacement, "sentinel"), []byte("unrelated replacement"), 0600); err != nil {
				fixture.fatal(t, op.finish(err))
			}
			err = op.finish(op.publish())
			fixture.operation(err)
			if err == nil || op.published {
				t.Fatal("published substituted namespace")
			}
			if got, err := os.ReadFile(filepath.Join(replacement, "sentinel")); err != nil || string(got) != "unrelated replacement" {
				t.Fatal("removed or rewrote replacement", err)
			}
			if _, err := os.Stat(filepath.Join(originalStage, "objects")); err != nil {
				t.Fatal("removed original owned stage", err)
			}
			for _, p := range []string{parent, originalParent} {
				if _, err := os.Lstat(filepath.Join(p, "published")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("published any destination", p, err)
				}
			}
			t.Logf("retained refused namespace case and exact frame: %s", base)
		})
	}
}

func TestCompanionRefusesWritablePublicationParent(t *testing.T) {
	base, err := companionExampleBase()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(base, 0720); err != nil {
		t.Fatal(err)
	}
	op, err := newCompanion(context.Background(), filepath.Join(base, "published"))
	fixture := &companionFixture{}
	err = fixture.operation(op.finish(err))
	if err == nil || op.stagePath != "" {
		fixture.fatal(t, errors.New("accepted group-writable publication namespace"))
	}
	if err := os.Chmod(base, 0700); err != nil {
		fixture.fatal(t, err)
	}
	if err := os.RemoveAll(base); err != nil {
		fixture.fatal(t, err)
	}
}

func TestCompanionRefusesSourceDestinationOverlap(t *testing.T) {
	f := newCompanionFixture(t, true)
	base := filepath.Dir(f.data)
	inside := filepath.Join(f.data, "inside")
	if err := os.Mkdir(inside, 0700); err != nil {
		f.fatal(t, err)
	}
	bundle := filepath.Join(base, "bundle")
	if err := f.operation(BackupCompanion(context.Background(), f.data, f.artifacts, bundle)); err != nil {
		f.fatal(t, err)
	}
	dataAlias, bundleAlias := filepath.Join(base, "data-alias"), filepath.Join(base, "bundle-alias")
	if err := os.Symlink(f.data, dataAlias); err != nil {
		f.fatal(t, err)
	}
	if err := os.Symlink(bundle, bundleAlias); err != nil {
		f.fatal(t, err)
	}
	before := observeCompanionSource(t, f)
	bundleBefore := observeCompanionPaths(t, bundle)
	for _, test := range []struct {
		name, destination string
		restore           bool
	}{
		{"backup-data", filepath.Join(f.data, "backup"), false},
		{"backup-artifacts", filepath.Join(f.artifacts, "backup"), false},
		{"backup-symlink-ancestor", filepath.Join(dataAlias, "inside", "backup"), false},
		{"backup-existing-ancestor", base, false},
		{"restore-bundle", filepath.Join(bundle, "restored"), true},
		{"restore-objects", filepath.Join(bundle, "objects", "restored"), true},
		{"restore-symlink-ancestor", filepath.Join(bundleAlias, "objects", "restored"), true},
		{"restore-existing-ancestor", base, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var err error
			if test.restore {
				err = RestoreCompanion(context.Background(), bundle, test.destination)
			} else {
				err = BackupCompanion(context.Background(), f.data, f.artifacts, test.destination)
			}
			if f.operation(err) == nil {
				t.Fatal("accepted source/destination overlap")
			}
			unchangedCompanionSource(t, f, before)
			unchangedCompanionPaths(t, bundleBefore, observeCompanionPaths(t, bundle))
		})
	}
}
