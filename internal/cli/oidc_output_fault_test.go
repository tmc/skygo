//go:build linux || darwin

package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// These tests are MODELED: they replace the results of close, fsync,
// unlinkat and the inspection before removing the temporary name to reach
// error branches. They show how publish reports and cleans up after each
// failure, not how any kernel or filesystem fails.

var (
	errInjected        = errors.New("injected failure")
	errInjectedClose   = errors.New("injected close failure")
	errInjectedInspect = errors.New("injected inspection failure")
)

// closeCall is one observed close. The descriptor was really closed; err is
// the real result, which is EBADF if it had been closed before.
type closeCall struct {
	fd       int
	kind     uint32 // S_IFDIR or S_IFREG
	dev, ino uint64
	err      error
}

// faults replaces the descriptor operations for one test. Each replacement
// performs the real operation (except a failed unlink or inspection),
// records it, and then reports the injected result, if any.
type faults struct {
	ops       tokenOutputOps
	output    *tokenOutput
	closeErr  func(c closeCall) error
	fsyncErr  func(kind uint32) error
	unlinkErr error
	inspect   func(dir int, name string) error // runs before the real inspection; an error is reported instead
	closes    []closeCall
	unlinks   int
}

func describeFD(fd int) (kind uint32, dev, ino uint64) {
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil {
		return 0, 0, 0
	}
	return uint32(st.Mode) & unix.S_IFMT, uint64(st.Dev), st.Ino
}

func injectFaults(t *testing.T) *faults {
	f := &faults{ops: defaultTokenOutputOps()}
	f.ops.close = func(fd int) error {
		c := closeCall{fd: fd}
		c.kind, c.dev, c.ino = describeFD(fd)
		c.err = unix.Close(fd)
		f.closes = append(f.closes, c)
		if c.err != nil {
			return c.err
		}
		if f.closeErr != nil {
			return f.closeErr(c)
		}
		return nil
	}
	f.ops.fsync = func(fd int) error {
		kind, _, _ := describeFD(fd)
		if err := unix.Fsync(fd); err != nil {
			return err
		}
		if f.fsyncErr != nil {
			return f.fsyncErr(kind)
		}
		return nil
	}
	f.ops.unlinkat = func(dir int, name string, flags int) error {
		f.unlinks++
		if f.unlinkErr != nil {
			return f.unlinkErr
		}
		return unix.Unlinkat(dir, name, flags)
	}
	f.ops.fstatat = func(dir int, name string, st *unix.Stat_t, flags int) error {
		if f.inspect != nil {
			if err := f.inspect(dir, name); err != nil {
				return err
			}
		}
		return unix.Fstatat(dir, name, st, flags)
	}

	return f
}

func (f *faults) publishToken(t *testing.T, path string, replace bool, hook func(string)) (err error) {
	t.Helper()
	o, err := openTokenOutputWithOps(path, replace, f.ops)
	if err != nil {
		return err
	}
	f.output = o
	defer func() { err = errors.Join(err, o.Close()) }()
	o.beforePublish = hook
	return o.publish(outputToken)
}

// failOnce returns a closeErr that fails the first close matching match.
func failOnce(match func(closeCall) bool, err error) func(closeCall) error {
	done := false
	return func(c closeCall) error {
		if done || !match(c) {
			return nil
		}
		done = true
		return err
	}
}

// requireClosedOnce fails if any observed close found its descriptor
// already closed.
func requireClosedOnce(t *testing.T, f *faults) {
	t.Helper()
	for i, c := range f.closes {
		if c.err != nil {
			t.Fatalf("close %d (fd %d) = %v: closed twice", i, c.fd, c.err)
		}
	}
}

func identity(t *testing.T, path string) (dev, ino uint64) {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		t.Fatal(err)
	}
	return uint64(st.Dev), st.Ino
}

func requireErr(t *testing.T, err error, cause error, texts ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("got no error")
	}
	if cause != nil && !errors.Is(err, cause) {
		t.Fatalf("error %q does not wrap %q", err, cause)
	}
	for _, text := range texts {
		if !strings.Contains(err.Error(), text) {
			t.Fatalf("error %q does not contain %q", err, text)
		}
	}
	if strings.Contains(err.Error(), outputToken) || strings.Contains(err.Error(), ".skygo-oidc-") {
		t.Fatalf("error %q names the token or the temporary file", err)
	}
}

// TestTokenOutputTraversalCloseFailure fails closing "/" after its child is
// open. The walk must close the child once and fail.
func TestTokenOutputTraversalCloseFailure(t *testing.T) {
	dir := privateTempDir(t)
	first := "/" + strings.SplitN(strings.TrimPrefix(dir, "/"), "/", 2)[0]
	rootDev, rootIno := identity(t, "/")
	childDev, childIno := identity(t, first)
	f := injectFaults(t)
	f.closeErr = failOnce(func(closeCall) bool { return true }, errInjected)
	_, err := openTokenOutputWithOps(filepath.Join(dir, "id.token"), false, f.ops)
	requireErr(t, err, errInjected, "close directory")
	requireClosedOnce(t, f)
	if len(f.closes) != 2 {
		t.Fatalf("closes = %d, want 2 (the root, then its child once)", len(f.closes))
	}
	if c := f.closes[0]; c.dev != rootDev || c.ino != rootIno {
		t.Fatalf("first close was not of /")
	}
	if c := f.closes[1]; c.dev != childDev || c.ino != childIno {
		t.Fatalf("second close was not of %s", first)
	}
	if got := tree(t, dir); len(got) != 0 {
		t.Fatalf("files written: %q", got)
	}
}

// TestTokenOutputRecheckCloseFailure fails closing the rechecked output
// directory. Nothing may be published.
func TestTokenOutputRecheckCloseFailure(t *testing.T) {
	dir := privateTempDir(t)
	dev, ino := identity(t, dir)
	f := injectFaults(t)
	path := filepath.Join(dir, "id.token")
	err := f.publishToken(t, path, false, func(string) {
		f.closeErr = failOnce(func(c closeCall) bool { return c.dev == dev && c.ino == ino }, errInjected)
	})
	requireErr(t, err, errInjected, "token output was not published", "close rechecked output directory")
	requireClosedOnce(t, f)
	if got := tree(t, dir); len(got) != 1 {
		t.Fatalf("files left: %q, want the retained private temporary token", got)
	} else {
		requirePrivate(t, dir, got)
	}
	if f.unlinks != 0 || f.output == nil || f.output.fault == nil {
		t.Fatal("uncertain Close did not retain the original or attempted unlink")
	}
}

// TestTokenOutputWriteAndCloseFailure fails both syncing and closing the
// temporary file. Both causes must be reported, and nothing published.
func TestTokenOutputWriteAndCloseFailure(t *testing.T) {
	dir := privateTempDir(t)
	f := injectFaults(t)
	f.fsyncErr = func(kind uint32) error {
		if kind == unix.S_IFREG {
			return errInjected
		}
		return nil
	}
	f.closeErr = failOnce(func(c closeCall) bool { return c.kind == unix.S_IFREG }, errInjectedClose)
	err := f.publishToken(t, filepath.Join(dir, "id.token"), false, nil)
	requireErr(t, err, errInjected, "token output was not published", "sync token output", "close temporary token output")
	requireErr(t, err, errInjectedClose)
	requireClosedOnce(t, f)
	if got := tree(t, dir); len(got) != 1 {
		t.Fatalf("files left: %q, want the retained private temporary token", got)
	} else {
		requirePrivate(t, dir, got)
	}
	if f.unlinks != 0 || f.output == nil || f.output.fault == nil {
		t.Fatal("uncertain Close did not retain the original or attempted unlink")
	}
}

// TestTokenOutputPublishedSyncFailure fails syncing the directory after
// publication. The error must say the token was published, and the token
// must stay.
func TestTokenOutputPublishedSyncFailure(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "link", true: "replace"}[replace], func(t *testing.T) {
			dir := privateTempDir(t)
			path := filepath.Join(dir, "id.token")
			if replace {
				writeFile(t, path, "OLD\n", 0o600)
			}
			f := injectFaults(t)
			f.fsyncErr = func(kind uint32) error {
				if kind == unix.S_IFDIR {
					return errInjected
				}
				return nil
			}
			err := f.publishToken(t, path, replace, nil)
			requireErr(t, err, errInjected, "token output was published, but", "sync token output directory")
			requireClosedOnce(t, f)
			if got := tree(t, dir); len(got) != 1 || got["id.token"] != outputToken+"\n" {
				t.Fatalf("directory = %q, want only the published token", got)
			}
			if info, err := os.Lstat(path); err != nil || info.Mode().Perm() != 0o600 {
				t.Fatalf("output = %v, %v", info, err)
			}
		})
	}
}

// TestTokenOutputPublishedCleanupFailure fails removing the temporary name
// after the link. The error must say the token was published and the alias
// failed to go; both names stay private.
func TestTokenOutputPublishedCleanupFailure(t *testing.T) {
	dir := privateTempDir(t)
	path := filepath.Join(dir, "id.token")
	f := injectFaults(t)
	f.unlinkErr = errInjected
	err := f.publishToken(t, path, false, nil)
	requireErr(t, err, errInjected, "token output was published, but", "remove temporary token output")
	requireClosedOnce(t, f)
	got := tree(t, dir)
	if len(got) != 2 || got["id.token"] != outputToken+"\n" {
		t.Fatalf("directory = %q, want the token and its temporary alias", got)
	}
	for name := range got {
		if info, err := os.Lstat(filepath.Join(dir, name)); err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("%s = %v, %v; want 0600", name, info, err)
		}
	}
}

// TestTokenOutputPublishedCloseFailure fails closing the held directory
// after publication. Close reports it once as after publication.
func TestTokenOutputPublishedCloseFailure(t *testing.T) {
	dir := privateTempDir(t)
	dev, ino := identity(t, dir)
	f := injectFaults(t)
	o, err := openTokenOutputWithOps(filepath.Join(dir, "id.token"), false, f.ops)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.publish(outputToken); err != nil {
		t.Fatalf("publish: %v", err)
	}
	f.closeErr = failOnce(func(c closeCall) bool { return c.dev == dev && c.ino == ino }, errInjected)
	n := len(f.closes)
	requireErr(t, o.Close(), errInjected, "token output was published, but", "close output directory")
	if err := o.Close(); !errors.Is(err, errInjected) || len(f.closes) != n+1 {
		t.Fatalf("second Close = %v after %d closes, want the same failure and exactly one close", err, len(f.closes)-n)
	}
	requireClosedOnce(t, f)
	if got := tree(t, dir); len(got) != 1 || got["id.token"] != outputToken+"\n" {
		t.Fatalf("directory = %q, want the published token", got)
	}
}

// requirePrivate fails unless every file in dir has mode 0600.
func requirePrivate(t *testing.T, dir string, got map[string]string) {
	t.Helper()
	for name := range got {
		if info, err := os.Lstat(filepath.Join(dir, name)); err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("%s = %v, %v; want 0600", name, info, err)
		}
	}
}

// TestTokenOutputCleanupInspectionFailure fails inspecting the temporary
// name after an otherwise successful publication. The error must say the
// token was published and the alias was not inspected, and the unverified
// alias must be left as it is.
func TestTokenOutputCleanupInspectionFailure(t *testing.T) {
	dir := privateTempDir(t)
	f := injectFaults(t)
	f.inspect = func(int, string) error { return errInjectedInspect }
	var temp string
	err := f.publishToken(t, filepath.Join(dir, "id.token"), false, func(name string) { temp = name })
	if err == nil {
		t.Fatal("publish = nil after the temporary name could not be inspected, want an error")
	}
	requireErr(t, err, errInjectedInspect, "token output was published, but", "inspect temporary token output for removal")
	if f.unlinks != 0 {
		t.Fatalf("unlinks = %d, want none of an uninspected entry", f.unlinks)
	}
	requireClosedOnce(t, f)
	got := tree(t, dir)
	if len(got) != 2 || got["id.token"] != outputToken+"\n" || got[temp] != outputToken+"\n" {
		t.Fatalf("directory = %q, want the token and its retained temporary alias", got)
	}
	requirePrivate(t, dir, got)
}

// TestTokenOutputCleanupInspectionJoined fails inspecting the temporary name
// after another failure. Both causes must be reported under the right
// publication state.
func TestTokenOutputCleanupInspectionJoined(t *testing.T) {
	tests := []struct {
		name      string
		published bool
		cause     error  // the primary cause, if a sentinel
		primary   string // the primary failure's text
		output    string // the content left at the output name
	}{
		{"published", true, errInjected, "sync token output directory", outputToken + "\n"},
		{"unpublished", false, nil, "appeared during sign-in", "OTHER"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := privateTempDir(t)
			path := filepath.Join(dir, "id.token")
			f := injectFaults(t)
			f.inspect = func(int, string) error { return errInjectedInspect }
			if tt.published {
				f.fsyncErr = func(kind uint32) error {
					if kind == unix.S_IFDIR {
						return errInjected
					}
					return nil
				}
			}
			var temp string
			err := f.publishToken(t, path, false, func(name string) {
				temp = name
				if !tt.published {
					writeFile(t, path, "OTHER", 0o600)
				}
			})
			state, wrong := "token output was not published", "token output was published, but"
			if tt.published {
				state, wrong = wrong, state
			}
			requireErr(t, err, tt.cause, state, tt.primary)
			requireErr(t, err, errInjectedInspect, "inspect temporary token output for removal")
			if strings.Contains(err.Error(), wrong) {
				t.Fatalf("error %q says %q", err, wrong)
			}
			if f.unlinks != 0 {
				t.Fatalf("unlinks = %d, want none of an uninspected entry", f.unlinks)
			}
			requireClosedOnce(t, f)
			got := tree(t, dir)
			if len(got) != 2 || got["id.token"] != tt.output || got[temp] != outputToken+"\n" {
				t.Fatalf("directory = %q, want the output and the retained temporary alias", got)
			}
			requirePrivate(t, dir, got)
		})
	}
}

// TestTokenOutputCleanupAbsent removes the temporary name just before its
// inspection. Absence is a completed cleanup, not an error.
func TestTokenOutputCleanupAbsent(t *testing.T) {
	dir := privateTempDir(t)
	f := injectFaults(t)
	f.inspect = func(dir int, name string) error {
		if err := unix.Unlinkat(dir, name, 0); err != nil {
			t.Error(err)
		}
		return nil
	}
	if err := f.publishToken(t, filepath.Join(dir, "id.token"), false, nil); err != nil {
		t.Fatalf("publish = %v, want nil after the temporary name was already gone", err)
	}
	if f.unlinks != 0 {
		t.Fatalf("unlinks = %d, want none of an absent name", f.unlinks)
	}
	requireClosedOnce(t, f)
	if got := tree(t, dir); len(got) != 1 || got["id.token"] != outputToken+"\n" {
		t.Fatalf("directory = %q, want only the published token", got)
	}
}

// TestTokenOutputCleanupSubstituted replaces the temporary name just before
// its inspection. The substitute is not this process's file and is left.
func TestTokenOutputCleanupSubstituted(t *testing.T) {
	dir := privateTempDir(t)
	f := injectFaults(t)
	var temp string
	f.inspect = func(_ int, name string) error {
		temp = name
		writeFile(t, filepath.Join(dir, "decoy"), "DECOY", 0o600)
		if err := os.Rename(filepath.Join(dir, "decoy"), filepath.Join(dir, name)); err != nil {
			t.Error(err)
		}
		return nil
	}
	if err := f.publishToken(t, filepath.Join(dir, "id.token"), false, nil); err != nil {
		t.Fatalf("publish = %v, want nil with a substituted temporary name", err)
	}
	if f.unlinks != 0 {
		t.Fatalf("unlinks = %d, want none of a substituted entry", f.unlinks)
	}
	requireClosedOnce(t, f)
	if got := tree(t, dir); len(got) != 2 || got["id.token"] != outputToken+"\n" || got[temp] != "DECOY" {
		t.Fatalf("directory = %q, want the token and the untouched substitute", got)
	}
}

func TestOIDCCAFIFORefused(t *testing.T) {
	dir := privateTempDir(t)
	fifo := filepath.Join(dir, "ca.fifo")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := readOIDCCA(fifo, maxOIDCCABytes, (*os.File).Close); done <- err }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "regular") {
			t.Fatalf("FIFO=%v", err)
		}
	case <-time.After(3 * time.Second):
		// Unblock the same original reader for positive cleanup if a regression
		// reintroduces blocking Open; the timeout itself is not a join.
		fd, err := unix.Open(fifo, unix.O_WRONLY|unix.O_NONBLOCK, 0)
		if err == nil {
			_ = unix.Close(fd)
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("FIFO reader remains held")
		}
		t.Fatal("CA FIFO blocked before its regular-file refusal")
	}
}

func TestOIDCCAFirstClose(t *testing.T) {
	dir := privateTempDir(t)
	for _, directory := range []bool{false, true} {
		name := filepath.Join(dir, "ca.pem")
		if directory {
			name = dir
		} else {
			writeFile(t, name, "CA", 0600)
		}
		cause := errors.New("SECRET-CA-CLOSE")
		var original *os.File
		calls := 0
		data, err := readOIDCCA(name, maxOIDCCABytes, func(f *os.File) error {
			original = f
			calls++
			return errors.Join(f.Close(), cause) // MODELED returned failure after actual Close.
		})
		var held *oidcFailure
		if len(data) != 0 || !errors.Is(err, cause) || !errors.As(err, &held) || held.file != original || calls != 1 {
			t.Fatal("CA first Close lost original")
		}
		if strings.Contains(err.Error(), cause.Error()) {
			t.Fatal("CA close error leaked producer text")
		}
		if directory && len(held.causes) != 2 {
			t.Fatal("CA primary type refusal was lost")
		}
	}
}

func TestTokenOutputIndependentCloseFaults(t *testing.T) {
	for i := 0; i < 2; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			dir := privateTempDir(t)
			f := injectFaults(t)
			f.closeErr = failOnce(func(c closeCall) bool { return c.kind == unix.S_IFREG }, errInjectedClose)
			err := f.publishToken(t, filepath.Join(dir, "id.token"), false, nil)
			requireErr(t, err, errInjectedClose)
			if f.unlinks != 0 || f.output == nil || len(tree(t, dir)) != 1 {
				t.Fatal("uncertain temp was removed or lost")
			}
			first := f.output.Close()
			n := len(f.closes)
			if first == nil || f.output.Close() != first || len(f.closes) != n {
				t.Fatal("uncertain owner was retried or lost")
			}
			var held *oidcFailure
			if !errors.As(err, &held) || held.output != f.output || held.descriptor == nil {
				t.Fatal("actual descriptor-attempt frame was lost")
			}
		})
	}
}
