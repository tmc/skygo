//go:build linux || darwin

package cli

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

var errUnsafeOutput = errors.New("token output path must be real directories owned by root or you and writable only by their owners")

// tokenOutputOps is copied into one output before it opens any descriptor.
// Tests can model a returned failure without changing another sign-in's calls.
type tokenOutputOps struct {
	close    func(int) error
	fsync    func(int) error
	unlinkat func(int, string, int) error
	fstatat  func(int, string, *unix.Stat_t, int) error
}

func defaultTokenOutputOps() tokenOutputOps {
	return tokenOutputOps{unix.Close, unix.Fsync, unix.Unlinkat, unix.Fstatat}
}

// openOIDCCA cannot block on a FIFO before its regular-file check. A symlink
// to a regular configured CA is allowed; this is not credential output.
func openOIDCCA(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "OIDC CA"), nil
}

// tokenOutput is a token file to publish in one verified directory. The
// directory is reached from "/" one component at a time without following
// symlinks. Each component must be a directory owned by root or the caller
// that only its owner can write; a root-owned sticky directory such as /tmp
// is also accepted, since other users cannot rename or remove what the
// caller owns in it. The output directory itself must be owned by the caller
// and private. It stays open from before sign-in until publication, which
// works relative to it.
//
// This is a mode and ownership boundary. It does not defend against root,
// against the caller's own processes, or against writers granted access by
// ACLs.
type tokenOutput struct {
	dir       int         // the output directory; -1 once closed
	st        unix.Stat_t // its identity when opened
	path      string      // its absolute path, rechecked before publication
	name      string
	replace   bool
	published bool // the token is at name

	beforePublish func(temp string) // tests only
	ops           tokenOutputOps
	descriptors   []*tokenDescriptor
	fault         error // retains every uncertain original; prevents publication
	closed        bool
	closeErr      error
}

// closeFD closes fd and labels a failure. A failed close is never retried:
// whether the descriptor is still open is unknown, and its number may
// already belong to another file.
func (o *tokenOutput) closeFD(fd int, label string) error {
	f := &tokenDescriptor{fd: fd}
	o.descriptors = append(o.descriptors, f)
	if f.err = o.ops.close(fd); f.err != nil {
		err := &oidcFailure{message: label, output: o, descriptor: f, causes: []error{f.err}}
		o.fault = errors.Join(o.fault, err)
		return err
	}
	return nil
}

// openTokenOutput opens and verifies path's directory and checks that path
// may be written: absent, or with replace a private regular file.
func openTokenOutput(path string, replace bool) (*tokenOutput, error) {
	return openTokenOutputWithOps(path, replace, defaultTokenOutputOps())
}

func openTokenOutputWithOps(path string, replace bool, ops tokenOutputOps) (*tokenOutput, error) {
	o := &tokenOutput{dir: -1, replace: replace, ops: ops}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, errors.New("cannot resolve token output")
	}
	dir, name := filepath.Split(abs)
	if name == "" {
		return nil, errors.New("token output must name a file")
	}
	dir = filepath.Clean(dir)
	fd, st, err := o.openOutputDirectory(dir)
	if err != nil {
		return nil, err
	}
	if st.Uid != uint32(os.Geteuid()) || uint32(st.Mode)&0o077 != 0 {
		return nil, errors.Join(errors.New("token output directory must be owned by you and private (mode 0700)"), o.closeFD(fd, "close output directory"))
	}
	o.dir, o.st, o.path, o.name = fd, st, dir, name
	if err := o.checkLeaf(); err != nil {
		return nil, errors.Join(err, o.Close())
	}
	return o, nil
}

// openOutputDirectory opens the clean absolute directory dir from "/"
// without following symlinks, checking every directory on the way. Every
// descriptor it opens is closed exactly once, except the one it returns.
func (o *tokenOutput) openOutputDirectory(dir string) (int, unix.Stat_t, error) {
	var names []string
	if dir != "/" {
		names = strings.Split(strings.TrimPrefix(dir, "/"), "/")
	}
	var st unix.Stat_t
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, st, errors.Join(errUnsafeOutput, err)
	}
	for i := 0; ; i++ {
		if err := unix.Fstat(fd, &st); err != nil {
			return -1, st, errors.Join(errUnsafeOutput, err, o.closeFD(fd, "close directory"))
		}
		if !trustedDirectory(&st) {
			return -1, st, errors.Join(errUnsafeOutput, o.closeFD(fd, "close directory"))
		}
		if i == len(names) {
			return fd, st, nil
		}
		next, err := unix.Openat(fd, names[i], unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		cerr := o.closeFD(fd, "close directory")
		if err != nil {
			return -1, st, errors.Join(errUnsafeOutput, err, cerr)
		}
		if cerr != nil {
			return -1, st, errors.Join(cerr, o.closeFD(next, "close directory"))
		}
		fd = next
	}
}

// trustedDirectory reports whether st is a directory owned by root or the
// caller that only its owner can write, or a root-owned sticky directory.
func trustedDirectory(st *unix.Stat_t) bool {
	mode := uint32(st.Mode)
	if mode&unix.S_IFMT != unix.S_IFDIR || st.Uid != 0 && st.Uid != uint32(os.Geteuid()) {
		return false
	}
	return mode&0o022 == 0 || st.Uid == 0 && mode&unix.S_ISVTX != 0
}

// checkLeaf checks the output name in the held directory.
func (o *tokenOutput) checkLeaf() error {
	var st unix.Stat_t
	err := unix.Fstatat(o.dir, o.name, &st, unix.AT_SYMLINK_NOFOLLOW)
	switch {
	case errors.Is(err, unix.ENOENT):
		return nil
	case err != nil:
		return errors.New("cannot inspect token output")
	case !o.replace:
		return errors.New("token output already exists; use --replace to replace it")
	case uint32(st.Mode)&unix.S_IFMT != unix.S_IFREG || st.Uid != uint32(os.Geteuid()) || uint32(st.Mode)&0o077 != 0:
		return errors.New("token output to replace must be a private regular file")
	}
	return nil
}

// stateError labels err with whether the token was published. Before
// publication nothing of the CLI's is at the output name. After publication
// the output holds the new token, with --replace in place of the old file,
// which is not restored; err reports what failed afterwards, such as
// syncing the directory or removing the temporary name.
func (o *tokenOutput) stateError(err error) error {
	if err == nil {
		return nil
	}
	if o.published {
		return fmt.Errorf("token output was published, but its durability or cleanup is uncertain: %w", err)
	}
	return fmt.Errorf("token output was not published: %w", err)
}

// publish writes token and a newline to a new 0600 file in the held
// directory and links it to the output name, which must still be absent, or
// with replace renames it over the private regular file there. The old file
// is untouched until the rename. Errors are labeled by o.stateError.
// Nothing is retried, and neither a published token nor a file someone else
// put at the output name is removed.
func (o *tokenOutput) publish(token string) (err error) {
	defer func() { err = o.stateError(err) }()
	if o.fault != nil {
		return o.fault
	}
	if err := o.checkLeaf(); err != nil {
		return err
	}
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return errors.New("create token output")
	}
	temp := ".skygo-oidc-" + hex.EncodeToString(b)
	fd, err := unix.Openat(o.dir, temp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return fmt.Errorf("create token output: %w", err)
	}
	var mine unix.Stat_t
	if err := unix.Fstat(fd, &mine); err != nil {
		// The entry was created exclusively in the private directory a
		// moment ago and holds nothing yet, so it is removed by name.
		err = fmt.Errorf("inspect temporary token output: %w", err)
		closeErr := o.closeFD(fd, "close temporary token output")
		if closeErr != nil {
			return errors.Join(err, closeErr)
		}
		return errors.Join(err, o.unlinkLabeled(temp))
	}
	tempNamed := true
	defer func() {
		if tempNamed && o.fault == nil {
			err = errors.Join(err, o.unlinkMine(temp, &mine))
		}
	}()
	werr := unix.Fchmod(fd, 0o600)
	if werr != nil {
		werr = fmt.Errorf("set token output mode: %w", werr)
	} else if werr = writeFD(fd, []byte(token+"\n")); werr != nil {
		werr = fmt.Errorf("write token output: %w", werr)
	} else if werr = o.ops.fsync(fd); werr != nil {
		werr = fmt.Errorf("sync token output: %w", werr)
	}
	if err := errors.Join(werr, o.closeFD(fd, "close temporary token output")); err != nil {
		return err
	}

	if o.beforePublish != nil {
		o.beforePublish(temp)
	}
	if err := o.recheck(); err != nil {
		return err
	}
	if !o.mine(temp, &mine) {
		return errors.New("token output temporary file was replaced")
	}
	if o.replace {
		if err := o.checkLeaf(); err != nil {
			return err
		}
		if err := unix.Renameat(o.dir, temp, o.dir, o.name); err != nil {
			return fmt.Errorf("publish token output: %w", err)
		}
		tempNamed = false
	} else if err := unix.Linkat(o.dir, temp, o.dir, o.name, 0); errors.Is(err, unix.EEXIST) {
		return errors.New("token output appeared during sign-in and was left unchanged")
	} else if err != nil {
		return fmt.Errorf("publish token output: %w", err)
	}
	o.published = true
	if !o.mine(o.name, &mine) {
		return errors.New("token output was replaced after publication and was left unchanged")
	}
	if err := o.ops.fsync(o.dir); err != nil {
		return fmt.Errorf("sync token output directory: %w", err)
	}
	return nil
}

// writeFD writes all of b to fd.
func writeFD(fd int, b []byte) error {
	for len(b) > 0 {
		n, err := unix.Write(fd, b)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

// recheck reopens the output directory by path and requires the held one.
// Its descriptor must close cleanly too, or publication does not proceed.
func (o *tokenOutput) recheck() error {
	fd, st, err := o.openOutputDirectory(o.path)
	if err != nil {
		return err
	}
	if err := o.closeFD(fd, "close rechecked output directory"); err != nil {
		return err
	}
	if st.Dev != o.st.Dev || st.Ino != o.st.Ino {
		return errors.New("token output directory changed during sign-in")
	}
	return nil
}

// mine reports whether name in the held directory is the file st describes.
func (o *tokenOutput) mine(name string, st *unix.Stat_t) bool {
	var got unix.Stat_t
	return unix.Fstatat(o.dir, name, &got, unix.AT_SYMLINK_NOFOLLOW) == nil && got.Dev == st.Dev && got.Ino == st.Ino
}

// unlinkMine removes name if it is still the file st describes. An absent
// name needs no removal, and a substituted entry is not this process's file
// and is left alone. An entry that cannot be inspected may still be a copy
// of the token, so it is left too and the failure is reported.
func (o *tokenOutput) unlinkMine(name string, st *unix.Stat_t) error {
	var got unix.Stat_t
	err := o.ops.fstatat(o.dir, name, &got, unix.AT_SYMLINK_NOFOLLOW)
	switch {
	case errors.Is(err, unix.ENOENT):
		return nil
	case err != nil:
		return fmt.Errorf("inspect temporary token output for removal: %w", err)
	case got.Dev != st.Dev || got.Ino != st.Ino:
		return nil
	}
	return o.unlinkLabeled(name)
}

func (o *tokenOutput) unlinkLabeled(name string) error {
	if err := o.ops.unlinkat(o.dir, name, 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("remove temporary token output: %w", err)
	}
	return nil
}

// Close closes the held directory once and keeps its result. An uncertain
// original keeps this output and fences all later cleanup and publication.
func (o *tokenOutput) Close() error {
	if o.closed {
		return o.closeErr
	}
	o.closed = true
	if o.fault != nil {
		o.closeErr = o.stateError(o.fault)
		return o.closeErr
	}
	o.closeErr = o.stateError(o.closeFD(o.dir, "close output directory"))
	if o.closeErr == nil {
		o.dir = -1
	}
	return o.closeErr
}
