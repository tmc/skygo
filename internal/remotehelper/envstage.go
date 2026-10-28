package remotehelper

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"time"
)

// Environment artifact staging, ratified with the Kubernetes environment
// runtime (integrations/kubernetes/internal/kube/environment_artifact.go).
// Neither verb uses the data service, a token or SKYGO_DATA_ variables.
//
// env-wait is the environment Pod's stage init container. It records the
// manifest, then waits for the release marker. Each Materialize runs one
// env-put in that container with the artifact on stdin. After every
// Materialize has succeeded, the runtime's Bootstrap runs one env-release,
// which records the release marker. env-wait then verifies the marker and
// every file and exits 0, so the workspace container starts.
//
// The staging directory holds the lock .lock, the manifest .manifest, the
// artifact files a0 through a7, temporary files and the marker .released.
// env-put holds the lock shared from before it checks the marker until it
// exits; env-release and env-wait take it exclusively. So the marker is
// written only while no env-put runs, and an env-put that starts afterwards
// finds the marker and refuses before any effect. The marker is durable:
// once written, the barrier stays released and no artifact changes.

const (
	maxEnvArtifacts     = 8
	maxEnvArtifactBytes = 1 << 30
	// identityExit is the exit code of a verb that refused before any
	// effect because of errIdentity; sealedExit, because of errSealed.
	identityExit = 3
	sealedExit   = 4

	envLock     = ".lock"
	envManifest = ".manifest"
	envReleased = ".released"

	// envWaitTimeout and envReleaseTimeout are source policies, not
	// measured bounds.
	envWaitTimeout    = 3600 * time.Second
	envReleaseTimeout = 60 * time.Second
)

// errIdentity reports that the instance's own UID differs from the expected
// one or cannot be read.
var errIdentity = errors.New("instance identity differs")

// errSealed reports that the instance's barrier is already released.
var errSealed = errors.New("instance already released")

// envArtifact is one artifact file: its name, a0 through a7, its SHA-256 in
// lowercase hex and its positive size.
type envArtifact struct {
	name, sha256 string
	size         int64
}

func (a envArtifact) String() string {
	return a.name + ":" + a.sha256 + ":" + strconv.FormatInt(a.size, 10)
}

func parseEnvArtifact(name, sum, size string) (envArtifact, error) {
	n, err := strconv.ParseInt(size, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != size || n < 1 || n > maxEnvArtifactBytes ||
		!lowerHex(sum, 64) || len(name) != 2 || name[0] != 'a' || name[1] < '0' || name[1] >= '0'+maxEnvArtifacts {
		return envArtifact{}, errArguments
	}
	return envArtifact{name, sum, n}, nil
}

// parseEnvManifest parses name:sha256:size operands, whose names are a0, a1
// and so on in order.
func parseEnvManifest(specs []string) ([]envArtifact, error) {
	if len(specs) == 0 || len(specs) > maxEnvArtifacts {
		return nil, errArguments
	}
	var list []envArtifact
	for i, s := range specs {
		name, rest, _ := strings.Cut(s, ":")
		sum, size, ok := strings.Cut(rest, ":")
		a, err := parseEnvArtifact(name, sum, size)
		if !ok || err != nil || a.name != "a"+strconv.Itoa(i) {
			return nil, errArguments
		}
		list = append(list, a)
	}
	return list, nil
}

// manifestBytes returns the content of .manifest for list.
func manifestBytes(list []envArtifact) []byte {
	var b bytes.Buffer
	for _, a := range list {
		b.WriteString(a.String())
		b.WriteByte('\n')
	}
	return b.Bytes()
}

// validInstance reports whether s has the form of an environment
// incarnation: 1 to 128 bytes of letters, digits, '.', '_', ':' and '-'.
func validInstance(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '.' || c == '_' || c == ':' || c == '-') {
			return false
		}
	}
	return true
}

// readInstance returns the file uid in dir, the instance's own downward API
// metadata.uid. Any failure wraps errIdentity, except a Close failure.
func readInstance(dir string) (uid string, err error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return "", errors.Join(errIdentity, err)
	}
	defer func() { err = errors.Join(err, closeOwned(root)) }()
	f, err := root.Open("uid")
	if err != nil {
		return "", errors.Join(errIdentity, err)
	}
	defer func() { err = errors.Join(err, closeOwned(f)) }()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", errIdentity
	}
	data, err := io.ReadAll(io.LimitReader(f, 129))
	if err != nil || !validInstance(string(data)) {
		return "", errIdentity
	}
	return string(data), nil
}

// checkInstance compares want with the instance's own UID. It reads only
// the uid file and writes nothing.
func checkInstance(dir, want string) error {
	if !validInstance(want) {
		return errIdentity
	}
	uid, err := readInstance(dir)
	if err == nil && uid != want {
		err = errIdentity
	}
	return err
}

// envDir is the open staging directory: its root, the directory itself for
// fsync and renames, and the lock file.
type envDir struct {
	root      *os.Root
	dir, lock *os.File
	// syncDir, if set, syncs dir in place of dir.Sync; tests use it to
	// inject a failure.
	syncDir func(dir *os.File) error
}

// sync syncs the staging directory, making its links durable.
func (d *envDir) sync() error {
	if d.syncDir != nil {
		return d.syncDir(d.dir)
	}
	return d.dir.Sync()
}

func openEnvDir(name string) (d *envDir, err error) {
	d = new(envDir)
	if d.root, err = os.OpenRoot(name); err != nil {
		return nil, err
	}
	if d.dir, err = d.root.Open("."); err == nil {
		d.lock, err = d.root.OpenFile(envLock, os.O_RDWR|os.O_CREATE, 0600)
	}
	if err != nil {
		return nil, errors.Join(err, d.Close())
	}
	return d, nil
}

// Close closes the lock file, which releases the lock, the directory and
// the root.
func (d *envDir) Close() error {
	var err error
	if d.lock != nil {
		err = closeOwned(d.lock)
	}
	if d.dir != nil {
		err = errors.Join(err, closeOwned(d.dir))
	}
	return errors.Join(err, closeOwned(d.root))
}

// lockExclusive takes the lock exclusively, polling until ctx ends.
func (d *envDir) lockExclusive(ctx context.Context) error {
	for {
		locked, err := tryLockExclusive(d.lock)
		if err != nil || locked {
			return err
		}
		if err := pause(ctx, time.Second); err != nil {
			return err
		}
	}
}

// released reports whether the marker exists.
func (d *envDir) released() (bool, error) {
	_, err := d.root.Lstat(envReleased)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

// readSmall returns the content of name, a regular file of at most max
// bytes.
func (d *envDir) readSmall(name string, max int64) (data []byte, err error) {
	f, err := d.root.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, closeOwned(f)) }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > max {
		return nil, errors.New("staging file differs")
	}
	return io.ReadAll(io.LimitReader(f, max+1))
}

// publish writes data to a new temporary file, sets mode 0444, syncs it and
// links it to name without replacing a file, then syncs the directory. It
// returns an error matching fs.ErrExist if name exists.
func (d *envDir) publish(name string, data io.Reader, check func(n int64, sum string) error) (err error) {
	var suffix [8]byte
	rand.Read(suffix[:]) // never returns an error
	tmp := "." + strings.TrimPrefix(name, ".") + "." + hex.EncodeToString(suffix[:]) + ".part"
	f, err := d.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	linked := false
	defer func() {
		if !linked {
			err = errors.Join(err, d.root.Remove(tmp))
		}
	}()
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, h), data)
	if copyErr == nil {
		copyErr = check(n, hex.EncodeToString(h.Sum(nil)))
	}
	if copyErr == nil {
		copyErr = f.Chmod(0444)
	}
	if copyErr == nil {
		copyErr = f.Sync()
	}
	if err := errors.Join(copyErr, closeOwned(f)); err != nil {
		return err
	}
	if err := renameNoReplace(int(d.dir.Fd()), tmp, name); err != nil {
		return err
	}
	linked = true
	return d.sync()
}

// verify checks that a.name is a regular file of mode 0444 with a's size
// and SHA-256.
func (d *envDir) verify(a envArtifact) (err error) {
	f, err := d.root.Open(a.name)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeOwned(f)) }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0444 || info.Size() != a.size {
		return errors.New("artifact file differs")
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, a.size+1))
	if err != nil {
		return err
	}
	if n != a.size || hex.EncodeToString(h.Sum(nil)) != a.sha256 {
		return errors.New("artifact file differs")
	}
	return nil
}

// putArtifact runs "env-put uid name sha256 size". It first checks that
// uid is the instance's own UID; until that check passes it reads no stdin
// and changes no file. It then holds the lock shared until it returns. Under
// the lock it refuses with errSealed if the marker exists, and requires the
// artifact in the manifest. It copies stdin to a new temporary file,
// requiring exactly size bytes and then the end of stdin, with that
// SHA-256, and links it to name. If name already exists it verifies that
// file instead and removes its own.
func putArtifact(args []string, stdin io.Reader, p paths) (err error) {
	if len(args) != 4 {
		return errArguments
	}
	a, err := parseEnvArtifact(args[1], args[2], args[3])
	if err != nil {
		return err
	}
	// The identity check precedes every other read and every write.
	if err := checkInstance(p.instance, args[0]); err != nil {
		return err
	}
	d, err := openEnvDir(p.artifacts)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.Close()) }()
	if err := lockShared(d.lock); err != nil {
		return err
	}
	switch released, err := d.released(); {
	case err != nil:
		return err
	case released:
		return errSealed
	}
	manifest, err := d.readSmall(envManifest, 4096)
	if err != nil {
		return err
	}
	if !bytes.Contains(append([]byte("\n"), manifest...), []byte("\n"+a.String()+"\n")) {
		return errors.New("artifact is not in the manifest")
	}
	err = d.publish(a.name, io.LimitReader(stdin, a.size+1), func(n int64, sum string) error {
		if n != a.size || sum != a.sha256 {
			return errors.New("artifact size or digest differs")
		}
		return nil
	})
	if errors.Is(err, fs.ErrExist) {
		return d.verify(a)
	}
	return err
}

// releaseArtifacts runs "env-release uid -- name:sha256:size...". It first
// checks that uid is the instance's own UID. It then takes the lock
// exclusively, waiting at most envReleaseTimeout for running env-puts, and
// requires the manifest to be exactly specs and every file to verify. It
// records the marker, uid and the manifest, durably. A marker that already
// records exactly that is success once a sync of the directory that links
// it succeeds; any other marker is an error.
func releaseArtifacts(ctx context.Context, uid string, specs []string, p paths) error {
	return releaseArtifactsWith(ctx, uid, specs, p, nil)
}

// releaseArtifactsWith is releaseArtifacts with the staging directory synced
// by syncDir; nil means its Sync method.
func releaseArtifactsWith(ctx context.Context, uid string, specs []string, p paths, syncDir func(*os.File) error) (err error) {
	list, err := parseEnvManifest(specs)
	if err != nil {
		return err
	}
	if err := checkInstance(p.instance, uid); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, envReleaseTimeout)
	defer cancel()
	d, err := openEnvDir(p.artifacts)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.Close()) }()
	d.syncDir = syncDir
	if err := d.lockExclusive(ctx); err != nil {
		return err
	}
	manifest := manifestBytes(list)
	if got, err := d.readSmall(envManifest, 4096); err != nil || !bytes.Equal(got, manifest) {
		return errors.Join(errors.New("manifest differs"), err)
	}
	for _, a := range list {
		if err := d.verify(a); err != nil {
			return err
		}
	}
	marker := append([]byte(uid+"\n"), manifest...)
	err = d.publish(envReleased, bytes.NewReader(marker), func(int64, string) error { return nil })
	if errors.Is(err, fs.ErrExist) {
		got, err := d.readSmall(envReleased, int64(len(marker)))
		if err != nil || !bytes.Equal(got, marker) {
			return errors.Join(errors.New("release marker differs"), err)
		}
		// An earlier call may have linked the marker and then failed to
		// sync the directory, so the link may not be durable. It is once
		// a sync of that directory succeeds.
		return d.sync()
	}
	return err
}

// waitArtifacts runs "env-wait -- name:sha256:size...". It records the
// manifest, or requires an existing one to be exactly specs. Then it waits
// for the marker. It returns nil only once it holds the lock exclusively,
// the marker records the instance's own UID and exactly the manifest, and
// every file verified, all before envWaitTimeout. A stalled file operation
// can delay its failure past that deadline.
func waitArtifacts(ctx context.Context, specs []string, p paths) (err error) {
	list, err := parseEnvManifest(specs)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, envWaitTimeout)
	defer cancel()
	uid, err := readInstance(p.instance)
	if err != nil {
		return err
	}
	d, err := openEnvDir(p.artifacts)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.Close()) }()
	manifest := manifestBytes(list)
	if err := d.lockExclusive(ctx); err != nil {
		return err
	}
	err = d.publish(envManifest, bytes.NewReader(manifest), func(int64, string) error { return nil })
	if errors.Is(err, fs.ErrExist) {
		got, readErr := d.readSmall(envManifest, int64(len(manifest)))
		if readErr != nil || !bytes.Equal(got, manifest) {
			return errors.Join(errors.New("manifest differs"), readErr)
		}
		err = nil
	}
	if err != nil {
		return err
	}
	if err := unlockFile(d.lock); err != nil {
		return err
	}
	marker := append([]byte(uid+"\n"), manifest...)
	for {
		released, err := d.released()
		if err != nil {
			return err
		}
		if released {
			if err := d.lockExclusive(ctx); err != nil {
				return err
			}
			got, err := d.readSmall(envReleased, int64(len(marker)))
			if err != nil || !bytes.Equal(got, marker) {
				return errors.Join(errors.New("release marker differs"), err)
			}
			for _, a := range list {
				if err := d.verify(a); err != nil {
					return err
				}
			}
			return ctx.Err()
		}
		if err := pause(ctx, time.Second); err != nil {
			return err
		}
	}
}
