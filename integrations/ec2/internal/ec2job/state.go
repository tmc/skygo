package ec2job

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sync"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/artifact"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrNoState reports a job directory or journal that does not exist.
	ErrNoState = errors.New("no job state")

	// ErrBusy reports a job directory locked by another owner.
	ErrBusy = errors.New("job state is locked by another process")

	// ErrMismatch reports a journal that does not belong to the
	// configuration, VM, task or agent it is used with.
	ErrMismatch = errors.New("job state does not match")

	// ErrUnpersisted reports a journal state that may not be durably
	// stored. The Job makes no further call until it is.
	ErrUnpersisted = errors.New("job state not durably stored")

	// ErrHeld reports a job whose outcome cannot be learned without an
	// effect this package never takes. The hold is stored and latched.
	ErrHeld = errors.New("job is held")

	// ErrUnsettled reports the first Close of an original file that
	// failed, so that the file's state is unknown. The error keeps the
	// file reachable, and nothing closes it again. After the Close of a
	// temporary journal write, or of a job directory file the Job read,
	// fails, the Job writes and reads nothing more, refuses every call
	// and keeps the job directory's lock until the process exits.
	ErrUnsettled = errors.New("an original file close did not settle")
)

// Names in a job directory.
const (
	lockName     = "lock"        // flock target; never removed
	stateName    = "job.json"    // the committed journal
	workdirName  = "workdir.tar" // the frozen workdir snapshot
	outputName   = "output.tar"  // the downloaded, verified output
	tempSuffix   = ".tmp"        // an uncommitted write
	maxState     = 256 << 10     // bytes of an encoded journal
	stateFormat  = "skygo-ec2-job-state"
	stateVersion = 3
)

// Method is the one submission method a job uses.
const Method = "SubmitWorkspaceJob"

// Retention modes recorded before the first remote mutation.
const (
	RetentionUnimplemented = "unimplemented" // GetRetention unimplemented: no request domain
	RetentionDisabled      = "disabled"      // retention disabled: no request domain
	RetentionEnabled       = "enabled"       // the request domain is recorded
)

// Journal values. Each is fixed text.
const (
	desiredRun    = "run"
	desiredCancel = "cancel"

	uploadNone      = "none"          // no Begin sent
	uploadBeginSent = "begin-sent"    // a Begin may have allocated an upload
	uploadOpen      = "open"          // the upload ID is known
	uploadFinalize  = "finalize-sent" // a Finalize may have completed
	uploadDone      = "finalized"     // the reference is known

	submitNone     = "none"
	submitSent     = "sent" // a submit may have been accepted
	submitAccepted = "accepted"

	cancelNone      = "none"
	cancelSent      = "sent" // a CancelJob may have been delivered
	cancelDelivered = "delivered"

	cleanupNone      = ""          // no CancelArtifactUpload sent
	cleanupSent      = "sent"      // a CancelArtifactUpload may have been applied
	cleanupDelivered = "delivered" // the server marked the upload for deletion

	outputNone    = ""         // not yet collected
	outputSaved   = "saved"    // output.tar holds the verified result
	outputAbsent  = "absent"   // the server reported the result explicitly absent
	outputExpired = "expired"  // the result expired before collection
	outputHeld    = "held"     // the server holds the result
	outputNotUsed = "not-used" // the task declared no outputs

	downloadNone   = ""           // no download since the last closed one
	downloadBegin  = "begin-sent" // a Begin may have allocated a download
	downloadOpen   = "open"       // the download ID is known
	downloadClose  = "close-sent" // a Close may have released the download
	downloadClosed = "closed"     // the server released the download
)

// Hold reasons. Each is fixed text naming the uncertainty.
const (
	HoldUploadBegin    = "upload-begin-outcome-unknown"
	HoldCanceledSubmit = "canceled-with-submit-outcome-unknown"
	HoldAcceptedTask   = "accepted-job-does-not-match"
	HoldAcceptedReply  = "accepted-job-reply-invalid"
	HoldDownloadBegin  = "download-begin-outcome-unknown"
	HoldDownloadClose  = "download-close-outcome-unknown"
)

var (
	hexID32  = regexp.MustCompile(`^[0-9a-f]{32}$`)
	hex64    = regexp.MustCompile(`^[0-9a-f]{64}$`)
	commitRE = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)
)

// A closeError is the failed first Close of an original file. It keeps
// the file, and the store whose lock stays held because of it, reachable
// while the error is. Nothing closes the file again.
type closeError struct {
	name string
	f    *os.File
	s    *store // set when the store keeps its lock because of f
	err  error
}

func (e *closeError) Error() string   { return "close " + e.name + ": " + e.err.Error() }
func (e *closeError) Unwrap() []error { return []error{ErrUnsettled, e.err} }

// closeOriginal closes f, the original file opened as name, once. hook,
// if set, is called with f first. A failed Close returns a *closeError
// holding f.
func closeOriginal(name string, f *os.File, hook func(*os.File)) *closeError {
	if hook != nil {
		hook(f)
	}
	if err := f.Close(); err != nil {
		return &closeError{name: name, f: f, err: err}
	}
	return nil
}

// errOf returns e as an error, nil if e is nil.
func errOf(e *closeError) error {
	if e == nil {
		return nil
	}
	return e
}

// A store is an open, locked job directory.
//
// Every journal save writes a new temporary file, syncs it, closes it,
// renames it over the journal and syncs the directory. Frozen files are
// written the same way, once, and never replaced.
type store struct {
	dir  string
	d    *os.File // the directory, kept open for fsync
	lock *os.File // holds the exclusive flock

	// failed is the first original file whose Close failed: a temporary
	// file the store wrote or a file it read. Once it is set, the store
	// reads and writes nothing more, never removes or reuses that file's
	// name, and keeps its lock.
	failed *closeError

	closeOnce sync.Once
	closeErr  error

	// Test hooks, set only by this package's tests.
	//
	// fault is called at each named stage of a save and once before
	// close's closes. It fails that stage by returning an error; it is
	// not a close. The writes, syncs, renames and closes around it are
	// real.
	//
	// closing is called with each original file just before the store's
	// one Close of it. A test may close the file there itself, so that
	// the store's own os.File.Close returns its real error for a file
	// already closed.
	fault   func(stage string) error
	closing func(*os.File)
}

// checkDir reports whether dir may name a job directory.
func checkDir(dir string) error {
	if dir == "" || !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return errors.New("ec2job: job directory must be a clean absolute path")
	}
	return nil
}

// openStore opens the job directory dir and takes its lock without
// waiting, creating the lock file if it is missing. If create is set,
// dir must not exist; openStore creates it. Before it opens dir,
// openStore syncs dir's parent, so that dir's entry is durable before
// anything is stored in it. It removes nothing; clean does, once the
// caller has validated the journal.
//
// Every file openStore opens and does not return in a store is closed
// once; a failed Close is joined to the error, which keeps the file.
func openStore(dir string, create bool) (*store, error) {
	if err := checkDir(dir); err != nil {
		return nil, err
	}
	if create {
		if err := os.Mkdir(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create job directory: %w", err)
		}
	}
	if err := syncParent(filepath.Dir(dir)); err != nil {
		return nil, err
	}
	d, err := os.OpenFile(dir, os.O_RDONLY|openDirectory|openNoFollow, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s does not exist", ErrNoState, dir)
	}
	if err != nil {
		return nil, fmt.Errorf("open job directory: %w", err)
	}
	if err := checkOpen(d, dir, true); err != nil {
		return nil, errors.Join(err, errOf(closeOriginal(dir, d, nil)))
	}
	lockPath := filepath.Join(dir, lockName)
	lock, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE|openNoFollow, 0o600)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open job lock: %w", err), errOf(closeOriginal(dir, d, nil)))
	}
	err = checkOpen(lock, lockPath, false)
	if err == nil {
		err = lockFile(lock)
	}
	if err != nil {
		return nil, errors.Join(err, errOf(closeOriginal(lockPath, lock, nil)), errOf(closeOriginal(dir, d, nil)))
	}
	return &store{dir: dir, d: d, lock: lock}, nil
}

// preflight reads the journal of the job directory dir, without the lock
// and without creating, removing or changing anything, and refuses it
// unless it is a journal this package reads. A missing journal is
// ErrNoState. It is a bounded read under the same file checks as every
// read, so that an unknown, future or corrupt journal is refused before
// Open creates a lock or removes anything.
//
// preflight is not a lock. It assumes that the job directory's only
// writers are owners that hold its lock, as every writer in this
// package does; a writer that ignores the lock may change the journal
// after preflight reads it, and Open reads it again under the lock
// before it removes anything.
func preflight(dir string) error {
	if err := checkDir(dir); err != nil {
		return err
	}
	data, err := readPrivate(filepath.Join(dir, stateName), maxState, nil)
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w: %s does not exist", ErrNoState, filepath.Join(dir, stateName))
	}
	if err != nil {
		return err
	}
	_, err = decodeState(data)
	return err
}

// clean removes the temporary files a writer of this journal format
// leaves: uncommitted writes of an owner that has exited. The caller
// holds the lock and has validated the journal it holds, so that the
// names are this format's. The directory is synced after a removal.
func (s *store) clean() error {
	removed := false
	for _, name := range []string{stateName, workdirName, outputName} {
		err := os.Remove(s.path(name + tempSuffix))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("remove uncommitted %s: %w", name, err)
		}
		removed = true
	}
	if !removed {
		return nil
	}
	if err := syncDir(s.d); err != nil {
		return fmt.Errorf("remove uncommitted files: sync directory: %w", err)
	}
	return nil
}

func syncParent(parent string) error {
	p, err := os.OpenFile(parent, os.O_RDONLY|openDirectory, 0)
	if err != nil {
		return fmt.Errorf("sync parent directory: %w", err)
	}
	err = syncDir(p)
	if err != nil {
		err = fmt.Errorf("sync parent directory %s: %w", parent, err)
	}
	return errors.Join(err, errOf(closeOriginal(parent, p, nil)))
}

func checkOpen(f *os.File, name string, dir bool) error {
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	return checkPrivate(name, fi, dir)
}

// readPrivate reads the private regular file name of at most max bytes.
// The open does not wait on a FIFO, and anything but a private regular
// file is refused before it is read. Errors never quote the content. A
// failed Close fails the read and is joined to its error as a
// *closeError holding the file. hook, if set, is called with the file
// just before its one Close.
func readPrivate(name string, max int64, hook func(*os.File)) (data []byte, err error) {
	f, err := os.OpenFile(name, os.O_RDONLY|openNoFollow|openNonblock, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := closeOriginal(name, f, hook); cerr != nil {
			data, err = nil, errors.Join(err, cerr)
		}
	}()
	if err := checkOpen(f, name, false); err != nil {
		return nil, err
	}
	data, err = io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	if int64(len(data)) > max {
		return nil, fmt.Errorf("%s exceeds %d bytes", name, max)
	}
	return data, nil
}

func (s *store) path(name string) string { return filepath.Join(s.dir, name) }

func (s *store) fail(stage string) error {
	if s.fault == nil {
		return nil
	}
	if err := s.fault(stage); err != nil {
		return fmt.Errorf("%s: %w", stage, err)
	}
	return nil
}

// settled reports the store's failed original file, if any.
func (s *store) settled() error {
	if s.failed != nil {
		return fmt.Errorf("%w; the job directory stays locked until the process exits", s.failed)
	}
	return nil
}

// load reads the committed journal. A missing journal is ErrNoState.
func (s *store) load() ([]byte, error) {
	return s.read(stateName, maxState)
}

// read reads the private file name of at most max bytes. A failed Close
// of the file is latched as the store's failed file, with the file and
// the store kept by its error, as a failed temporary file's is.
func (s *store) read(name string, max int64) ([]byte, error) {
	if err := s.settled(); err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	data, err := readPrivate(s.path(name), max, s.closing)
	var cerr *closeError
	if errors.As(err, &cerr) {
		cerr.s = s
		s.failed = cerr
		return nil, fmt.Errorf("%w; the job directory stays locked until the process exits", err)
	}
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s does not exist", ErrNoState, s.path(name))
	}
	return data, err
}

// save durably replaces the journal with data. When save fails after the
// rename, the new journal may or may not survive a crash: the caller must
// save it again before acting on it.
func (s *store) save(data []byte) error {
	if len(data) > maxState {
		return fmt.Errorf("save job state: %d bytes exceeds %d", len(data), maxState)
	}
	return s.write(stateName, data, true)
}

// freeze durably writes data to name, which must not exist. Frozen files
// are never replaced.
func (s *store) freeze(name string, data []byte) error {
	if _, err := os.Lstat(s.path(name)); !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("freeze %s: it exists or cannot be checked", name)
	}
	return s.write(name, data, false)
}

// write writes data to a new temporary file, syncs and closes it once,
// and renames it to name. A temporary file whose write, sync or rename
// failed after a successful Close is the store's own closed file and is
// removed. A temporary file whose Close failed is kept, with its original
// os.File, and the store writes nothing more.
func (s *store) write(name string, data []byte, replace bool) error {
	if err := s.settled(); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	tmp := s.path(name + tempSuffix)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|openNoFollow, 0o600)
	if err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	err = s.fail("write")
	if err == nil {
		_, err = f.Write(data) // a short write returns an error
	}
	if err == nil {
		err = s.fail("sync")
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := closeOriginal(tmp, f, s.closing); cerr != nil {
		cerr.s = s
		s.failed = cerr
		return fmt.Errorf("write %s: %w", name, errors.Join(err, cerr))
	}
	if err == nil {
		err = s.fail("rename")
	}
	if err == nil && !replace {
		if _, lerr := os.Lstat(s.path(name)); !errors.Is(lerr, fs.ErrNotExist) {
			err = fmt.Errorf("%s appeared while it was written", name)
		}
	}
	if err == nil {
		err = os.Rename(tmp, s.path(name))
	}
	if err != nil {
		if rerr := os.Remove(tmp); rerr != nil {
			err = errors.Join(err, fmt.Errorf("remove uncommitted %s: %w", name, rerr))
		}
		return fmt.Errorf("write %s: %w", name, err)
	}
	if err := s.fail("sync directory"); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	if err := syncDir(s.d); err != nil {
		return fmt.Errorf("write %s: sync directory: %w", name, err)
	}
	return nil
}

// close releases the lock, once. Every call returns the first close's
// result, and the store keeps referencing its files: a failed Close is
// never retried, and each failure is joined with the others.
//
// A store whose temporary file's Close failed closes nothing: it keeps
// its lock, and its error keeps the store, until the process exits.
func (s *store) close() error {
	s.closeOnce.Do(func() {
		if err := s.settled(); err != nil {
			s.closeErr = err
			return
		}
		err := s.fail("before close")
		lerr := closeOriginal(s.path(lockName), s.lock, s.closing)
		derr := closeOriginal(s.dir, s.d, s.closing)
		s.closeErr = errors.Join(err, errOf(lerr), errOf(derr))
	})
	return s.closeErr
}

// A state is the durable content of a job directory: the immutable
// binding written before any effect, and the progress of each effect.
type state struct {
	Created      time.Time
	ConfigDigest string
	VM           VM
	Agent        *AgentFacts // nil until the first session is bound

	Task      []byte // the task, without workdir, exactly as first stored
	RequestID string
	Snapshot  artifact.Ref // ID empty: the frozen snapshot's digest and size

	Retention  string // "" until recorded
	Lineage    string
	Epoch      uint64
	RetryUntil time.Time

	Desired      string
	Upload       string
	UploadID     string
	Acknowledged int64
	Workdir      artifact.Ref // the upload's reference, once its ID is known
	Cleanup      string       // the canceled job's upload cleanup

	Submit string
	JobID  string
	Cancel string

	Observed   string // last observed job state name, "" if none
	ExitCode   *int32
	ObservedAt time.Time

	Output        string
	OutputRef     artifact.Ref
	OutputAttempt string

	// The result download: its stage, the server's download ID once
	// known, and the result reference and attempt bound before its Begin.
	Download        string
	DownloadID      string
	DownloadRef     artifact.Ref
	DownloadAttempt string

	// The log checkpoint: where the next log copy starts, the attempt
	// it reads, and whether a delivered copy skipped truncated bytes or
	// reached the end of the attempt's log.
	LogAttempt   string
	LogOffset    int64
	LogTruncated bool
	LogComplete  bool

	Hold string
}

// AgentFacts are what the first SSH session reported about the agent. A
// later session must report the same; a changed source or binary is a
// different agent.
type AgentFacts struct {
	Commit       string `json:"commit"`
	Digest       string `json:"digest"`
	Dirty        bool   `json:"dirty"`
	AgentDir     string `json:"agent_dir"`
	AgentAddress string `json:"agent_address"`
}

type wireVM struct {
	Account      string `json:"account"`
	Region       string `json:"region"`
	ProfileHash  string `json:"profile_hash"`
	Owner        string `json:"owner"`
	Environment  string `json:"environment"`
	Generation   uint64 `json:"generation"`
	Lease        string `json:"lease"`
	ClientToken  string `json:"client_token"`
	InstanceID   string `json:"instance_id"`
	ImageID      string `json:"image_id"`
	InstanceType string `json:"instance_type"`
	Architecture string `json:"architecture"`
	Public       bool   `json:"public"`
	Address      string `json:"address"`
}

type wireRef struct {
	ID     string `json:"id"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type wireLog struct {
	Attempt   string `json:"attempt"`
	Offset    int64  `json:"offset"`
	Truncated bool   `json:"truncated"`
	Complete  bool   `json:"complete"`
}

type wireDownload struct {
	Stage   string  `json:"stage"`
	ID      string  `json:"id"`
	Ref     wireRef `json:"ref"`
	Attempt string  `json:"attempt"`
}

// wireState is every version's journal. Version 1 has neither cleanup
// nor log; version 2 adds them, each omitted while empty. Version 3 adds
// the result download and its holds, the download omitted while empty.
type wireState struct {
	Format       string        `json:"format"`
	Version      int           `json:"version"`
	Created      time.Time     `json:"created"`
	ConfigDigest string        `json:"config_digest"`
	VM           wireVM        `json:"vm"`
	Agent        *AgentFacts   `json:"agent"`
	Task         []byte        `json:"task"`
	RequestID    string        `json:"request_id"`
	Method       string        `json:"method"`
	Snapshot     wireRef       `json:"snapshot"`
	Retention    string        `json:"retention"`
	Lineage      string        `json:"lineage"`
	Epoch        uint64        `json:"epoch"`
	RetryUntil   time.Time     `json:"retry_until"`
	Desired      string        `json:"desired"`
	Upload       string        `json:"upload"`
	UploadID     string        `json:"upload_id"`
	Acknowledged int64         `json:"acknowledged"`
	Workdir      wireRef       `json:"workdir"`
	Submit       string        `json:"submit"`
	JobID        string        `json:"job_id"`
	Cancel       string        `json:"cancel"`
	Observed     string        `json:"observed"`
	ExitCode     *int32        `json:"exit_code"`
	ObservedAt   time.Time     `json:"observed_at"`
	Output       string        `json:"output"`
	OutputRef    wireRef       `json:"output_ref"`
	Attempt      string        `json:"output_attempt"`
	Hold         string        `json:"hold"`
	Cleanup      string        `json:"cleanup,omitempty"`
	Log          *wireLog      `json:"log,omitempty"`
	Download     *wireDownload `json:"download,omitempty"`
}

func toWireRef(r artifact.Ref) wireRef { return wireRef{r.ID, r.SHA256, r.Size} }
func (w wireRef) ref() artifact.Ref    { return artifact.Ref{ID: w.ID, SHA256: w.SHA256, Size: w.Size} }

// encodeState returns the canonical encoding of st in the current
// version: indented JSON with a final newline and times in UTC.
func encodeState(st state) ([]byte, error) {
	return encodeVersion(st, stateVersion)
}

// encodeVersion returns the canonical encoding of st in version. Version
// 1 cannot hold an upload cleanup or a log checkpoint, and neither 1 nor
// 2 a result download or its holds: a reader of those versions refuses a
// journal that holds them, so that it cannot lose a download's custody.
func encodeVersion(st state, version int) ([]byte, error) {
	v := st.VM
	w := wireState{
		Format:       stateFormat,
		Version:      version,
		Created:      st.Created.UTC(),
		ConfigDigest: st.ConfigDigest,
		VM: wireVM{
			Account: v.Account, Region: v.Region, ProfileHash: v.ProfileHash,
			Owner: v.Owner, Environment: v.Environment, Generation: v.Generation, Lease: v.Lease,
			ClientToken: v.ClientToken, InstanceID: v.InstanceID, ImageID: v.ImageID,
			InstanceType: v.InstanceType, Architecture: v.Architecture,
			Public: v.Public, Address: v.Address.String(),
		},
		Agent:        st.Agent,
		Task:         st.Task,
		RequestID:    st.RequestID,
		Method:       Method,
		Snapshot:     toWireRef(st.Snapshot),
		Retention:    st.Retention,
		Lineage:      st.Lineage,
		Epoch:        st.Epoch,
		RetryUntil:   st.RetryUntil.UTC(),
		Desired:      st.Desired,
		Upload:       st.Upload,
		UploadID:     st.UploadID,
		Acknowledged: st.Acknowledged,
		Workdir:      toWireRef(st.Workdir),
		Submit:       st.Submit,
		JobID:        st.JobID,
		Cancel:       st.Cancel,
		Observed:     st.Observed,
		ExitCode:     st.ExitCode,
		ObservedAt:   st.ObservedAt.UTC(),
		Output:       st.Output,
		OutputRef:    toWireRef(st.OutputRef),
		Attempt:      st.OutputAttempt,
		Hold:         st.Hold,
		Cleanup:      st.Cleanup,
	}
	if l := (wireLog{st.LogAttempt, st.LogOffset, st.LogTruncated, st.LogComplete}); l != (wireLog{}) {
		w.Log = &l
	}
	if d := (wireDownload{st.Download, st.DownloadID, toWireRef(st.DownloadRef), st.DownloadAttempt}); d != (wireDownload{}) {
		w.Download = &d
	}
	if version == 1 && (w.Cleanup != "" || w.Log != nil) {
		return nil, errors.New("encode job state: version 1 holds no cleanup or log checkpoint")
	}
	if version < 3 && (w.Download != nil || st.Hold == HoldDownloadBegin || st.Hold == HoldDownloadClose) {
		return nil, fmt.Errorf("encode job state: version %d holds no result download", version)
	}
	data, err := json.MarshalIndent(w, "", "\t")
	if err != nil {
		return nil, fmt.Errorf("encode job state: %w", err)
	}
	data = append(data, '\n')
	if len(data) > maxState {
		return nil, fmt.Errorf("encode job state: %d bytes exceeds %d", len(data), maxState)
	}
	return data, nil
}

var errUnsupportedState = errors.New("unsupported job state format")

// decodeState decodes a journal of version 1, 2 or 3. It refuses another
// format or version, an unknown field, trailing data, any value this
// package could not have written, and any JSON encodeVersion would not
// produce for the decoded value in the journal's own version.
//
// The task's bytes are kept exactly as stored. They are decoded and
// checked, never encoded again for comparison: protobuf's deterministic
// encoding is not stable across implementations, and the server binds a
// request to the task's meaning, not to its bytes.
//
// A version 1 or 2 journal is read as written; the next save stores the
// same state as version 3, with the same task bytes, request ID, request
// domain and identities. A journal is never repaired.
func decodeState(data []byte) (state, error) {
	var fence struct {
		Format  string `json:"format"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal(data, &fence); err != nil {
		return state{}, errors.New("decode job state: not JSON")
	}
	if fence.Format != stateFormat || fence.Version < 1 || fence.Version > stateVersion {
		return state{}, fmt.Errorf("%w: version %d", errUnsupportedState, fence.Version)
	}
	var w wireState
	if err := decodeStrict(data, &w); err != nil {
		return state{}, errors.New("decode job state: unknown field or trailing data")
	}
	if w.Method != Method {
		return state{}, errors.New("decode job state: unknown method")
	}
	addr, err := netip.ParseAddr(w.VM.Address)
	if err != nil {
		return state{}, errors.New("decode job state: invalid address")
	}
	st := state{
		Created:      w.Created,
		ConfigDigest: w.ConfigDigest,
		VM: VM{
			Account: w.VM.Account, Region: w.VM.Region, ProfileHash: w.VM.ProfileHash,
			Owner: w.VM.Owner, Environment: w.VM.Environment, Generation: w.VM.Generation, Lease: w.VM.Lease,
			ClientToken: w.VM.ClientToken, InstanceID: w.VM.InstanceID, ImageID: w.VM.ImageID,
			InstanceType: w.VM.InstanceType, Architecture: w.VM.Architecture,
			Public: w.VM.Public, Address: addr,
		},
		Agent:         w.Agent,
		Task:          w.Task,
		RequestID:     w.RequestID,
		Snapshot:      w.Snapshot.ref(),
		Retention:     w.Retention,
		Lineage:       w.Lineage,
		Epoch:         w.Epoch,
		RetryUntil:    w.RetryUntil,
		Desired:       w.Desired,
		Upload:        w.Upload,
		UploadID:      w.UploadID,
		Acknowledged:  w.Acknowledged,
		Workdir:       w.Workdir.ref(),
		Cleanup:       w.Cleanup,
		Submit:        w.Submit,
		JobID:         w.JobID,
		Cancel:        w.Cancel,
		Observed:      w.Observed,
		ExitCode:      w.ExitCode,
		ObservedAt:    w.ObservedAt,
		Output:        w.Output,
		OutputRef:     w.OutputRef.ref(),
		OutputAttempt: w.Attempt,
		Hold:          w.Hold,
	}
	if w.Log != nil {
		st.LogAttempt, st.LogOffset, st.LogTruncated, st.LogComplete = w.Log.Attempt, w.Log.Offset, w.Log.Truncated, w.Log.Complete
	}
	if w.Download != nil {
		st.Download, st.DownloadID, st.DownloadRef, st.DownloadAttempt = w.Download.Stage, w.Download.ID, w.Download.Ref.ref(), w.Download.Attempt
	}
	if err := st.check(); err != nil {
		return state{}, fmt.Errorf("decode job state: %w", err)
	}
	if fence.Version == 1 && st.Hold == HoldAcceptedReply {
		return state{}, errors.New("decode job state: version 1 has no such hold")
	}
	again, err := encodeVersion(st, fence.Version)
	if err != nil {
		return state{}, fmt.Errorf("decode job state: %w", err)
	}
	if !bytes.Equal(again, data) {
		return state{}, errors.New("decode job state: not in canonical form")
	}
	return st, nil
}

// check reports the first value or combination of values st could not
// hold if this package wrote it.
func (st state) check() error {
	in := func(v string, set ...string) bool { return slices.Contains(set, v) }
	snap := st.Snapshot
	snap.ID = placeholderID
	switch {
	case st.Created.IsZero() || !hex64.MatchString(st.ConfigDigest):
		return errors.New("invalid binding")
	case st.VM.check() != nil:
		return errors.New("invalid VM binding")
	case st.Agent != nil && (!commitRE.MatchString(st.Agent.Commit) || !hex64.MatchString(st.Agent.Digest)):
		return errors.New("invalid agent binding")
	case !hexID32.MatchString(st.RequestID):
		return errors.New("invalid request ID")
	case snap.Validate() != nil || st.Snapshot.ID != "":
		return errors.New("invalid snapshot")
	case !in(st.Retention, "", RetentionUnimplemented, RetentionDisabled, RetentionEnabled):
		return errors.New("invalid retention mode")
	case (st.Retention == RetentionEnabled) != (st.Lineage != ""),
		st.Retention == RetentionEnabled && (!hexID32.MatchString(st.Lineage) || st.Epoch == 0 || st.RetryUntil.IsZero()),
		st.Retention != RetentionEnabled && (st.Epoch != 0 || !st.RetryUntil.IsZero()):
		return errors.New("invalid request domain")
	case !in(st.Desired, desiredRun, desiredCancel):
		return errors.New("invalid desired state")
	case !in(st.Upload, uploadNone, uploadBeginSent, uploadOpen, uploadFinalize, uploadDone):
		return errors.New("invalid upload stage")
	case st.Upload != uploadNone && st.Retention == "":
		return errors.New("upload before the retention mode was recorded")
	case in(st.Upload, uploadNone, uploadBeginSent) != (st.UploadID == ""),
		st.UploadID != "" && !hexID32.MatchString(st.UploadID),
		st.Acknowledged < 0 || st.Acknowledged > st.Snapshot.Size,
		in(st.Upload, uploadNone, uploadBeginSent) != (st.Workdir == artifact.Ref{}),
		st.Workdir != (artifact.Ref{}) && (st.Workdir.Validate() != nil || st.Workdir.SHA256 != st.Snapshot.SHA256 || st.Workdir.Size != st.Snapshot.Size),
		st.Upload == uploadDone && st.Acknowledged != st.Snapshot.Size:
		return errors.New("invalid upload progress")
	case !in(st.Submit, submitNone, submitSent, submitAccepted):
		return errors.New("invalid submit stage")
	case st.Submit != submitNone && st.Upload != uploadDone:
		return errors.New("submit before the workdir was finalized")
	case !in(st.Cleanup, cleanupNone, cleanupSent, cleanupDelivered),
		st.Cleanup != cleanupNone && (st.Desired != desiredCancel || st.Submit != submitNone || !in(st.Upload, uploadOpen, uploadFinalize)):
		return errors.New("invalid upload cleanup")
	case (st.Submit == submitAccepted) != (st.JobID != ""),
		st.JobID != "" && !hexID32.MatchString(st.JobID):
		return errors.New("invalid job ID")
	case !in(st.Cancel, cancelNone, cancelSent, cancelDelivered),
		st.Cancel != cancelNone && (st.Desired != desiredCancel || st.JobID == ""):
		return errors.New("invalid cancel progress")
	case !in(st.Observed, "", "QUEUED", "STARTING", "RUNNING", "RETRYING", "SUCCEEDED", "FAILED", "CANCELED", "UNKNOWN", "NEVER_SUBMITTED"),
		(st.Observed == "") != st.ObservedAt.IsZero(),
		st.ExitCode != nil && st.Observed == "":
		return errors.New("invalid observation")
	case !in(st.Output, outputNone, outputSaved, outputAbsent, outputExpired, outputHeld, outputNotUsed),
		(st.Output == outputSaved) != (st.OutputRef != artifact.Ref{}),
		st.Output == outputSaved && (st.OutputRef.Validate() != nil || !hexID32.MatchString(st.OutputAttempt)):
		return errors.New("invalid output")
	case !in(st.Download, downloadNone, downloadBegin, downloadOpen, downloadClose, downloadClosed),
		st.Download == downloadNone && (st.DownloadID != "" || st.DownloadRef != (artifact.Ref{}) || st.DownloadAttempt != ""),
		st.Download != downloadNone && (st.JobID == "" || st.DownloadRef.Validate() != nil || !hexID32.MatchString(st.DownloadAttempt)),
		st.Download != downloadNone && (st.Download == downloadBegin) != (st.DownloadID == ""),
		st.DownloadID != "" && !hexID32.MatchString(st.DownloadID),
		st.Download != downloadNone && st.Output == outputSaved && (st.Download != downloadClosed || st.OutputRef != st.DownloadRef || st.OutputAttempt != st.DownloadAttempt):
		return errors.New("invalid result download")
	case st.LogOffset < 0 || st.LogOffset > MaxLogBytes,
		st.LogAttempt != "" && !hexID32.MatchString(st.LogAttempt),
		st.JobID == "" && (st.LogOffset != 0 || st.LogAttempt != "" || st.LogTruncated || st.LogComplete):
		return errors.New("invalid log checkpoint")
	case !in(st.Hold, "", HoldUploadBegin, HoldCanceledSubmit, HoldAcceptedTask, HoldAcceptedReply, HoldDownloadBegin, HoldDownloadClose),
		st.Hold == HoldDownloadBegin && st.Download != downloadBegin,
		st.Hold == HoldDownloadClose && st.Download != downloadClose:
		return errors.New("invalid hold")
	}
	_, err := decodeTask(st.Task)
	return err
}

// encodeTask returns the deterministic protobuf encoding of t, which
// Create stores once. Nothing compares a stored task with a new encoding.
func encodeTask(t *v1.Task) ([]byte, error) {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(t)
	if err != nil {
		return nil, errors.New("encode task")
	}
	return b, nil
}

// decodeTask decodes a stored task. It refuses bytes that are not a task,
// a task naming a workdir, and anything this package never stores: a
// retry policy, data mounts and fields this version does not know, which
// a newer writer may have stored and this one cannot send faithfully.
// The Task has no other message fields that could hold unknown fields.
func decodeTask(b []byte) (*v1.Task, error) {
	if len(b) == 0 || len(b) > maxTaskBytes {
		return nil, errors.New("invalid task encoding")
	}
	t := new(v1.Task)
	if err := proto.Unmarshal(b, t); err != nil {
		return nil, errors.New("invalid task encoding")
	}
	switch {
	case len(t.ProtoReflect().GetUnknown()) != 0:
		return nil, errors.New("journal task holds fields this version does not know")
	case t.Workdir != nil:
		return nil, errors.New("journal task names a workdir")
	case t.Retry != nil || len(t.DataMounts) != 0:
		return nil, errors.New("journal task holds a retry policy or data mounts")
	}
	return t, nil
}

// withWorkdir returns a copy of t whose workdir is ref.
func withWorkdir(t *v1.Task, ref *v1.ArtifactRef) *v1.Task {
	c := proto.Clone(t).(*v1.Task)
	c.Workdir = ref
	return c
}
