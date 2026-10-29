package cli

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/artifact"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const workspaceVersion = 1
const workspaceInputLimit = 8 << 20
const workspaceStateLimit = 16 << 10

type workspaceIntent struct {
	Version          int
	Endpoint         string
	CredentialSHA256 string
	RequestID        string
	RequestedDomain  string
	YAML             []byte
	Shell            string
	Target           string
	Infra            string
	BindingTarget    string
	Workdir          string
	Env              []string
	Outputs          []string
}

type workspaceState struct {
	IntentSHA256  string
	Checksum      string `json:",omitempty"`
	Version       int
	Phase         string
	ArchiveSHA256 string
	ArchiveSize   int64
	Domain        *v1.RequestDomain
	Upload        *v1.ArtifactRef
	TaskSHA256    string
	JobID         string
}

// workspaceRun owns the lock and every concrete directory/failed-close owner.
// A failed operation poisons this invocation; it cannot authorize later effects.
type workspaceRun struct {
	dir          string
	intent       workspaceIntent
	intentSHA256 string
	state        workspaceState
	archive      []byte
	task         []byte
	root         *os.Root
	directory    *os.File
	lock         *os.File
	failed       error
	closed       bool
}

type workspaceCloseError struct {
	file *os.File
	root *os.Root
	err  error
}

func (e *workspaceCloseError) Error() string { return e.err.Error() }
func (e *workspaceCloseError) Unwrap() error { return e.err }

func workspaceHash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func workspaceHex(s string, n int) bool {
	return len(s) == n && strings.Trim(s, "0123456789abcdef") == ""
}

func validWorkspaceIntent(in workspaceIntent) bool {
	if in.RequestedDomain != "" {
		if _, err := parseRequestDomain(in.RequestedDomain); err != nil {
			return false
		}
	}
	return in.Version == workspaceVersion && in.Endpoint != "" && workspaceHex(in.CredentialSHA256, 64) && validRequestID(in.RequestID) && len(in.YAML) > 0 && len(in.YAML) <= workspaceInputLimit && filepath.IsAbs(in.Workdir) && filepath.Clean(in.Workdir) == in.Workdir && filepath.IsAbs(in.Shell) && ((in.Infra == "") == (in.BindingTarget == ""))
}

func workspacePhase(phase string) int {
	switch phase {
	case "prepared":
		return 1
	case "begin":
		return 2
	case "upload":
		return 3
	case "finalize":
		return 4
	case "sealed":
		return 5
	case "submit":
		return 6
	case "accepted":
		return 7
	}
	return 0
}

// workspaceStateChecksum covers every state payload field, including the
// immutable intent digest. It detects corruption, not same-user tampering.
func workspaceStateChecksum(s workspaceState) (string, error) {
	s.Checksum = ""
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return workspaceHash(b), nil
}

func validWorkspaceState(s workspaceState) bool {
	if !workspaceHex(s.IntentSHA256, 64) || !workspaceHex(s.Checksum, 64) {
		return false
	}
	phase := workspacePhase(s.Phase)
	if s.Version != workspaceVersion || phase == 0 || !workspaceHex(s.ArchiveSHA256, 64) || s.ArchiveSize < 1 || s.ArchiveSize > artifact.MaxSize {
		return false
	}
	if s.Domain != nil && (!workspaceHex(s.Domain.Lineage, 32) || s.Domain.Epoch == 0) {
		return false
	}
	if (phase >= 2) != (s.Domain != nil) {
		return false
	}
	if s.Upload != nil && (!workspaceHex(s.Upload.Id, 32) || s.Upload.Sha256 != s.ArchiveSHA256 || s.Upload.Size != s.ArchiveSize) {
		return false
	}
	if (phase >= 3) != (s.Upload != nil) {
		return false
	}
	if s.TaskSHA256 != "" && !workspaceHex(s.TaskSHA256, 64) {
		return false
	}
	if phase >= 5 && s.TaskSHA256 == "" {
		return false
	}
	if phase < 5 && s.TaskSHA256 != "" {
		return false
	}
	if s.JobID != "" && !workspaceHex(s.JobID, 32) {
		return false
	}
	return (phase == 7) == (s.JobID != "")
}

// openWorkspaceRun creates or resumes a single private intent. A nil YAML is
// an explicit resume: only binding continuity is supplied; frozen input wins.
func openWorkspaceRun(directory string, in workspaceIntent) (*workspaceRun, error) {
	return openWorkspaceRunWithSync(directory, in, syncWorkspaceDirectory)
}

func openWorkspaceRunWithSync(directory string, in workspaceIntent, syncParent func(string) error) (run *workspaceRun, err error) {
	if err = workspacePlatform(); err != nil {
		return nil, err
	}
	if in.Endpoint == "" || !workspaceHex(in.CredentialSHA256, 64) {
		return nil, errors.New("invalid workspace transport binding")
	}
	boundary := ""
	if directory == "" {
		if in.YAML == nil {
			return nil, errors.New("workspace resume requires its state directory")
		}
		if in.RequestID == "" {
			var id [16]byte
			if _, err = rand.Read(id[:]); err != nil {
				return nil, err
			}
			in.RequestID = hex.EncodeToString(id[:])
		}
		cache, e := os.UserCacheDir()
		if e != nil {
			return nil, e
		}
		cache, e = cleanWorkspacePath(cache)
		if e != nil {
			return nil, e
		}
		if e = prepareWorkspaceCache(cache, syncParent); e != nil {
			return nil, e
		}
		boundary = filepath.Join(cache, "skygo")
		directory = filepath.Join(boundary, "workspaces", workspaceHash([]byte(in.Endpoint+"\x00"+in.RequestID)))
	} else {
		directory, err = cleanWorkspacePath(directory)
		if err != nil {
			return nil, err
		}
		boundary = directory
	}
	parts, e := workspacePathWithSync(directory, in.YAML != nil, boundary, syncParent)
	if e != nil {
		return nil, e
	}
	root, e := os.OpenRoot(directory)
	if e != nil {
		return nil, e
	}
	run = &workspaceRun{dir: directory, root: root}
	// Keep the owner even on failure; Close preserves concrete uncertain handles.
	defer func() {
		if err != nil {
			err = errors.Join(err, run.Close())
		}
	}()
	run.directory, err = root.OpenFile(".", os.O_RDONLY|workspaceNoFollow, 0)
	if err != nil {
		return run, err
	}
	info, e := run.directory.Stat()
	if e != nil {
		return run, e
	}
	if !os.SameFile(info, parts[len(parts)-1].info) {
		return run, errors.New("workspace state directory changed")
	}
	if err = recheckWorkspacePath(parts); err != nil {
		return run, err
	}
	createdLock := false
	lockInfo, e := root.Lstat("lock")
	if errors.Is(e, os.ErrNotExist) {
		run.lock, err = root.OpenFile("lock", os.O_RDWR|os.O_CREATE|os.O_EXCL|workspaceNoFollow, 0600)
		createdLock = err == nil
	} else if e != nil {
		return run, e
	} else {
		if err = checkWorkspaceFile(lockInfo); err != nil {
			return run, err
		}
		run.lock, err = root.OpenFile("lock", os.O_RDWR|workspaceNoFollow, 0)
	}
	if err != nil {
		return run, err
	}
	opened, e := run.lock.Stat()
	if e != nil {
		return run, e
	}
	if err = checkWorkspaceFile(opened); err != nil {
		return run, err
	}
	if lockInfo != nil && !os.SameFile(lockInfo, opened) {
		return run, errors.New("workspace lock changed")
	}
	if opened.Size() != 0 {
		return run, errors.New("invalid workspace lock")
	}
	if err = lockWorkspace(run.lock); err != nil {
		return run, err
	}
	entries, e := run.inspect()
	if e != nil {
		return run, e
	}
	if len(entries) == 1 && entries["lock"] {
		if !createdLock {
			return run, errors.New("workspace intent is incomplete; manual recovery required")
		}
		if in.YAML == nil {
			return run, errors.New("workspace intent is incomplete")
		}
		if in.RequestID == "" {
			var id [16]byte
			if _, err = rand.Read(id[:]); err != nil {
				return run, err
			}
			in.RequestID = hex.EncodeToString(id[:])
		}
		in.Version = workspaceVersion
		if !validWorkspaceIntent(in) {
			return run, errors.New("invalid workspace intent")
		}
		data, e := json.Marshal(in)
		if e != nil {
			return run, e
		}
		if err = decodeWorkspaceJSON(data, &run.intent); err != nil {
			return run, err
		}
		if len(data) > workspaceInputLimit {
			return run, errors.New("workspace input exceeds 8 MiB")
		}
		run.intentSHA256 = workspaceHash(data)
		if err = run.publish("intent.json", data, false); err != nil {
			return run, err
		}
		return run, nil
	}
	if !entries["intent.json"] || !entries["archive.tar"] || !entries["state.json"] {
		return run, errors.New("workspace intent is incomplete; manual recovery required")
	}
	b, e := run.read("intent.json", workspaceInputLimit)
	if e != nil {
		return run, e
	}
	if err = decodeWorkspaceJSON(b, &run.intent); err != nil || !validWorkspaceIntent(run.intent) {
		return run, errors.New("invalid workspace intent record")
	}
	run.intentSHA256 = workspaceHash(b)
	if in.Endpoint != run.intent.Endpoint || in.CredentialSHA256 != run.intent.CredentialSHA256 || (in.RequestID != "" && in.RequestID != run.intent.RequestID) {
		return run, errors.New("workspace transport or request binding changed")
	}
	if in.YAML != nil {
		in.Version = workspaceVersion
		if in.RequestID == "" {
			in.RequestID = run.intent.RequestID
		}
		if !reflect.DeepEqual(in, run.intent) {
			return run, errors.New("workspace input does not match frozen intent")
		}
	}
	b, e = run.read("state.json", workspaceStateLimit)
	if e != nil {
		return run, e
	}
	if err = decodeWorkspaceJSON(b, &run.state); err != nil || !validWorkspaceState(run.state) {
		return run, errors.New("invalid workspace state record")
	}
	checksum, e := workspaceStateChecksum(run.state)
	if e != nil || run.state.IntentSHA256 != run.intentSHA256 || run.state.Checksum != checksum {
		return run, errors.New("workspace state integrity mismatch")
	}
	run.archive, err = run.read("archive.tar", artifact.MaxSize)
	if err != nil {
		return run, err
	}
	digest, e := artifact.Validate(bytes.NewReader(run.archive), int64(len(run.archive)))
	if e != nil || digest != run.state.ArchiveSHA256 || int64(len(run.archive)) != run.state.ArchiveSize {
		return run, errors.New("workspace archive does not match frozen state")
	}
	if entries["task.json"] {
		run.task, err = run.read("task.json", workspaceInputLimit)
		if err != nil {
			return run, err
		}
		task := new(v1.Task)
		if run.state.TaskSHA256 != workspaceHash(run.task) || protojson.Unmarshal(run.task, task) != nil || !proto.Equal(task.Workdir, run.state.Upload) {
			return run, errors.New("workspace task does not match frozen state")
		}
	} else if run.state.TaskSHA256 != "" {
		return run, errors.New("workspace task is missing")
	}
	return run, nil
}

func checkWorkspaceFile(info os.FileInfo) error {
	if !info.Mode().IsRegular() {
		return errors.New("workspace state entry is not a regular file")
	}
	return workspacePermissions(info, true, false)
}

// inspect bounds names and byte sizes before reading any retained content.
// Interrupted exclusive staging remains evidence, never a commit record.
func (r *workspaceRun) inspect() (map[string]bool, error) {
	f, err := r.root.Open(".")
	if err != nil {
		return nil, err
	}
	entries, readErr := f.ReadDir(10)
	closeErr := f.Close()
	if closeErr != nil {
		closeErr = &workspaceCloseError{file: f, err: closeErr}
	}
	if errors.Is(readErr, io.EOF) {
		readErr = nil
	}
	if err = errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	if len(entries) > 9 {
		return nil, errors.New("unknown workspace state entries")
	}
	limits := map[string]int64{"lock": 0, "intent.json": workspaceInputLimit, "archive.tar": artifact.MaxSize, "state.json": workspaceStateLimit, "task.json": workspaceInputLimit}
	names := make(map[string]bool, len(entries))
	staging := false
	for _, entry := range entries {
		name := entry.Name()
		base := strings.TrimSuffix(name, ".next")
		limit, ok := limits[base]
		if !ok || base == "lock" && name != "lock" {
			return nil, errors.New("unknown workspace state entry")
		}
		info, e := r.root.Lstat(name)
		if e != nil {
			return nil, e
		}
		if e = checkWorkspaceFile(info); e != nil {
			return nil, e
		}
		if info.Size() > limit {
			return nil, errors.New("workspace state entry exceeds bound")
		}
		if name != base {
			staging = true
		}
		names[name] = true
	}
	if staging {
		return nil, errors.New("workspace staging file remains; manual recovery required")
	}
	if !names["lock"] {
		return nil, errors.New("workspace lock is missing")
	}
	return names, nil
}

func (r *workspaceRun) read(name string, limit int64) (b []byte, err error) {
	before, err := r.root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if err = checkWorkspaceFile(before); err != nil {
		return nil, err
	}
	f, err := r.root.OpenFile(name, os.O_RDONLY|workspaceNoFollow, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		e := f.Close()
		if e != nil {
			e = &workspaceCloseError{file: f, err: e}
		}
		err = errors.Join(err, e)
	}()
	opened, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if e := checkWorkspaceFile(opened); e != nil {
		return nil, e
	}
	if !os.SameFile(before, opened) || opened.Size() > limit {
		return nil, errors.New("workspace state file changed or exceeds bound")
	}
	b, err = io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	after, e := f.Stat()
	if e != nil {
		return nil, e
	}
	current, e := r.root.Lstat(name)
	if e != nil {
		return nil, e
	}
	if e := checkWorkspaceFile(current); e != nil {
		return nil, e
	}
	if len(b) > int(limit) || int64(len(b)) != opened.Size() || !os.SameFile(opened, current) || after.Size() != opened.Size() || !after.ModTime().Equal(opened.ModTime()) {
		return nil, errors.New("workspace state file changed")
	}
	return b, nil
}

func (r *workspaceRun) publish(name string, b []byte, replace bool) (err error) {
	if r.failed != nil {
		return r.failed
	}
	if r.closed {
		return errors.New("workspace state is closed")
	}
	defer func() {
		if err != nil {
			r.failed = err
		}
	}()
	if !replace {
		if _, e := r.root.Lstat(name); !errors.Is(e, os.ErrNotExist) {
			return errors.New("workspace immutable file already exists")
		}
	}
	f, e := r.root.OpenFile(name+".next", os.O_WRONLY|os.O_CREATE|os.O_EXCL|workspaceNoFollow, 0600)
	if e != nil {
		return e
	}
	info, writeErr := f.Stat()
	if writeErr == nil {
		writeErr = checkWorkspaceFile(info)
	}
	n := 0
	if writeErr == nil {
		n, writeErr = f.Write(b)
	}
	if writeErr == nil && n != len(b) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if closeErr != nil {
		closeErr = &workspaceCloseError{file: f, err: closeErr}
	}
	if err = errors.Join(writeErr, closeErr); err != nil {
		return err
	}
	if err = renameWorkspaceFile(r.directory, name+".next", name); err != nil {
		return err
	}
	return r.directory.Sync()
}

func (r *workspaceRun) saveArchive(archive []byte) error {
	if r.failed != nil {
		return r.failed
	}
	digest, err := artifact.Validate(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		return err
	}
	if r.archive != nil {
		if !bytes.Equal(archive, r.archive) {
			return errors.New("workspace archive is immutable")
		}
		return nil
	}
	if err = r.publish("archive.tar", archive, false); err != nil {
		return err
	}
	next := workspaceState{Version: workspaceVersion, Phase: "prepared", ArchiveSHA256: digest, ArchiveSize: int64(len(archive))}
	if err = r.saveState(next); err != nil {
		return err
	}
	r.archive = bytes.Clone(archive)
	return nil
}

func (r *workspaceRun) saveState(next workspaceState) error {
	if r.failed != nil {
		return r.failed
	}
	intentBytes, err := r.read("intent.json", workspaceInputLimit)
	if err != nil {
		return err
	}
	currentIntent, err := json.Marshal(r.intent)
	if err != nil {
		return err
	}
	if r.intentSHA256 == "" || workspaceHash(intentBytes) != r.intentSHA256 || !bytes.Equal(intentBytes, currentIntent) {
		return errors.New("workspace immutable intent binding changed")
	}
	old := r.state
	if old.Version != 0 {
		checksum, err := workspaceStateChecksum(old)
		if err != nil || old.IntentSHA256 != r.intentSHA256 || old.Checksum != checksum {
			return errors.New("workspace current state integrity mismatch")
		}
	}
	if next.IntentSHA256 == "" && old.Version == 0 {
		next.IntentSHA256 = r.intentSHA256
	}
	if next.IntentSHA256 != r.intentSHA256 {
		return errors.New("workspace intent digest changed")
	}
	next.Checksum, err = workspaceStateChecksum(next)
	if err != nil {
		return err
	}
	if !validWorkspaceState(next) {
		return errors.New("invalid workspace state transition")
	}
	if old.Version != 0 && (workspacePhase(next.Phase) < workspacePhase(old.Phase) || old.ArchiveSHA256 != next.ArchiveSHA256 || old.ArchiveSize != next.ArchiveSize || old.Domain != nil && !proto.Equal(old.Domain, next.Domain) || old.Upload != nil && !proto.Equal(old.Upload, next.Upload) || old.TaskSHA256 != "" && old.TaskSHA256 != next.TaskSHA256 || old.JobID != "" && old.JobID != next.JobID) {
		return errors.New("workspace state binding changed")
	}
	if reflect.DeepEqual(old, next) {
		return nil
	}
	b, err := json.Marshal(next)
	if err != nil {
		return err
	}
	if len(b) > workspaceStateLimit {
		return errors.New("workspace state exceeds 16 KiB")
	}
	if err = r.publish("state.json", b, old.Version != 0); err != nil {
		return err
	}
	r.state = next
	if next.Domain != nil {
		r.state.Domain = proto.Clone(next.Domain).(*v1.RequestDomain)
	}
	if next.Upload != nil {
		r.state.Upload = proto.Clone(next.Upload).(*v1.ArtifactRef)
	}
	return nil
}

func (r *workspaceRun) saveTask(task []byte) error {
	if r.failed != nil {
		return r.failed
	}
	if len(task) == 0 || len(task) > workspaceInputLimit {
		return errors.New("workspace task exceeds bound")
	}
	parsed := new(v1.Task)
	if err := protojson.Unmarshal(task, parsed); err != nil {
		return errors.New("invalid workspace task")
	}
	if !proto.Equal(parsed.Workdir, r.state.Upload) {
		return errors.New("workspace task reference changed")
	}
	if r.task != nil {
		if !bytes.Equal(r.task, task) {
			return errors.New("workspace task is immutable")
		}
		return nil
	}
	if r.state.Upload == nil || r.state.Phase != "finalize" {
		return errors.New("workspace task requires a finalized upload")
	}
	if err := r.publish("task.json", task, false); err != nil {
		return err
	}
	next := r.state
	next.Phase = "sealed"
	next.TaskSHA256 = workspaceHash(task)
	if err := r.saveState(next); err != nil {
		return err
	}
	r.task = bytes.Clone(task)
	return nil
}

func (r *workspaceRun) Close() error {
	if r == nil {
		return nil
	}
	if r.closed {
		return r.failed
	}
	r.closed = true
	var result error
	// Never explicitly unlock: only a positive close joins the concrete lock owner.
	for _, f := range []*os.File{r.directory, r.lock} {
		if f != nil {
			if err := f.Close(); err != nil {
				result = errors.Join(result, &workspaceCloseError{file: f, err: err})
			}
		}
	}
	if r.root != nil {
		if err := r.root.Close(); err != nil {
			result = errors.Join(result, &workspaceCloseError{root: r.root, err: err})
		}
	}
	r.failed = errors.Join(r.failed, result)
	return r.failed
}

// decodeWorkspaceJSON accepts only this version's exact writer representation.
// Canonical re-encoding rejects duplicate fields, decoder case aliases and extra
// values without maintaining a second JSON grammar or Unicode folding table.
func decodeWorkspaceJSON(b []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	canonical, err := json.Marshal(out)
	if err != nil {
		return err
	}
	if !bytes.Equal(b, canonical) {
		return errors.New("noncanonical workspace JSON")
	}
	return nil
}
