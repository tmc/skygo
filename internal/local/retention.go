package local

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/tmc/skygo/internal/backend"
)

// RetentionConfig comes from the trusted stopped-state policy and reservation
// index. Outputs includes unclosed reservations as well as persisted closures.
// It is not a client-supplied grant to adopt files or processes.
type RetentionConfig struct {
	Owner                                   string
	OutputUnit, Volume                      uint64
	OutputBytes, OutputDomains, OutputFiles uint64
	Outputs                                 []backend.OutputReference
}

// OutputUsage separates observed allocation from logical bytes and reservations.
// An error makes the entire sample unavailable; zero is then not a measurement.
type OutputUsage struct{ AllocatedBytes, LogicalBytes, Files uint64 }

type retentionState struct {
	config     RetentionConfig
	binding    string
	root       os.FileInfo
	reserve    uint64
	outputs    map[string]*retainedOutput
	key        [32]byte
	syncOutput func(string) error
	removeFile func(string) error
}

type retainedOutput struct {
	reference                 backend.OutputReference
	closure                   string
	closed, released, removed bool
	taskHash                  [32]byte
	hasTask                   bool
	log, marker               os.FileInfo
	historical                bool
}

// NewRetained opens the actual built-in local target against its persisted policy.
// The caller must supply only store-validated output reservations before serving.
// The zero RetentionConfig is invalid. New preserves disabled retention behavior.
func NewRetained(directory string, config RetentionConfig) (*Backend, error) {
	binding, err := backend.LocalBinding(config.Owner)
	if err != nil {
		return nil, err
	}
	if config.OutputUnit < 4096 || config.OutputUnit > 65536 || config.OutputUnit&(config.OutputUnit-1) != 0 || config.OutputBytes == 0 || config.OutputDomains == 0 || config.OutputFiles == 0 || len(config.Outputs) > 256 || config.OutputDomains > 256 || config.OutputFiles > 512 || config.OutputBytes > 256<<20 {
		return nil, errors.New("invalid local output policy")
	}
	info, statErr := os.Lstat(directory)
	if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
		return nil, statErr
	}
	if statErr == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
		return nil, errors.New("output root is not a directory")
	}
	b, err := New(directory)
	if err != nil {
		return nil, err
	}
	root, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	if !root.IsDir() || root.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("output root is not a directory")
	}
	unit, volume, err := outputFilesystem(directory)
	if err != nil {
		return nil, err
	}
	if unit != config.OutputUnit || volume != config.Volume {
		return nil, errors.New("output volume requires stopped-state revalidation")
	}
	config.Owner = strings.Clone(config.Owner)
	b.retention = &retentionState{config: config, binding: binding, root: root, reserve: uint64(backend.MaxLogBytes) + 4*unit, outputs: make(map[string]*retainedOutput), syncOutput: syncOutputDirectory, removeFile: os.Remove}
	if _, err := rand.Read(b.retention.key[:]); err != nil {
		return nil, fmt.Errorf("create output receipt key: %w", err)
	}
	// Do not retain the caller's aggregate slice or any unrelated backing storage.
	b.retention.config.Outputs = nil
	for _, ref := range config.Outputs {
		ref = compactOutputReference(ref)
		if err := b.validOutputReference(ref); err != nil {
			return nil, err
		}
		if ref.ClosureID == "" && ref.PayloadReleased || ref.ClosureID != "" && !validClosure(ref.ClosureID) {
			return nil, errors.New("invalid persisted output closure")
		}
		if _, exists := b.retention.outputs[ref.AttemptID]; exists {
			return nil, errors.New("duplicate output reservation")
		}
		r := &retainedOutput{reference: ref, closure: ref.ClosureID, closed: ref.ClosureID != "", released: ref.PayloadReleased, historical: true}
		if err := b.inspectDomain(r); err != nil {
			return nil, err
		}
		b.retention.outputs[ref.AttemptID] = r
	}
	// Sampling validates inventory and arithmetic, but over-budget stopped-state
	// inventories stay inspectable/collectible; admitOutput refuses their growth.
	if _, err := b.outputUsage(context.Background()); err != nil {
		return nil, err
	}
	return b, nil
}

// OutputReservation is the validated per-execution estimate, or zero when disabled.
func (b *Backend) OutputReservation() uint64 {
	if b.retention == nil {
		return 0
	}
	return b.retention.reserve
}

// Retention returns trusted callbacks only for a validated retained constructor.
func (b *Backend) Retention() *backend.Retention {
	if b.retention == nil {
		return nil
	}
	return &backend.Retention{CheckOutputClosed: b.checkOutputClosed, RemoveOutput: b.removeOutput, RemoveExecution: b.removeExecution}
}

// BindOutput binds a committed reservation before its first local Start. The
// actual control worker supplies this reference; it is not an inbound API.
func (b *Backend) BindOutput(ref backend.OutputReference) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.retention == nil {
		return errors.New("local retention is disabled")
	}
	if b.closed {
		return errors.New("local backend is closed")
	}
	if err := b.validOutputReference(ref); err != nil {
		return err
	}
	if ref.ClosureID != "" || ref.PayloadReleased {
		return errors.New("start binding cannot supply closure proof")
	}
	if r := b.retention.outputs[ref.AttemptID]; r != nil {
		if sameOutput(r.reference, ref) && !r.closed && !r.removed {
			return nil
		}
		return backend.ErrIdentityMismatch
	}
	domains := b.outputDomains()
	policy := b.retention.config
	if domains >= policy.OutputDomains || domains >= policy.OutputBytes/b.retention.reserve || domains >= policy.OutputFiles/2 {
		return errors.New("local output domain limit reached")
	}
	ref = compactOutputReference(ref)
	b.retention.outputs[ref.AttemptID] = &retainedOutput{reference: ref, closure: ""}
	return nil
}

func sameOutput(a, b backend.OutputReference) bool {
	return a.Owner == b.Owner && a.JobID == b.JobID && a.AttemptID == b.AttemptID && a.Binding == b.Binding && a.Generation == b.Generation
}

func validClosure(s string) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}

func (b *Backend) validOutputReference(ref backend.OutputReference) error {
	if ref.Owner != b.retention.config.Owner || !validID.MatchString(ref.JobID) || !validID.MatchString(ref.AttemptID) || ref.Binding != b.retention.binding || ref.Generation != 1 {
		return backend.ErrIdentityMismatch
	}
	return nil
}

// admitOutput holds b.mu, after duplicate execution checks and before file create.
func (b *Backend) admitOutput(ctx context.Context, id string, task backend.Task) error {
	if b.retention == nil {
		return nil
	}
	r := b.retention.outputs[id]
	if r == nil || r.closed || r.removed || r.hasTask {
		return errors.New("local output reservation is not bound for start")
	}
	usage, err := b.outputUsage(ctx)
	if err != nil {
		return err
	}
	policy := b.retention.config
	domains := b.outputDomains()
	if domains > policy.OutputBytes/b.retention.reserve || domains > policy.OutputFiles/2 || usage.AllocatedBytes > domains*b.retention.reserve || usage.AllocatedBytes > policy.OutputBytes || usage.Files > policy.OutputFiles {
		return errors.New("local output capacity reached")
	}
	data, err := backend.TaskBytes(task)
	if err != nil {
		return err
	}
	r.taskHash, r.hasTask = sha256.Sum256(data), true
	return nil
}

// OutputUsage samples the actual private root outside any store transaction.
func (b *Backend) OutputUsage(ctx context.Context) (OutputUsage, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.retention == nil {
		return OutputUsage{}, errors.New("local retention is disabled")
	}
	return b.outputUsage(ctx)
}

func (b *Backend) outputUsage(ctx context.Context) (OutputUsage, error) {
	var out OutputUsage
	if err := ctx.Err(); err != nil {
		return out, err
	}
	r := b.retention
	if err := b.checkOutputRoot(); err != nil {
		return out, err
	}
	f, err := os.Open(b.directory)
	if err != nil {
		return out, err
	}
	defer f.Close()
	entries, err := f.ReadDir(513)
	if err != nil && !errors.Is(err, io.EOF) {
		return out, err
	}
	if len(entries) > 512 {
		return out, errors.New("output inventory exceeds file limit")
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return OutputUsage{}, err
		}
		name := entry.Name()
		id := strings.TrimSuffix(name, ".log")
		marker := false
		if id == name {
			id = strings.TrimSuffix(name, ".truncated")
			marker = true
		}
		if id == name || !validID.MatchString(id) || (r.outputs[id] == nil || r.outputs[id].removed) {
			return OutputUsage{}, errors.New("unexplained output entry")
		}
		info, err := os.Lstat(filepath.Join(b.directory, name))
		if err != nil {
			return OutputUsage{}, err
		}
		domain := r.outputs[id]
		expected := domain.log
		if marker {
			if domain.closed {
				expected = domain.marker
			} else {
				expected = info
			}
		}
		if expected == nil || !os.SameFile(expected, info) {
			return OutputUsage{}, backend.ErrIdentityMismatch
		}
		if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > backend.MaxLogBytes || marker && info.Size() > 64 {
			return OutputUsage{}, errors.New("invalid retained output file")
		}
		n, err := allocatedOutput(info)
		if err != nil {
			return OutputUsage{}, err
		}
		if n > math.MaxUint64-out.AllocatedBytes || uint64(info.Size()) > math.MaxUint64-out.LogicalBytes {
			return OutputUsage{}, errors.New("output measurement overflow")
		}
		out.AllocatedBytes += n
		out.LogicalBytes += uint64(info.Size())
		out.Files++
	}
	return out, nil
}

func (b *Backend) inspectDomain(r *retainedOutput) error {
	for i, suffix := range []string{".log", ".truncated"} {
		info, err := os.Lstat(filepath.Join(b.directory, r.reference.AttemptID+suffix))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > backend.MaxLogBytes || i == 1 && info.Size() > 64 {
			return errors.New("invalid retained output identity")
		}
		if i == 0 {
			r.log = info
		} else {
			r.marker = info
		}
	}
	if r.marker != nil && r.log == nil {
		return errors.New("output marker has no log")
	}
	return nil
}

func (b *Backend) checkOutputClosed(ctx context.Context, ref backend.OutputReference) (backend.OutputReceipt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return backend.OutputReceipt{}, err
	}
	if err := b.validOutputReference(ref); err != nil {
		return backend.OutputReceipt{}, err
	}
	r := b.retention.outputs[ref.AttemptID]
	if r == nil {
		if e := b.jobs[ref.AttemptID]; e != nil {
			r = e.retained
		}
	}
	if r == nil || !sameOutput(r.reference, ref) || !r.closed || r.removed {
		return backend.OutputReceipt{}, errors.New("output closure unavailable")
	}
	if ref.ClosureID != "" && ref.ClosureID != r.closure || ref.PayloadReleased && !r.released {
		return backend.OutputReceipt{}, backend.ErrIdentityMismatch
	}
	return backend.OutputReceipt{ClosureID: r.closure, PayloadReleased: r.released}, nil
}

func (b *Backend) removeOutput(ctx context.Context, ref backend.OutputReference) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.validOutputReference(ref); err != nil {
		return err
	}
	if !validClosure(ref.ClosureID) {
		return backend.ErrIdentityMismatch
	}
	if err := b.checkOutputRoot(); err != nil {
		return err
	}
	r := b.retention.outputs[ref.AttemptID]
	if r == nil {
		if !hmac.Equal([]byte(ref.ClosureID), []byte(b.outputClosure(ref))) {
			return backend.ErrIdentityMismatch
		}
		// The trusted GC dispatcher already checked this durable immutable receipt.
		// Absence is cleanup absence only; it cannot mint a closure or stopped proof.
		for _, suffix := range []string{".log", ".truncated"} {
			if _, err := os.Lstat(filepath.Join(b.directory, ref.AttemptID+suffix)); !errors.Is(err, fs.ErrNotExist) {
				if err != nil {
					return err
				}
				return backend.ErrIdentityMismatch
			}
		}
		return b.retention.syncOutput(b.directory)
	}
	if !sameOutput(r.reference, ref) || r.closure != ref.ClosureID || !r.closed || ref.PayloadReleased && !r.released {
		return backend.ErrIdentityMismatch
	}
	for _, suffix := range []string{".truncated", ".log"} {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := filepath.Join(b.directory, ref.AttemptID+suffix)
		info, err := os.Lstat(path)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err == nil {
			expected := r.log
			if suffix == ".truncated" {
				expected = r.marker
			}
			if expected == nil || !info.Mode().IsRegular() || !os.SameFile(expected, info) {
				return backend.ErrIdentityMismatch
			}
			if err := b.retention.removeFile(path); err != nil {
				return err
			}
		}
		if suffix == ".truncated" {
			// Establish durable marker absence even after an earlier failed sync/retry.
			// Only then can removal of the log leave a restart-valid durable prefix.
			if err := b.retention.syncOutput(b.directory); err != nil {
				return err
			}
		}
	}

	if err := b.retention.syncOutput(b.directory); err != nil {
		return err
	}
	for _, suffix := range []string{".log", ".truncated"} {
		if _, err := os.Lstat(filepath.Join(b.directory, ref.AttemptID+suffix)); !errors.Is(err, fs.ErrNotExist) {
			if err != nil {
				return err
			}
			return errors.New("output deletion remains pending")
		}
	}
	r.removed = true
	if !r.historical {
		delete(b.retention.outputs, ref.AttemptID)
	}
	return nil
}

func (b *Backend) removeExecution(ctx context.Context, ref backend.Reference) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ref.Task.Retry == nil || ref.Task.Retry.MaxAttempts <= 1 || ref.Binding != b.retention.binding || !validID.MatchString(ref.ID) || !validClosure(ref.Instance) {
		return backend.ErrIdentityMismatch
	}
	data, err := backend.TaskBytes(ref.Task)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(data)
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.jobs[ref.ID]
	if e == nil {
		// Accepted stopped evidence is the trusted dispatcher's precondition. This
		// acknowledges only absence of an owned map entry, never process termination.
		return nil
	}
	r := e.retained
	if r == nil || !r.hasTask || r.taskHash != hash || ref.Instance != e.status.Instance {
		return backend.ErrIdentityMismatch
	}
	select {
	case <-e.done:
	default:
		return errors.New("local execution cleanup pending")
	}
	if !r.released || e.status.Termination != backend.StoppedWithExitCode || e.status.ExitCode == nil {
		return errors.New("local stopped execution receipt unavailable")
	}
	delete(b.jobs, ref.ID)
	return nil
}

func syncOutputDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

// outputClosure binds an opaque lifetime receipt to exact domain identity. The
// key is never persisted or exported; it avoids accumulating live tombstones.
// Restart uses the independently validated bounded historical input instead.
func (b *Backend) outputClosure(ref backend.OutputReference) string {
	mac := hmac.New(sha256.New, b.retention.key[:])
	fmt.Fprintf(mac, "%s\x00%s\x00%s\x00%s\x001", ref.Owner, ref.JobID, ref.AttemptID, ref.Binding)
	return "local-" + hex.EncodeToString(mac.Sum(nil))
}

func (b *Backend) outputDomains() uint64 {
	var n uint64
	for _, r := range b.retention.outputs {
		if !r.removed {
			n++
		}
	}
	return n
}

func compactOutputReference(ref backend.OutputReference) backend.OutputReference {
	ref.Owner = strings.Clone(ref.Owner)
	ref.JobID = strings.Clone(ref.JobID)
	ref.AttemptID = strings.Clone(ref.AttemptID)
	ref.Binding = strings.Clone(ref.Binding)
	ref.ClosureID = strings.Clone(ref.ClosureID)
	return ref
}

func (b *Backend) checkOutputRoot() error {
	info, err := os.Lstat(b.directory)
	if err != nil {
		return err
	}
	if !info.IsDir() || !os.SameFile(info, b.retention.root) {
		return errors.New("output root identity changed")
	}
	unit, volume, err := outputFilesystem(b.directory)
	if err != nil {
		return err
	}
	if unit != b.retention.config.OutputUnit || volume != b.retention.config.Volume {
		return errors.New("output volume requires stopped-state revalidation")
	}
	return nil
}
