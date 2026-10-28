package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

func (b *Backend) materializeWorkspace(w *workspaceExecution) error {
	b.beginWorkspacePhysical(w)
	defer b.endWorkspacePhysical(w)
	config := b.workspaces.config
	if err := b.workspaceOutputAdmission(w); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(b.directory, w.binding.AttemptID+".log"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	out := &output{file: file, marker: filepath.Join(b.directory, w.binding.AttemptID+".truncated")}
	info, statErr := file.Stat()
	b.mu.Lock()
	w.parent.output = out
	if b.retention != nil && statErr == nil {
		out.retained = b.retention.outputs[w.binding.AttemptID]
		out.retained.log = info
		b.attachRetention(w.parent, w.binding.AttemptID)
	}
	// Running means the owned workspace has its real output domain, not that
	// the main argv started. Failed Stat or attachment leaves it Starting.
	if statErr == nil && info.Mode().IsRegular() && w.parent.status.State == backend.Starting &&
		(b.retention == nil || out.retained != nil && w.parent.retained == out.retained && out.retained.log == info) {
		w.parent.status.State = backend.Running
		w.observation.Status.State = backend.Running
	}
	b.mu.Unlock()
	if statErr != nil {
		return statErr
	}
	info, err = config.Roots.Lstat(".")
	if err != nil {
		return err
	}
	id, err := workspaceFileID(info)
	if err != nil {
		return err
	}
	if id != b.workspaces.root {
		return backend.ErrIdentityMismatch
	}
	w.rootName = "workspace-" + w.binding.AttemptID
	w.rootAttempted = true
	if err := config.Roots.Mkdir(w.rootName, 0700); err != nil {
		return err
	}
	w.created = true
	before, err := config.Roots.Lstat(w.rootName)
	if err != nil {
		w.rootErr = err
		return err
	}
	w.rootInfo = before
	w.root, err = config.Roots.OpenRoot(w.rootName)
	if err != nil {
		w.rootErr = err
		return err
	}
	w.directory, err = openWorkspaceDirectory(w.root)
	if err != nil {
		w.rootErr = err
		return err
	}
	if err = w.checkWorkspaceRoot(); err != nil {
		w.rootErr = err
		return err
	}
	reader, err := config.Files.OpenObject(w.binding.Input, w.binding.InputFile)
	if err != nil {
		if errors.Is(err, artifact.ErrFileCloseUncertain) {
			w.rootErr = err
		}
		return err
	}
	digest := sha256.New()
	err = artifact.Extract(w.root, io.TeeReader(io.NewSectionReader(reader, 0, w.binding.Input.Size), digest), w.binding.Input.Size)
	closeErr := reader.Close()
	if closeErr != nil {
		w.rootErr = closeErr
	}
	err = errors.Join(err, closeErr)
	if errors.Is(err, artifact.ErrFileCloseUncertain) {
		w.rootErr = err
	}
	if err == nil && hex.EncodeToString(digest.Sum(nil)) != w.binding.Input.SHA256 {
		err = errors.New("materialized input digest mismatch")
	}
	if rootErr := w.checkWorkspaceRoot(); rootErr != nil {
		w.rootErr = errors.Join(w.rootErr, rootErr)
		err = errors.Join(err, rootErr)
	}
	return err
}

func (w *workspaceExecution) checkWorkspaceRoot() error {
	if w.root == nil || w.directory == nil || w.rootInfo == nil {
		return errors.New("workspace root is not owned")
	}
	info, err := w.root.Lstat(".")
	if err != nil {
		return err
	}
	opened, err := w.directory.Stat()
	if err != nil {
		return err
	}
	if !info.IsDir() || !os.SameFile(w.rootInfo, info) || !os.SameFile(info, opened) || info.Mode().Perm() != 0700 {
		return backend.ErrIdentityMismatch
	}
	_, err = workspaceFileID(info)
	return err
}

func (b *Backend) collectWorkspace(w *workspaceExecution) (receipt artifact.SealReceipt, err error) {
	collected := false
	defer func() {
		releaseCollection(w, err)
		b.endWorkspacePhysical(w)
		if errors.Is(err, artifact.ErrFileCloseUncertain) {
			b.workspaces.config.PhysicalChanged()
		}
	}()
	for {
		wake := b.publicationAttempt(w)
		stopped, _, _ := b.workspaceStopped(w)
		if w.collectionRelease == nil {
			if stopped && w.candidate == nil {
				return artifact.SealReceipt{}, nil
			}
			release, ok := b.workspaces.config.TryCollectionIO()
			if !ok || release == nil {
				phase := "waiting-collection"
				if collected {
					phase = "waiting-publication"
				}
				if stopped {
					phase = "settling-cancel"
				}
				b.waitPublication(w, wake, phase)
				continue
			}
			w.collectionRelease = onceWorkspaceRelease(release)
		}
		if stopped {
			if w.candidate != nil {
				err = w.candidate.Close()
				w.candidateFile = w.candidate.Description()
				if !errors.Is(err, artifact.ErrFileCloseUncertain) {
					w.candidate = nil
				}
			}
			return artifact.SealReceipt{}, err
		}
		if !collected {
			if stop, _, _ := b.workspaceStopped(w); stop {
				continue
			}
			if err = b.workspaces.config.CheckGrowth(); err != nil {
				releaseCollection(w, nil)
				b.waitPublication(w, wake, "waiting-collection")
				continue
			}
			if stop, _, _ := b.workspaceStopped(w); stop {
				continue
			}
			b.beginWorkspacePhysical(w)
			if err = w.checkWorkspaceRoot(); err != nil {
				w.rootErr = err
				err = errors.Join(errWorkspaceCollectionHeld, err)
				b.endWorkspacePhysical(w)
				return artifact.SealReceipt{}, err
			}
			w.candidateAttempted = true
			w.candidate, err = b.workspaces.config.Files.CreateCandidate(w.binding.ResultID)
			// Frozen Files CreateCandidate returns Busy only BEFORE part OpenFile.
			// No general nil-candidate error establishes no-create or absence.
			if w.candidate == nil && errors.Is(err, artifact.ErrFilesBusy) && !errors.Is(err, artifact.ErrFileCloseUncertain) {
				w.candidateAttempted = false
				b.endWorkspacePhysical(w)
				releaseCollection(w, nil)
				b.waitPublication(w, wake, "waiting-collection")
				continue
			}
			if err == nil {
				deadline := time.Now().Add(10 * time.Second)
				err = artifact.Collect(w.root, w.binding.Task.Outputs, w.candidate)
				if rootErr := w.checkWorkspaceRoot(); rootErr != nil {
					w.rootErr = rootErr
					err = errors.Join(errWorkspaceCollectionHeld, err, rootErr)
				}
				if !time.Now().Before(deadline) {
					err = errors.Join(err, errors.New("workspace collection timed out"))
				}
			}
			if err != nil {
				if w.candidate != nil {
					err = errors.Join(err, w.candidate.Close())
					w.candidateFile = w.candidate.Description()
					if !errors.Is(err, artifact.ErrFileCloseUncertain) {
						w.candidate = nil
					}
				}
				if w.candidateFile.File.Device == 0 || w.candidateFile.File.Inode == 0 {
					err = errors.Join(errWorkspaceCollectionHeld, err)
					w.collectionErr = err
				}
				b.endWorkspacePhysical(w)
				return artifact.SealReceipt{}, err
			}
			collected = true
			b.endWorkspacePhysical(w)
			// Intent can arrive during Collect. Do not publish after it wins.
			if stop, _, _ := b.workspaceStopped(w); stop {
				continue
			}
		}
		var guardErr error
		guard := func() error { guardErr = b.workspaces.config.CheckGrowth(); return guardErr }
		deadline := time.Now().Add(10 * time.Second)
		receipt, err = w.candidate.Seal(guard)
		if errors.Is(err, artifact.ErrFileCloseUncertain) {
			w.collectionErr = err
			return artifact.SealReceipt{}, err
		}
		if errors.Is(err, artifact.ErrFilesBusy) || guardErr != nil && errors.Is(err, guardErr) {
			// Only the actually invoked late guard proves original writer Close.
			// An early f.ready Busy keeps the original writer and its same permit.
			if guardErr != nil {
				releaseCollection(w, nil)
			}
			b.endWorkspacePhysical(w)
			b.waitPublication(w, wake, "waiting-publication")
			continue
		}
		if err == nil && !time.Now().Before(deadline) {
			err = errors.New("workspace publication timed out")
		}
		w.candidateFile = w.candidate.Description()
		if err != nil {
			err = errors.Join(err, w.candidate.Close())
		}
		if !errors.Is(err, artifact.ErrFileCloseUncertain) {
			w.candidate = nil
		}
		return receipt, err
	}
}

// workspaceOutputAdmission samples a private immutable reservation snapshot
// outside b.mu. The final hash/unchanged reservation check is under the lock;
// concurrent inventory changes fail closed, never launch on an uncertain sample.
func (b *Backend) workspaceOutputAdmission(w *workspaceExecution) error {
	b.mu.Lock()
	if b.retention == nil {
		b.mu.Unlock()
		return nil
	}
	r := b.retention.outputs[w.binding.AttemptID]
	if r == nil || r.closed || r.removed || r.hasTask {
		b.mu.Unlock()
		return errors.New("local output reservation is not bound for workspace")
	}
	state := *b.retention
	state.outputs = make(map[string]*retainedOutput, len(b.retention.outputs))
	for id, value := range b.retention.outputs {
		copy := *value
		state.outputs[id] = &copy
	}
	b.mu.Unlock()
	sampler := &Backend{directory: b.directory, retention: &state}
	usage, err := sampler.outputUsage(context.Background())
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.retention.outputs[w.binding.AttemptID] != r || r.closed || r.removed || r.hasTask {
		return backend.ErrIdentityMismatch
	}
	policy := b.retention.config
	domains := b.outputDomains()
	if domains > policy.OutputBytes/b.retention.reserve || domains > policy.OutputFiles/2 || usage.AllocatedBytes > domains*b.retention.reserve || usage.AllocatedBytes > policy.OutputBytes || usage.Files > policy.OutputFiles {
		return errors.New("local output capacity reached")
	}
	r.taskHash, r.hasTask = w.taskHash, true
	return nil
}
