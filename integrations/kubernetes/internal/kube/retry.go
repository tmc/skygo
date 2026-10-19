package kube

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// The durable retry reservation permits at most 16 MiB / 8192 attempts.
// Failed and ambiguous creates retain their reservation for this server lifetime.
const maxRetryReceipts = 2048

type retryReceipt struct {
	hash   string
	uid    string
	cancel bool
}

func checkRetry(t backend.Task) error {
	if t.Retry == nil {
		return nil
	}
	p, err := backend.NormalizeRetry(t.Retry)
	if err != nil {
		return err
	}
	if p == nil || p.MaxAttempts != t.Retry.MaxAttempts || p.DelaySeconds != t.Retry.DelaySeconds || !slices.Equal(p.ExitCodes, t.Retry.ExitCodes) {
		return errors.New("retry policy must be normalized")
	}
	if t.TimeoutSeconds > 7*24*3600 {
		return errors.New("retry timeout exceeds seven days")
	}
	return nil
}

func validInstance(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range []byte(s) {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}

func taskHash(t backend.Task) (string, error) {
	raw, err := backend.TaskBytes(t)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(raw)), nil
}

func (b *Backend) startRetry(ctx context.Context, id string, t backend.Task) error {
	if b.binding == "" {
		return fmt.Errorf("%w: kubernetes target is unbound", backend.ErrIdentityMismatch)
	}
	if err := b.Check(t); err != nil {
		return err
	}
	name, err := jobName(id)
	if err != nil {
		return err
	}
	hash, err := taskHash(t)
	if err != nil {
		return err
	}
	client, ok := b.client.CoreV1().RESTClient().(*rest.RESTClient)
	if !ok || client == nil {
		return errors.New("kubernetes retry requires a REST client")
	}
	if err := b.claimRetry(id, false); err != nil {
		return err
	}
	defer b.releaseRetry(id)
	b.mu.Lock()
	if b.retries == nil {
		b.retries = make(map[string]*retryReceipt)
	}
	if _, ok := b.retries[id]; ok {
		b.mu.Unlock()
		return errors.New("retry attempt already started")
	}
	if len(b.retries) >= maxRetryReceipts {
		b.mu.Unlock()
		return errors.New("kubernetes retry receipt limit reached")
	}
	b.retries[id] = &retryReceipt{hash: hash}
	b.mu.Unlock()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: b.namespace,
		Labels:      map[string]string{ownerLabel: b.owner, idLabel: id},
		Annotations: map[string]string{taskAnnotation: hash, bindingAnnotation: b.binding},
	}, Spec: b.podSpec(t)}
	if t.TimeoutSeconds > 0 {
		pod.Spec.ActiveDeadlineSeconds = &t.TimeoutSeconds
	}
	var created bool
	result := &corev1.Pod{}
	err = client.Post().Namespace(b.namespace).Resource("pods").
		VersionedParams(&metav1.CreateOptions{}, scheme.ParameterCodec).Body(pod).
		MaxRetries(0).Do(ctx).WasCreated(&created).Into(result)
	if err != nil {
		return err
	}
	if !created {
		return errors.New("kubernetes retry create did not return created")
	}
	uid := string(result.UID)
	if !validInstance(uid) {
		return fmt.Errorf("%w: invalid pod instance", backend.ErrIdentityMismatch)
	}
	if err := b.ownedRetry(result, id, hash, uid); err != nil {
		return err
	}
	b.mu.Lock()
	b.retries[id].uid = uid
	b.mu.Unlock()
	return nil
}

// retryIdentity authorizes a native read using the first receipt or a durable
// pin. An ambiguous create reservation never acquires an identity by observation.
func (b *Backend) retryIdentity(ref backend.Reference) (string, string, error) {
	if b.binding == "" || ref.Binding != b.binding {
		return "", "", fmt.Errorf("%w: kubernetes target binding differs", backend.ErrIdentityMismatch)
	}
	if _, err := jobName(ref.ID); err != nil {
		return "", "", err
	}
	if err := b.Check(ref.Task); err != nil {
		return "", "", err
	}
	if ref.Instance != "" && !validInstance(ref.Instance) {
		return "", "", fmt.Errorf("%w: invalid pod instance", backend.ErrIdentityMismatch)
	}
	hash, err := taskHash(ref.Task)
	if err != nil {
		return "", "", err
	}
	uid := ref.Instance
	b.mu.Lock()
	if r := b.retries[ref.ID]; r != nil {
		if r.uid == "" || r.hash != hash || (uid != "" && uid != r.uid) {
			b.mu.Unlock()
			return "", "", fmt.Errorf("%w: retry receipt differs or is unavailable", backend.ErrIdentityMismatch)
		}
		uid = r.uid
	}
	b.mu.Unlock()
	if uid == "" {
		return "", "", fmt.Errorf("%w: retry pod instance is unavailable", backend.ErrIdentityMismatch)
	}
	return uid, hash, nil
}

func (b *Backend) ownedRetry(p *corev1.Pod, id, hash, uid string) error {
	name, err := jobName(id)
	if err != nil {
		return err
	}
	if p.Name != name || p.Namespace != b.namespace || string(p.UID) != uid ||
		p.Labels[ownerLabel] != b.owner || p.Labels[idLabel] != id ||
		p.Annotations[taskAnnotation] != hash || p.Annotations[bindingAnnotation] != b.binding {
		return fmt.Errorf("%w: kubernetes retry pod differs", backend.ErrIdentityMismatch)
	}
	return nil
}

func (b *Backend) getRetry(ctx context.Context, ref backend.Reference) (*corev1.Pod, string, error) {
	uid, hash, err := b.retryIdentity(ref)
	if err != nil {
		return nil, "", err
	}
	name, _ := jobName(ref.ID)
	p, err := b.client.CoreV1().Pods(b.namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		err = b.ownedRetry(p, ref.ID, hash, uid)
	}
	return p, uid, err
}

// stoppedRetry accepts a current runtime receipt, not a phase or a synthesized
// missing-container status. CRI's reported exit code has no signal provenance.
func stoppedRetry(p *corev1.Pod, t backend.Task) bool {
	if p.Status.Phase != corev1.PodFailed || p.Status.Reason != "" || p.DeletionTimestamp != nil ||
		len(p.OwnerReferences) != 0 || p.Spec.RestartPolicy != corev1.RestartPolicyNever ||
		len(p.Spec.Containers) != 1 || p.Spec.Containers[0].Name != "task" ||
		len(p.Spec.InitContainers) != 0 || len(p.Spec.EphemeralContainers) != 0 ||
		len(p.Status.ContainerStatuses) != 1 || len(p.Status.InitContainerStatuses) != 0 || len(p.Status.EphemeralContainerStatuses) != 0 {
		return false
	}
	c := p.Status.ContainerStatuses[0]
	last := c.LastTerminationState
	if c.Name != "task" || c.RestartCount != 0 || c.State.Terminated == nil ||
		c.State.Running != nil || c.State.Waiting != nil || last.Terminated != nil || last.Running != nil || last.Waiting != nil {
		return false
	}
	exit := c.State.Terminated
	runtime, id, ok := strings.Cut(exit.ContainerID, "://")
	if !ok || runtime == "" || id == "" || c.ContainerID != exit.ContainerID ||
		exit.StartedAt.UnixNano() <= 0 || exit.FinishedAt.UnixNano() <= 0 || exit.FinishedAt.Before(&exit.StartedAt) ||
		exit.ExitCode <= 0 || !slices.Contains(t.Retry.ExitCodes, exit.ExitCode) {
		return false
	}
	if t.TimeoutSeconds > 0 {
		if p.Spec.ActiveDeadlineSeconds == nil || *p.Spec.ActiveDeadlineSeconds != t.TimeoutSeconds ||
			p.Status.StartTime == nil || p.Status.StartTime.UnixNano() <= 0 ||
			!exit.FinishedAt.Before(&metav1.Time{Time: p.Status.StartTime.Add(time.Duration(t.TimeoutSeconds) * time.Second)}) {
			return false
		}
	} else if p.Spec.ActiveDeadlineSeconds != nil {
		return false
	}
	return true
}

func sameTermination(a, b *corev1.Pod) bool {
	x := a.Status.ContainerStatuses[0].State.Terminated
	y := b.Status.ContainerStatuses[0].State.Terminated
	return x.ContainerID == y.ContainerID && x.ExitCode == y.ExitCode && x.StartedAt.Equal(&y.StartedAt) && x.FinishedAt.Equal(&y.FinishedAt)
}

func (b *Backend) observeRetry(ctx context.Context, ref backend.Reference) (backend.Status, error) {
	if err := b.claimRetryReference(ref); err != nil {
		return backend.Status{}, err
	}
	defer b.releaseRetry(ref.ID)
	p, uid, err := b.getRetry(ctx, ref)
	if apierrors.IsNotFound(err) {
		return backend.Status{State: backend.Unknown, Instance: uid, Reason: "kubernetes retry pod missing"}, nil
	}
	if err != nil {
		return backend.Status{}, err
	}
	status := backend.Status{State: backend.Starting, Instance: uid}
	switch p.Status.Phase {
	case corev1.PodRunning:
		status.State = backend.Running
	case corev1.PodSucceeded:
		status.State, status.ExitCode = backend.Succeeded, int32ptr(0)
	case corev1.PodFailed:
		status.State, status.Reason = backend.Failed, "kubernetes retry pod failed"
		if len(p.Status.ContainerStatuses) == 1 && p.Status.ContainerStatuses[0].Name == "task" {
			if exit := p.Status.ContainerStatuses[0].State.Terminated; exit != nil {
				status.ExitCode = int32ptr(exit.ExitCode)
			}
		}
		if !stoppedRetry(p, ref.Task) {
			break
		}
		again, _, err := b.getRetry(ctx, ref)
		if err != nil {
			return backend.Status{}, err
		}
		if !stoppedRetry(again, ref.Task) || !sameTermination(p, again) {
			return backend.Status{State: backend.Unknown, Instance: uid, Reason: "kubernetes retry termination changed"}, nil
		}
		b.mu.Lock()
		canceled := b.retries[ref.ID] != nil && b.retries[ref.ID].cancel
		b.mu.Unlock()
		if canceled {
			status.State, status.Reason = backend.Canceled, "canceled by operator"
		} else {
			status.Termination = backend.StoppedWithExitCode
		}
	case corev1.PodUnknown:
		status.State, status.Reason = backend.Unknown, "kubernetes retry pod state unknown"
	}
	return status, nil
}

func (b *Backend) cancelRetry(ctx context.Context, ref backend.Reference) error {
	if err := b.claimRetryReference(ref); err != nil {
		return err
	}
	defer b.releaseRetry(ref.ID)
	p, uid, err := b.getRetry(ctx, ref)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	// An already observed terminal outcome wins over a later cancellation.
	if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
		return nil
	}
	hash, err := taskHash(ref.Task)
	if err != nil {
		return err
	}
	b.mu.Lock()
	if b.retries == nil {
		b.retries = make(map[string]*retryReceipt)
	}
	r := b.retries[ref.ID]
	if r == nil {
		if len(b.retries) >= maxRetryReceipts {
			b.mu.Unlock()
			return errors.New("kubernetes retry receipt limit reached")
		}
		r = &retryReceipt{uid: uid, hash: hash}
		b.retries[ref.ID] = r
	}
	if r.uid != uid || r.hash != hash {
		b.mu.Unlock()
		return fmt.Errorf("%w: retry receipt differs", backend.ErrIdentityMismatch)
	}
	r.cancel = true
	b.mu.Unlock()
	nativeUID := types.UID(uid)
	err = b.client.CoreV1().Pods(b.namespace).Delete(ctx, p.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &nativeUID}})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (b *Backend) logsRetry(ctx context.Context, ref backend.Reference, offset int64, limit int) (backend.LogChunk, error) {
	if offset < 0 || limit <= 0 || limit > backend.MaxChunkBytes {
		return backend.LogChunk{}, fmt.Errorf("invalid log offset or limit: %w", fs.ErrInvalid)
	}
	if err := b.claimRetryReference(ref); err != nil {
		return backend.LogChunk{}, err
	}
	defer b.releaseRetry(ref.ID)
	p, _, err := b.getRetry(ctx, ref)
	if apierrors.IsNotFound(err) {
		return backend.LogChunk{}, fmt.Errorf("retry pod logs are no longer retained: %w", fs.ErrNotExist)
	}
	if err != nil {
		return backend.LogChunk{}, err
	}
	stream, err := b.client.CoreV1().Pods(b.namespace).GetLogs(p.Name, &corev1.PodLogOptions{Container: "task"}).Stream(ctx)
	if err != nil {
		return backend.LogChunk{}, err
	}
	defer stream.Close()
	data, err := io.ReadAll(io.LimitReader(stream, backend.MaxLogBytes+1))
	if err != nil {
		return backend.LogChunk{}, err
	}
	// Pod logs have no UID precondition. Check again before releasing bytes;
	// trusted namespace writers remain necessary for the metadata/read boundary.
	if _, _, err := b.getRetry(ctx, ref); err != nil {
		return backend.LogChunk{}, err
	}
	truncated := len(data) > backend.MaxLogBytes
	if truncated {
		data = data[:backend.MaxLogBytes]
	}
	if offset > int64(len(data)) {
		return backend.LogChunk{}, fmt.Errorf("log offset exceeds retained prefix: %w", fs.ErrInvalid)
	}
	end := min(int64(len(data)), offset+int64(limit))
	complete := p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
	return backend.LogChunk{Data: data[offset:end], NextOffset: end, Complete: (complete || truncated) && end == int64(len(data)), Truncated: truncated}, nil
}
