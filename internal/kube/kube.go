package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"sync"

	"github.com/tmc/skygo/internal/backend"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	ownerLabel     = "skygo.tmc.dev/owner"
	idLabel        = "skygo.tmc.dev/id"
	taskAnnotation = "skygo.tmc.dev/task-sha256"
)

// Backend executes native Kubernetes Jobs in a single namespace.
// Construct one with New or FromConfig; the zero value is not usable.
type Backend struct {
	client           kubernetes.Interface
	namespace, owner string
	mu               sync.Mutex
	canceled         map[string]bool
}

// New binds a client to an explicit namespace and stable operator owner label.
func New(client kubernetes.Interface, namespace, owner string) (*Backend, error) {
	if client == nil {
		return nil, errors.New("kubernetes client is required")
	}
	if namespace == "" || len(validation.IsDNS1123Label(namespace)) != 0 {
		return nil, errors.New("invalid kubernetes namespace")
	}
	if owner == "" || len(validation.IsValidLabelValue(owner)) != 0 {
		return nil, errors.New("invalid kubernetes owner")
	}
	return &Backend{client: client, namespace: namespace, owner: owner, canceled: make(map[string]bool)}, nil
}

// FromConfig uses in-cluster credentials when both kubeconfig and context are
// empty. Otherwise it loads the requested kubeconfig context.
func FromConfig(kubeconfig, contextName, namespace, owner string) (*Backend, error) {
	var config *rest.Config
	var err error
	if kubeconfig == "" && contextName == "" {
		config, err = rest.InClusterConfig()
	} else {
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		rules.ExplicitPath = kubeconfig
		config, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: contextName}).ClientConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("load kubernetes configuration: %w", err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	return New(client, namespace, owner)
}

// Check validates capability requirements without making network requests.
func (b *Backend) Check(t backend.Task) error {
	if len(t.Argv) == 0 || t.Argv[0] == "" {
		return errors.New("argv is required")
	}
	if t.Image == "" {
		return errors.New("kubernetes requires an image")
	}
	if t.Directory != "" && !path.IsAbs(t.Directory) {
		return errors.New("kubernetes directory must be absolute")
	}
	if t.CPUMillis < 0 || t.MemoryBytes < 0 || t.TimeoutSeconds < 0 || t.AcceleratorCount < 0 {
		return errors.New("resource quantities must be nonnegative")
	}
	if (t.Accelerator == "") != (t.AcceleratorCount == 0) {
		return errors.New("accelerator and count must be specified together")
	}
	if t.Accelerator != "" && t.Accelerator != "nvidia" {
		return errors.New("kubernetes supports only the nvidia accelerator")
	}
	for k := range t.Env {
		if len(validation.IsEnvVarName(k)) != 0 {
			return fmt.Errorf("invalid environment name %q", k)
		}
	}
	return nil
}

func jobName(id string) (string, error) {
	v, err := hex.DecodeString(id)
	if err != nil || len(v) != 16 || hex.EncodeToString(v) != id {
		return "", errors.New("invalid job id")
	}
	return "skygo-" + id, nil
}

func (b *Backend) owned(j *batchv1.Job, id string) error {
	if j.Labels[ownerLabel] != b.owner || j.Labels[idLabel] != id {
		return fmt.Errorf("%w: kubernetes job ownership differs", backend.ErrIdentityMismatch)
	}
	return nil
}

// Start creates a Job or adopts an existing Job with the same owner and task.
func (b *Backend) Start(ctx context.Context, id string, t backend.Task) error {
	if err := b.Check(t); err != nil {
		return err
	}
	name, err := jobName(id)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(t)
	if err != nil {
		return err
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(raw))
	zero := int32(0)
	automount := false
	one := int32(1)
	resources := corev1.ResourceList{}
	if t.CPUMillis > 0 {
		resources[corev1.ResourceCPU] = *resource.NewMilliQuantity(t.CPUMillis, resource.DecimalSI)
	}
	if t.MemoryBytes > 0 {
		resources[corev1.ResourceMemory] = *resource.NewQuantity(t.MemoryBytes, resource.BinarySI)
	}
	limits := corev1.ResourceList{}
	if t.AcceleratorCount > 0 {
		q := *resource.NewQuantity(int64(t.AcceleratorCount), resource.DecimalSI)
		resources["nvidia.com/gpu"] = q
		limits["nvidia.com/gpu"] = q
	}
	keys := make([]string, 0, len(t.Env))
	for k := range t.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	env := make([]corev1.EnvVar, 0, len(keys))
	for _, k := range keys {
		env = append(env, corev1.EnvVar{Name: k, Value: t.Env[k]})
	}
	jobLabels := map[string]string{ownerLabel: b.owner, idLabel: id}
	j := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: b.namespace, Labels: jobLabels,
			Annotations: map[string]string{taskAnnotation: hash},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &zero, Parallelism: &one, Completions: &one,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: jobLabels},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: &automount,
					RestartPolicy:                corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name: "task", Image: t.Image, Command: t.Argv,
						Env: env, WorkingDir: t.Directory,
						Resources: corev1.ResourceRequirements{Requests: resources, Limits: limits},
					}},
				},
			},
		},
	}
	if t.TimeoutSeconds > 0 {
		j.Spec.ActiveDeadlineSeconds = &t.TimeoutSeconds
	}
	_, err = b.client.BatchV1().Jobs(b.namespace).Create(ctx, j, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := b.client.BatchV1().Jobs(b.namespace).Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		if err = b.owned(existing, id); err != nil {
			return err
		}
		if existing.Annotations[taskAnnotation] != hash {
			return fmt.Errorf("%w: kubernetes task differs", backend.ErrIdentityMismatch)
		}
		return nil
	}
	return err
}

func (b *Backend) get(ctx context.Context, ref backend.Reference) (*batchv1.Job, error) {
	id := ref.ID
	name, err := jobName(id)
	if err != nil {
		return nil, err
	}
	j, err := b.client.BatchV1().Jobs(b.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if err := b.owned(j, id); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(ref.Task)
	if err != nil {
		return nil, err
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(raw))
	if j.Annotations[taskAnnotation] != hash {
		return nil, fmt.Errorf("%w: kubernetes task differs", backend.ErrIdentityMismatch)
	}
	return j, nil
}

func (b *Backend) pods(ctx context.Context, j *batchv1.Job, id string) ([]corev1.Pod, error) {
	list, err := b.client.CoreV1().Pods(b.namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.Set{ownerLabel: b.owner, idLabel: id}.String()})
	if err != nil {
		return nil, err
	}
	var pods []corev1.Pod
	for _, p := range list.Items {
		for _, ref := range p.OwnerReferences {
			if ref.UID == j.UID && ref.Kind == "Job" && ref.Name == j.Name && ref.Controller != nil && *ref.Controller {
				pods = append(pods, p)
				break
			}
		}
	}
	return pods, nil
}

// Observe reports controller state; a missing Job is Unknown unless cancellation
// was requested through this Backend instance. It never recreates missing Jobs.
func (b *Backend) Observe(ctx context.Context, ref backend.Reference) (backend.Status, error) {
	id := ref.ID
	j, err := b.get(ctx, ref)
	if apierrors.IsNotFound(err) {
		b.mu.Lock()
		canceled := b.canceled[id]
		b.mu.Unlock()
		if canceled {
			return backend.Status{State: backend.Canceled, Reason: "kubernetes job deleted"}, nil
		}
		return backend.Status{State: backend.Unknown, Reason: "kubernetes job missing"}, nil
	}
	if err != nil {
		return backend.Status{}, err
	}
	for _, c := range j.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		if c.Type == batchv1.JobComplete {
			return backend.Status{State: backend.Succeeded, ExitCode: int32ptr(0)}, nil
		}
		if c.Type == batchv1.JobFailed {
			return backend.Status{State: backend.Failed, Reason: c.Reason + ": " + c.Message}, nil
		}
	}
	pods, err := b.pods(ctx, j, id)
	if err != nil {
		return backend.Status{}, err
	}
	for _, p := range pods {
		if p.Status.Phase == corev1.PodRunning {
			return backend.Status{State: backend.Running}, nil
		}
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse {
				return backend.Status{State: backend.Starting, Reason: c.Reason + ": " + c.Message}, nil
			}
		}
	}
	return backend.Status{State: backend.Starting}, nil
}
func int32ptr(v int32) *int32 { return &v }

// Cancel requests foreground deletion, using UID preconditions to avoid deleting
// a replacement Job. Observe confirms cancellation only after disappearance.
func (b *Backend) Cancel(ctx context.Context, ref backend.Reference) error {
	id := ref.ID
	j, err := b.get(ctx, ref)
	if apierrors.IsNotFound(err) {
		b.mu.Lock()
		b.canceled[id] = true
		b.mu.Unlock()
		return nil
	}
	if err != nil {
		return err
	}
	// Observed completion wins over cancellation; retain the Job and its logs.
	for _, condition := range j.Status.Conditions {
		if condition.Status == corev1.ConditionTrue && (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed) {
			return nil
		}
	}
	policy := metav1.DeletePropagationForeground
	err = b.client.BatchV1().Jobs(b.namespace).Delete(ctx, j.Name, metav1.DeleteOptions{PropagationPolicy: &policy, Preconditions: &metav1.Preconditions{UID: &j.UID}})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	b.mu.Lock()
	b.canceled[id] = true
	b.mu.Unlock()
	return nil
}

// Logs reads the bounded output prefix of the sole owned Pod. Multiple Pods are
// explicitly ambiguous, since native Job execution is not exactly once.
func (b *Backend) Logs(ctx context.Context, ref backend.Reference, offset int64, limit int) (backend.LogChunk, error) {
	id := ref.ID
	if offset < 0 || limit <= 0 || limit > backend.MaxChunkBytes {
		return backend.LogChunk{}, fmt.Errorf("invalid log offset or limit: %w", fs.ErrInvalid)
	}
	j, err := b.get(ctx, ref)
	if apierrors.IsNotFound(err) {
		return backend.LogChunk{}, fmt.Errorf("job logs are no longer retained: %w", fs.ErrNotExist)
	}
	if err != nil {
		return backend.LogChunk{}, err
	}
	pods, err := b.pods(ctx, j, id)
	if err != nil {
		return backend.LogChunk{}, err
	}
	if len(pods) > 1 {
		return backend.LogChunk{}, errors.New("logs ambiguous: multiple owned pods")
	}
	status, err := b.Observe(ctx, ref)
	if err != nil {
		return backend.LogChunk{}, err
	}
	if len(pods) == 0 {
		if status.State.Terminal() {
			return backend.LogChunk{}, fmt.Errorf("pod logs are no longer retained: %w", fs.ErrNotExist)
		}
		if offset != 0 {
			return backend.LogChunk{}, fmt.Errorf("log offset exceeds retained prefix: %w", fs.ErrInvalid)
		}
		return backend.LogChunk{}, nil
	}
	stream, err := b.client.CoreV1().Pods(b.namespace).GetLogs(pods[0].Name, &corev1.PodLogOptions{Container: "task"}).Stream(ctx)
	if err != nil {
		return backend.LogChunk{}, err
	}
	defer stream.Close()
	data, err := io.ReadAll(io.LimitReader(stream, backend.MaxLogBytes+1))
	if err != nil {
		return backend.LogChunk{}, err
	}
	truncated := len(data) > backend.MaxLogBytes
	if truncated {
		data = data[:backend.MaxLogBytes]
	}
	if offset > int64(len(data)) {
		return backend.LogChunk{}, fmt.Errorf("log offset exceeds retained prefix: %w", fs.ErrInvalid)
	}
	end := offset + int64(limit)
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return backend.LogChunk{Data: data[offset:end], NextOffset: end, Complete: (status.State.Terminal() || truncated) && end == int64(len(data)), Truncated: truncated}, nil
}
