package kube

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
)

// workerPodMode selects how the ranks of a worker group find each other.
type workerPodMode uint8

const (
	// workerIndependent ranks share no rendezvous.
	workerIndependent workerPodMode = 1
	// workerTCP ranks rendezvous at rank 0 through the attempt's Service.
	workerTCP workerPodMode = 2
)

// Worker group bounds. A single worker uses the ordinary Job or retry lanes.
const (
	minWorkers = 2
	maxWorkers = 16
)

// maxWorkerPullSecrets bounds the image pull secret snapshot of a worker plan.
const maxWorkerPullSecrets = 32

// workerMasterPort is the rank-0 TCP rendezvous port of workerTCP groups.
const workerMasterPort = 29500

// The environment injected into every worker container. A task may not set
// any of these names itself.
const (
	workerEnvRank       = "RANK"
	workerEnvWorldSize  = "WORLD_SIZE"
	workerEnvJobID      = "SKYGO_JOB_ID"
	workerEnvAttemptID  = "SKYGO_ATTEMPT_ID"
	workerEnvMasterAddr = "MASTER_ADDR"
	workerEnvMasterPort = "MASTER_PORT"
)

// workerPodPlan is one rank of a worker group. Task is the accepted task,
// whose TaskBytes are not changed; JobID and AttemptID use the job ID grammar.
type workerPodPlan struct {
	JobID, AttemptID string
	Rank, Count      uint32
	Task             backend.Task
	Mode             workerPodMode
	Registry         workerRegistry
}

// workerRegistry is the operator's trusted snapshot of the namespace's
// default service account: its UID and its image pull secret names, in order.
// The worker Pod sets the names explicitly, so ServiceAccount admission copies
// nothing into it. The snapshot is provenance of the worker profile, not a
// task field; it is never read from an observed Pod, and the builder copies
// it rather than retaining it.
type workerRegistry struct {
	ServiceAccountUID types.UID
	PullSecrets       []string
}

// check validates r: a printable service account UID of at most 128 bytes and
// at most maxWorkerPullSecrets distinct Secret names.
func (r workerRegistry) check() error {
	if !validInstance(string(r.ServiceAccountUID)) {
		return errors.New("invalid worker service account uid")
	}
	if len(r.PullSecrets) > maxWorkerPullSecrets {
		return errors.New("too many worker image pull secrets")
	}
	for i, name := range r.PullSecrets {
		if len(validation.IsDNS1123Subdomain(name)) != 0 || slices.Contains(r.PullSecrets[:i], name) {
			return errors.New("invalid worker image pull secret")
		}
	}
	return nil
}

// workerPod returns the exact direct Pod of plan p. It contacts no API and
// creates no Service; workerTCP Pods only name the attempt's Service in
// MASTER_ADDR. Tasks with a workdir, setup, outputs, data mounts or retry
// policy are refused, as is an invalid registry snapshot. This initial worker
// leaf accepts only CPU tasks and refuses DRA and execution-receipt backends.
//
// The Pod uses the fixed worker profile: the task's ordinary podSpec container
// with injected environment and IfNotPresent image pulls, no service links,
// PreemptLowerPriority at priority 0 with no priority class, exactly the two
// standard 300s NoExecute tolerations, ClusterFirst DNS, the default
// scheduler, a 30s grace period, restartPolicy Never, no service account token
// and no shared process namespace. Rank 0 of a workerTCP group alone has a TCP
// readiness probe on workerMasterPort; the application must tolerate its empty
// connections. There is no liveness probe and no owner reference. Every worker
// Pod is created with the single workerSchedulingGate and with exactly the
// snapshot's image pull secrets, in order.
func (b *Backend) workerPod(p workerPodPlan) (*corev1.Pod, error) {
	if b.binding == "" {
		return nil, fmt.Errorf("%w: kubernetes target is unbound", backend.ErrIdentityMismatch)
	}
	if b.namespace == "" || b.owner == "" {
		return nil, errors.New("kubernetes target is incomplete")
	}
	// Workers use a separate, CPU-only profile. Never inherit device claims
	// or execution receipts from the ordinary target's Pod builder.
	if b.dra != nil || b.gate != nil || b.receipts != nil {
		return nil, errors.New("kubernetes workers do not support dra or execution receipts")
	}
	for _, accelerator := range b.policy.Accelerators {
		if accelerator.DRA != nil {
			return nil, errors.New("kubernetes workers do not support dra policy")
		}
	}
	if p.Task.Accelerator != "" || p.Task.AcceleratorCount != 0 {
		return nil, errors.New("kubernetes workers do not support accelerators")
	}
	if p.Count < minWorkers || p.Count > maxWorkers {
		return nil, errors.New("worker count must be between 2 and 16")
	}
	if p.Rank >= p.Count {
		return nil, errors.New("worker rank must be less than worker count")
	}
	if p.Mode != workerIndependent && p.Mode != workerTCP {
		return nil, errors.New("invalid worker mode")
	}
	if _, err := jobName(p.JobID); err != nil {
		return nil, err
	}
	if _, err := jobName(p.AttemptID); err != nil {
		return nil, errors.New("invalid attempt id")
	}
	if err := p.Registry.check(); err != nil {
		return nil, err
	}
	if dataTask(p.Task) {
		return nil, errors.New("kubernetes workers do not support data tasks")
	}
	if p.Task.Retry != nil {
		return nil, errors.New("kubernetes workers do not support retry")
	}
	for _, k := range []string{workerEnvRank, workerEnvWorldSize, workerEnvJobID,
		workerEnvAttemptID, workerEnvMasterAddr, workerEnvMasterPort} {
		if _, ok := p.Task.Env[k]; ok {
			return nil, fmt.Errorf("environment %s is reserved for workers", k)
		}
	}
	if err := b.Check(p.Task); err != nil {
		return nil, err
	}
	raw, err := backend.TaskBytes(p.Task)
	if err != nil {
		return nil, err
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(raw))
	group := workerGroupDigest(raw, p.Count, p.Mode)

	// podSpec aliases Task.Argv as the container command; build from a copy.
	spec := b.podSpec(backend.CloneTask(p.Task))
	if p.Task.TimeoutSeconds > 0 {
		deadline := p.Task.TimeoutSeconds
		spec.ActiveDeadlineSeconds = &deadline
	}
	c := &spec.Containers[0]
	c.ImagePullPolicy = corev1.PullIfNotPresent
	inject := []corev1.EnvVar{
		{Name: workerEnvRank, Value: strconv.FormatUint(uint64(p.Rank), 10)},
		{Name: workerEnvWorldSize, Value: strconv.FormatUint(uint64(p.Count), 10)},
		{Name: workerEnvJobID, Value: p.JobID},
		{Name: workerEnvAttemptID, Value: p.AttemptID},
	}
	if p.Mode == workerTCP {
		inject = append(inject,
			corev1.EnvVar{Name: workerEnvMasterAddr, Value: workerServiceDNS(p.AttemptID, b.namespace)},
			corev1.EnvVar{Name: workerEnvMasterPort, Value: strconv.Itoa(workerMasterPort)})
		if p.Rank == 0 {
			c.ReadinessProbe = &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromInt32(workerMasterPort)}},
				// The API server defaults; set explicitly for exact comparison.
				TimeoutSeconds: 1, PeriodSeconds: 10, SuccessThreshold: 1, FailureThreshold: 3,
			}
		}
	}
	c.Env = append(c.Env, inject...)
	slices.SortFunc(c.Env, func(x, y corev1.EnvVar) int { return strings.Compare(x.Name, y.Name) })

	no := false
	priority := int32(0)
	preempt := corev1.PreemptLowerPriority
	grace := int64(30)
	tolerate := int64(300)
	spec.EnableServiceLinks = &no
	spec.ShareProcessNamespace = &no
	spec.Priority = &priority
	spec.PriorityClassName = ""
	spec.PreemptionPolicy = &preempt
	spec.DNSPolicy = corev1.DNSClusterFirst
	spec.SchedulerName = "default-scheduler"
	spec.TerminationGracePeriodSeconds = &grace
	spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: workerSchedulingGate}}
	spec.ImagePullSecrets = nil
	for _, name := range p.Registry.PullSecrets {
		spec.ImagePullSecrets = append(spec.ImagePullSecrets, corev1.LocalObjectReference{Name: name})
	}
	spec.Tolerations = []corev1.Toleration{
		{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists,
			Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &tolerate},
		{Key: corev1.TaintNodeUnreachable, Operator: corev1.TolerationOpExists,
			Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &tolerate},
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: workerPodName(p.AttemptID, p.Rank), Namespace: b.namespace,
			Labels: map[string]string{
				ownerLabel:         b.owner,
				workerJobLabel:     p.JobID,
				workerAttemptLabel: p.AttemptID,
				workerRankLabel:    strconv.FormatUint(uint64(p.Rank), 10),
				workerWorldLabel:   strconv.FormatUint(uint64(p.Count), 10),
			},
			Annotations: map[string]string{
				taskAnnotation:          hash,
				bindingAnnotation:       b.binding,
				workerGroupAnnotation:   group,
				workerProfileAnnotation: workerProfileVersion,
			},
			Finalizers: []string{workerFinalizer},
		},
		Spec: spec,
	}, nil
}
