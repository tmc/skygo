package kube

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Worker Pod identity. Labels hold bounded values; the 64-hex digests exceed
// the 63-byte label value bound and are annotations.
const (
	workerJobLabel        = "skygo.tmc.dev/job"
	workerAttemptLabel    = "skygo.tmc.dev/attempt"
	workerRankLabel       = "skygo.tmc.dev/rank"
	workerWorldLabel      = "skygo.tmc.dev/world"
	workerGroupAnnotation = "skygo.tmc.dev/worker-group-sha256"

	// workerProfileAnnotation records workerProfileVersion on every worker Pod.
	workerProfileAnnotation = "skygo.tmc.dev/worker-profile"

	// workerProfileVersion identifies the worker Pod builder, its injected
	// environment, the group digest encoding and the fixed Pod profile. Any
	// change to them requires a new version.
	workerProfileVersion = "skygo-worker-pod-v2"

	// workerFinalizer is the one finalizer skygo places on a worker Pod.
	// Only a separately authorized retirement phase removes it.
	workerFinalizer = "skygo.tmc.dev/worker"

	// workerSchedulingGate is the one scheduling gate on every new worker
	// Pod. The scheduler does not place the Pod while it is present. Only a
	// separate exact-UID update after durable activation removes it; this
	// leaf never does. Single-worker Pods have no gate.
	workerSchedulingGate = "skygo.tmc.dev/worker-activation"

	// workerGroupDomain separates the group digest from every other skygo hash.
	workerGroupDomain = "skygo-worker-group-v1"
)

// workerGroupDigest returns the lowercase hex SHA-256 of the domain-separated
// encoding
//
//	workerGroupDomain || 0x00 || uint32be(count) || uint8(mode) ||
//	uint64be(len(raw)) || raw
//
// where raw is the exact backend.TaskBytes of the accepted task. The digest is
// shared by all ranks of one group; it does not include job or attempt IDs.
func workerGroupDigest(raw []byte, count uint32, mode workerPodMode) string {
	h := sha256.New()
	h.Write([]byte(workerGroupDomain))
	h.Write([]byte{0})
	var n [13]byte
	binary.BigEndian.PutUint32(n[0:4], count)
	n[4] = byte(mode)
	binary.BigEndian.PutUint64(n[5:13], uint64(len(raw)))
	h.Write(n[:])
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil))
}

// workerPodName returns the name of rank's Pod in attempt. The caller has
// validated attempt with jobName.
func workerPodName(attempt string, rank uint32) string {
	return "skygo-" + attempt + "-" + strconv.FormatUint(uint64(rank), 10)
}

// workerServiceName returns the name of the attempt's rank-0 Service. This
// leaf neither creates nor owns that Service.
func workerServiceName(attempt string) string {
	return "skygo-" + attempt
}

// workerServiceDNS returns the namespace-qualified DNS name of the attempt's
// Service. It omits the cluster domain, which ClusterFirst resolution
// supplies through the Pod's search path.
func workerServiceDNS(attempt, namespace string) string {
	return workerServiceName(attempt) + "." + namespace + ".svc"
}

// workerPodMatches reports whether got is the original worker Pod of plan p,
// created with UID originalUID. originalUID must be the Create-return UID; a
// GET cannot supply it. Name, namespace, labels, skygo annotations, the
// finalizer list and the fixed spec, including its one scheduling gate, must
// be exact, with only the API defaults
// accepted by fixedPodSpecMatches. Status is not compared, and a match says
// nothing about scheduling, readiness or termination. Neither got nor p is
// modified.
func (b *Backend) workerPodMatches(got *corev1.Pod, p workerPodPlan, originalUID types.UID) error {
	mismatch := fmt.Errorf("%w: kubernetes worker pod differs", backend.ErrIdentityMismatch)
	if originalUID == "" || !validInstance(string(originalUID)) {
		return fmt.Errorf("%w: worker pod original uid is unavailable", backend.ErrIdentityMismatch)
	}
	want, err := b.workerPod(p)
	if err != nil {
		return err
	}
	if got == nil || got.UID != originalUID || !profileMetaMatches(want.ObjectMeta, got.ObjectMeta) ||
		!slices.Equal(got.Finalizers, want.Finalizers) || !fixedPodSpecMatches(&want.Spec, &got.Spec) {
		return mismatch
	}
	return nil
}
