package kube

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// maxQualificationResponse is the byte limit of each qualification response
// body, applied before decoding. It also limits every StartData response.
const maxQualificationResponse = 4 << 20

// maxQualificationNodes is the record's node bound. A pool list is requested
// with one more, so a larger pool is refused rather than truncated.
const maxQualificationNodes = 32

// qualificationReadTimeout bounds one set of qualification reads.
const qualificationReadTimeout = 30 * time.Second

// qualificationPremise holds the deployment facts that the Kubernetes API does
// not report. The server and CA digest are those of the constructor's frozen
// transport. The policy revisions are the operator's assertions from target
// configuration (DataQualification); they are trusted, never observed.
type qualificationPremise struct {
	server, caSHA256 string
	policy           string
	storage          map[string]string // PVC reference name to revision
}

// A qualificationReadError reports failed qualification reads. It retains the
// reads' frame, which keeps every original body whose Close failed or has not
// returned, and the original error, which may carry API server text and is
// therefore reached only through Unwrap.
type qualificationReadError struct {
	frame *dataFrame
	cause error
}

func (e *qualificationReadError) Error() string {
	if errors.Is(e.cause, errDataBodyHeld) {
		return errDataBodyHeld.Error()
	}
	return "kubernetes qualification read failed"
}

func (e *qualificationReadError) Unwrap() error { return e.cause }

// observedDrift reports whether err, from observeQualification, is a
// difference the reads found in the deployment rather than a failed read.
// With a held body it is a read failure, so that the frame is retained.
func observedDrift(err error) bool {
	return errors.Is(err, errQualificationCoverage) && !errors.Is(err, errDataBodyHeld)
}

// qualifyTarget installs the qualification configured for target t, or
// returns nil without any I/O if none is configured. It loads the record and
// the build receipt, validates the record against the actual build, t and the
// frozen config, then reads the deployment through client under ctx in one
// standalone frame that limits each response before decoding, and compares
// the record with those reads. Any failure refuses the target. The reads
// return only after their bodies' Close calls have returned.
func qualifyTarget(ctx context.Context, t TargetConfig, config *rest.Config, d *dataProfile, client kubernetes.Interface) (*dataQualification, error) {
	c := qualificationConfig(t.Data)
	if c == nil {
		return nil, nil
	}
	record, err := loadQualificationRecord(c.Record)
	if err != nil {
		return nil, err
	}
	actual, err := loadQualificationBuild(c.BuildReceipt)
	if err != nil {
		return nil, err
	}
	r, err := validateQualificationCoverage(record, t, config, actual)
	if err != nil {
		return nil, err
	}
	p := qualificationPremise{server: config.Host, caSHA256: fmt.Sprintf("%x", sha256.Sum256(config.CAData)),
		policy: c.PolicyRevision, storage: maps.Clone(c.StoragePolicy)}
	f := &dataFrame{limit: maxQualificationResponse}
	ctx, cancel := context.WithTimeout(context.WithValue(ctx, dataFrameKey{}, f), qualificationReadTimeout)
	defer cancel()
	e, err := observeQualification(ctx, client, t.Namespace, d, r, p)
	if !f.end() {
		err = withHeld(err)
	}
	if observedDrift(err) {
		return nil, err
	}
	if err != nil {
		return nil, &qualificationReadError{frame: f, cause: err}
	}
	if err := compareQualificationEnvironment(r, e); err != nil {
		return nil, err
	}
	id, err := qualificationRecordID(r)
	if err != nil {
		return nil, err
	}
	return &dataQualification{id: id, record: r, premise: p}, nil
}

// qualificationDrift repeats the startup reads in the frame of ctx, the
// caller's StartData operation, and compares them with the installed record.
func (b *Backend) qualificationDrift(ctx context.Context) error {
	q := b.dataQual
	if q == nil {
		return errors.New("kubernetes data profile is not qualified")
	}
	f, _ := ctx.Value(dataFrameKey{}).(*dataFrame)
	ctx, cancel := context.WithTimeout(ctx, qualificationReadTimeout)
	defer cancel()
	e, err := observeQualification(ctx, b.client, b.namespace, b.data, q.record, q.premise)
	if observedDrift(err) {
		return fmt.Errorf("kubernetes data deployment changed: %w", err)
	}
	if err != nil {
		return &qualificationReadError{frame: f, cause: err}
	}
	if err := compareQualificationEnvironment(q.record, e); err != nil {
		return fmt.Errorf("kubernetes data deployment changed: %w", err)
	}
	return nil
}

// observeQualification reads the facts compareQualificationEnvironment needs:
// the namespace UID, the server version, every node matching the record's
// pool selector, and each recorded PVC with its storage class provisioner.
// The record selects only the pool and the PVC references; apart from the
// premise p, every returned value is read, none copied from the record.
// It makes at most 3+2×16 requests, each without client retries, in the frame
// of ctx, which must limit response bodies. No Secret is read. A deployment
// that cannot match any record, such as a paginated or oversize pool or a PVC
// without a storage class, returns errQualificationCoverage.
func observeQualification(ctx context.Context, c kubernetes.Interface, namespace string, d *dataProfile, r *qualificationRecord, p qualificationPremise) (qualificationDeployment, error) {
	var e qualificationDeployment
	f, _ := ctx.Value(dataFrameKey{}).(*dataFrame)
	if f == nil || f.bodyLimit() <= 0 {
		return e, errors.New("kubernetes qualification reads require a limited frame")
	}
	core, _ := c.CoreV1().RESTClient().(*rest.RESTClient)
	storage, _ := c.StorageV1().RESTClient().(*rest.RESTClient)
	var discovery *rest.RESTClient
	if dc := c.Discovery(); dc != nil {
		discovery, _ = dc.RESTClient().(*rest.RESTClient)
	}
	if core == nil || storage == nil || discovery == nil {
		return e, errors.New("kubernetes qualification requires REST clients")
	}
	// A body whose Close failed may have delivered a result; it is not used.
	done := func(err error) error {
		if !f.usable() {
			return withHeld(err)
		}
		return err
	}
	var ns corev1.Namespace
	if err := done(core.Get().Resource("namespaces").Name(namespace).MaxRetries(0).Do(ctx).Into(&ns)); err != nil {
		return e, err
	}
	if ns.Name != namespace {
		return e, errQualificationCoverage
	}
	raw, err := discovery.Get().AbsPath("/version").MaxRetries(0).Do(ctx).Raw()
	if err := done(err); err != nil {
		return e, err
	}
	var v version.Info
	if err := json.Unmarshal(raw, &v); err != nil {
		return e, errQualificationCoverage
	}
	pool := r.Deployment.Pool
	var nodes corev1.NodeList
	opts := &metav1.ListOptions{LabelSelector: labels.SelectorFromSet(labels.Set{pool.Key: pool.Value}).String(), Limit: maxQualificationNodes + 1}
	if err := done(core.Get().Resource("nodes").VersionedParams(opts, scheme.ParameterCodec).MaxRetries(0).Do(ctx).Into(&nodes)); err != nil {
		return e, err
	}
	if nodes.Continue != "" || nodes.RemainingItemCount != nil && *nodes.RemainingItemCount != 0 || len(nodes.Items) > maxQualificationNodes {
		return e, errQualificationCoverage
	}
	e = qualificationDeployment{APIServer: p.server, CASHA256: p.caSHA256, Namespace: ns.Name, NamespaceUID: string(ns.UID),
		KubernetesVersion: v.GitVersion, Pool: pool, PolicyRevision: p.policy}
	for _, n := range nodes.Items {
		if n.Labels[pool.Key] != pool.Value {
			return qualificationDeployment{}, errQualificationCoverage
		}
		i := n.Status.NodeInfo
		e.Nodes = append(e.Nodes, qualificationNode{Name: n.Name, UID: string(n.UID), Kubelet: i.KubeletVersion,
			Runtime: i.ContainerRuntimeVersion, Kernel: i.KernelVersion, OS: i.OperatingSystem, Architecture: i.Architecture})
	}
	slices.SortFunc(e.Nodes, func(a, b qualificationNode) int { return strings.Compare(a.Name, b.Name) })
	for _, s := range r.Deployment.Storage {
		claim := d.references[s.Reference].pvc
		if claim == "" {
			return qualificationDeployment{}, errQualificationCoverage
		}
		var pvc corev1.PersistentVolumeClaim
		if err := done(core.Get().Namespace(namespace).Resource("persistentvolumeclaims").Name(claim).MaxRetries(0).Do(ctx).Into(&pvc)); err != nil {
			return qualificationDeployment{}, err
		}
		if pvc.Name != claim || pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
			return qualificationDeployment{}, errQualificationCoverage
		}
		class := *pvc.Spec.StorageClassName
		var sc storagev1.StorageClass
		if err := done(storage.Get().Resource("storageclasses").Name(class).MaxRetries(0).Do(ctx).Into(&sc)); err != nil {
			return qualificationDeployment{}, err
		}
		if sc.Name != class {
			return qualificationDeployment{}, errQualificationCoverage
		}
		e.Storage = append(e.Storage, qualificationStorage{Reference: s.Reference, PVCUID: string(pvc.UID),
			StorageClass: class, Driver: sc.Provisioner, PolicyRevision: p.storage[s.Reference]})
	}
	return e, nil
}
