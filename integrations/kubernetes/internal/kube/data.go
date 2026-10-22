package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"reflect"
	"slices"
	"strings"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

var _ backend.DataBackend = (*Backend)(nil)

// maxDataToken bounds one opaque scoped token. The integration never parses it.
const maxDataToken = 4096

// dataTask reports whether t uses any data field. Such a task never runs on
// the legacy Job or retry lanes.
func dataTask(t backend.Task) bool {
	return t.Workdir != nil || len(t.Setup) != 0 || len(t.Outputs) != 0 || len(t.DataMounts) != 0
}

// checkData validates a data task against the frozen target profile and its
// qualification. It performs no I/O.
func (b *Backend) checkData(t backend.Task) error {
	d := b.data
	if d == nil {
		return errors.New("kubernetes target has no data profile")
	}
	if t.Retry != nil {
		return errors.New("kubernetes staged retry is not supported")
	}
	if err := backend.ValidateDataTask(t, protectedPaths()); err != nil {
		return err
	}
	if t.Workdir == nil && t.Directory != "" {
		for _, p := range protectedPaths() {
			if pathsOverlap(t.Directory, p) {
				return errors.New("data directory overlaps protected path")
			}
		}
	}
	if _, err := backend.DefaultDataPolicy().Budgets(t.TimeoutSeconds); err != nil {
		return err
	}
	if !slices.Contains(d.taskImages, t.Image) {
		return errors.New("image is not approved for kubernetes data")
	}
	for _, m := range t.DataMounts {
		r, ok := d.references[m.Reference]
		if !ok {
			return errors.New("data reference is not configured")
		}
		if !slices.Contains(r.destinations, m.Destination) {
			return errors.New("data destination is not allowed")
		}
		if !m.ReadOnly && (r.pvc == "" || !r.allowWrite) {
			return errors.New("writable data mount is not allowed")
		}
	}
	return b.qualified(t)
}

// qualified refuses any task outside the installed qualification record.
// Without a configured record, the case for every legacy and unqualified
// target, it refuses all. It performs no I/O.
func (b *Backend) qualified(t backend.Task) error {
	q := b.dataQual
	if q == nil {
		return errors.New("kubernetes data profile is not qualified")
	}
	if err := checkQualificationTask(q.record, b.data, b.policy, t); err != nil {
		return fmt.Errorf("kubernetes data task is not qualified: %w", err)
	}
	return nil
}

func pathsOverlap(a, b string) bool {
	return a == b || a == "/" || b == "/" || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

// StartData creates the attempt's scoped Secrets and its one direct Pod. Each
// object is posted once without client retries. The returned receipts are the
// original Create-return UIDs, retained even alongside an error; a lost
// response leaves that resource unknown and is never repaired by lookup. If a
// response body's closure is uncertain, StartData sends nothing further and
// returns the receipts so far with errDataBodyHeld.
//
// Before the first create, StartData repeats the constructor's qualification
// reads in its own operation frame and refuses, creating nothing, if the
// deployment no longer matches the record. Earlier attempts remain observable
// and removable. Each response body of the call is limited before decoding.
func (b *Backend) StartData(ctx context.Context, s backend.DataStart) (c backend.DataCreated, err error) {
	ref := s.Execution
	t := ref.Task
	if b.binding == "" || ref.Binding != b.binding {
		return c, fmt.Errorf("%w: kubernetes target binding differs", backend.ErrIdentityMismatch)
	}
	if ref.Instance != "" {
		return c, fmt.Errorf("%w: data start has an instance", backend.ErrIdentityMismatch)
	}
	if _, err := jobName(s.JobID); err != nil {
		return c, err
	}
	if _, err := jobName(ref.ID); err != nil {
		return c, err
	}
	if !dataTask(t) {
		return c, errors.New("kubernetes data start requires a data task")
	}
	if err := b.Check(t); err != nil {
		return c, err
	}
	budgets, err := backend.DefaultDataPolicy().Budgets(t.TimeoutSeconds)
	if err != nil {
		return c, err
	}
	if s.Budgets != budgets {
		return c, errors.New("data budgets differ from target policy")
	}
	if !validToken(s.InputToken, t.Workdir != nil) || !validToken(s.OutputToken, len(t.Outputs) != 0) {
		return c, errors.New("data tokens do not match task scopes")
	}
	hash, err := taskHash(t)
	if err != nil {
		return c, err
	}
	pod, err := b.dataPod(ref.ID, t, budgets, hash)
	if err != nil {
		return c, err
	}
	client, ok := b.client.CoreV1().RESTClient().(*rest.RESTClient)
	if !ok || client == nil {
		return c, errors.New("kubernetes data requires a REST client")
	}
	ctx, f, err := b.beginData(ctx, ref.ID, false, false)
	if err != nil {
		return c, err
	}
	defer func() {
		if !b.endData(ref.ID, f) {
			err = withHeld(err)
		}
	}()
	f.limitBodies(maxQualificationResponse)
	if err := b.reserveData(ref.ID); err != nil {
		return c, err
	}
	if err := b.qualificationDrift(ctx); err != nil {
		return c, err
	}
	if t.Workdir != nil {
		c.InputSecret, err = b.createDataSecret(ctx, client, b.dataSecret(ref.ID, "input", s.InputToken, hash))
		if err != nil {
			return c, err
		}
	}
	if !f.clean() {
		return c, nil
	}
	if len(t.Outputs) != 0 {
		c.OutputSecret, err = b.createDataSecret(ctx, client, b.dataSecret(ref.ID, "output", s.OutputToken, hash))
		if err != nil {
			return c, err
		}
	}
	if !f.clean() {
		return c, nil
	}
	result := &corev1.Pod{}
	var created bool
	err = client.Post().Namespace(b.namespace).Resource("pods").
		VersionedParams(&metav1.CreateOptions{}, scheme.ParameterCodec).Body(pod).
		MaxRetries(0).Do(ctx).WasCreated(&created).Into(result)
	if err != nil {
		return c, err
	}
	if !created || !validInstance(string(result.UID)) {
		return c, errors.New("kubernetes data pod create did not return created")
	}
	c.Pod = backend.DataResource{Name: pod.Name, UID: string(result.UID)}
	c.Instance = c.Pod.UID
	return c, b.dataPodMatches(pod, result)
}

// validToken reports whether an opaque token is present exactly when its scope
// is required. Token bytes never appear in errors.
func validToken(token []byte, required bool) bool {
	return required == (len(token) != 0) && len(token) <= maxDataToken
}

// reserveData records that StartData was called for id. A reservation is never
// released: an attempt ID is started at most once in this server lifetime.
func (b *Backend) reserveData(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.dataStarts == nil {
		b.dataStarts = make(map[string]bool)
	}
	if b.dataStarts[id] {
		return errors.New("data attempt already started")
	}
	if len(b.dataStarts) >= maxRetryReceipts {
		return errors.New("kubernetes data start limit reached")
	}
	b.dataStarts[id] = true
	return nil
}

func (b *Backend) createDataSecret(ctx context.Context, client *rest.RESTClient, want *corev1.Secret) (backend.DataResource, error) {
	result := &corev1.Secret{}
	var created bool
	err := client.Post().Namespace(b.namespace).Resource("secrets").
		VersionedParams(&metav1.CreateOptions{}, scheme.ParameterCodec).Body(want).
		MaxRetries(0).Do(ctx).WasCreated(&created).Into(result)
	if err != nil {
		return backend.DataResource{}, err
	}
	if !created || !validInstance(string(result.UID)) {
		return backend.DataResource{}, errors.New("kubernetes data secret create did not return created")
	}
	r := backend.DataResource{Name: want.Name, UID: string(result.UID)}
	return r, b.dataSecretMatches(want, result)
}

// dataIdentity validates a reference to an accepted data attempt before any
// native read. Only the durable original Pod UID authorizes it.
func (b *Backend) dataIdentity(ref backend.Reference) error {
	if b.binding == "" || ref.Binding != b.binding {
		return fmt.Errorf("%w: kubernetes target binding differs", backend.ErrIdentityMismatch)
	}
	if _, err := jobName(ref.ID); err != nil {
		return err
	}
	if !validInstance(ref.Instance) {
		return fmt.Errorf("%w: data pod instance is unavailable", backend.ErrIdentityMismatch)
	}
	return b.Check(ref.Task)
}

// getData reads the Pod and checks its original UID and complete profile.
// A read whose response body Close failed reports errDataBodyHeld, joined
// with the read's own error, if any.
func (b *Backend) getData(ctx context.Context, ref backend.Reference, want *corev1.Pod) (*corev1.Pod, error) {
	p, err := b.client.CoreV1().Pods(b.namespace).Get(ctx, want.Name, metav1.GetOptions{})
	if f, _ := ctx.Value(dataFrameKey{}).(*dataFrame); f != nil && !f.usable() {
		if err != nil {
			return nil, withHeld(err)
		}
		return nil, errDataBodyHeld
	}
	if err != nil {
		return nil, err
	}
	if string(p.UID) != ref.Instance {
		return nil, fmt.Errorf("%w: kubernetes data pod replaced", backend.ErrIdentityMismatch)
	}
	if err := b.dataPodMatches(want, p); err != nil {
		return nil, err
	}
	return p, nil
}

func (b *Backend) expectedData(ref backend.Reference) (*corev1.Pod, backend.DataBudgets, error) {
	budgets, err := backend.DefaultDataPolicy().Budgets(ref.Task.TimeoutSeconds)
	if err != nil {
		return nil, budgets, err
	}
	hash, err := taskHash(ref.Task)
	if err != nil {
		return nil, budgets, err
	}
	want, err := b.dataPod(ref.ID, ref.Task, budgets, hash)
	return want, budgets, err
}

// ObserveData classifies the original Pod. Termination, no-launch and
// collector receipts require a second identical read within this call.
// A missing Pod is Held: absence is neither stop nor no-launch evidence. No
// observation is returned unless every response body closed without error.
func (b *Backend) ObserveData(ctx context.Context, ref backend.Reference) (o backend.DataObservation, err error) {
	if err := b.dataIdentity(ref); err != nil {
		return backend.DataObservation{}, err
	}
	want, budgets, err := b.expectedData(ref)
	if err != nil {
		return backend.DataObservation{}, err
	}
	ctx, f, err := b.beginData(ctx, ref.ID, false, false)
	if err != nil {
		return backend.DataObservation{}, err
	}
	defer func() {
		if !b.endData(ref.ID, f) {
			o, err = backend.DataObservation{}, withHeld(err)
		}
	}()
	held := func(reason string) backend.DataObservation {
		return backend.DataObservation{Status: backend.Status{State: backend.Unknown, Instance: ref.Instance, Reason: reason}, Phase: backend.DataHeld}
	}
	p, err := b.getData(ctx, ref, want)
	if apierrors.IsNotFound(err) {
		return held("kubernetes data pod missing"), nil
	}
	if err != nil {
		return backend.DataObservation{}, err
	}
	o = classifyData(p, ref.Task, budgets)
	if o.TaskStop != nil || o.NoLaunch != nil || o.Collector != nil {
		again, err := b.getData(ctx, ref, want)
		if apierrors.IsNotFound(err) {
			return held("kubernetes data pod missing"), nil
		}
		if err != nil {
			return backend.DataObservation{}, err
		}
		if !reflect.DeepEqual(o, classifyData(again, ref.Task, budgets)) {
			return held("kubernetes data termination changed"), nil
		}
	}
	if err := backend.ValidateDataObservation(ref, o); err != nil {
		return backend.DataObservation{}, err
	}
	return o, nil
}

// RemoveData retires the exact recorded resources. Each flag reports this
// call's own UID-preconditioned deletion and subsequent absence of that UID.
// A same-name replacement is left untouched and its flag stays false. Removal
// never proves that execution stopped. All flags are false unless every
// response body of the call closed without error; after an uncertain closure
// no further request is sent.
func (b *Backend) RemoveData(ctx context.Context, r backend.DataResources) (out backend.DataRemoval, err error) {
	out = backend.DataRemoval{Pod: r.Pod, InputSecret: r.InputSecret, OutputSecret: r.OutputSecret}
	if err := backend.ValidateDataResources(r); err != nil {
		return out, err
	}
	ref := r.Execution
	if b.binding == "" || ref.Binding != b.binding {
		return out, fmt.Errorf("%w: kubernetes target binding differs", backend.ErrIdentityMismatch)
	}
	name, err := jobName(ref.ID)
	if err != nil {
		return out, err
	}
	if !dataTask(ref.Task) {
		return out, errors.New("kubernetes data removal requires a data task")
	}
	if r.Pod.Name != "" && r.Pod.Name != name ||
		r.InputSecret.Name != "" && r.InputSecret.Name != dataSecretName(ref.ID, "input") ||
		r.OutputSecret.Name != "" && r.OutputSecret.Name != dataSecretName(ref.ID, "output") {
		return out, fmt.Errorf("%w: data resource names differ", backend.ErrIdentityMismatch)
	}
	if b.data == nil {
		return out, errors.New("kubernetes target has no data profile")
	}
	// The expected ownership and profile of the original objects. A present
	// object with the recorded UID must still match them before deletion.
	hash, err := taskHash(ref.Task)
	if err != nil {
		return out, err
	}
	pod, _, err := b.expectedData(ref)
	if err != nil {
		return out, err
	}
	ctx, f, err := b.beginData(ctx, ref.ID, true, false)
	if err != nil {
		return out, err
	}
	defer func() {
		if !b.endData(ref.ID, f) {
			out.PodRemoved, out.InputRemoved, out.OutputRemoved = false, false, false
			err = withHeld(err)
		}
	}()
	var errs []error
	remove := func(resource string, r backend.DataResource, want metav1.ObjectMeta, pod *corev1.Pod) bool {
		if r == (backend.DataResource{}) || !f.clean() {
			return false
		}
		ok, err := b.removeData(ctx, resource, r, want, pod)
		errs = append(errs, err)
		return ok
	}
	out.PodRemoved = remove("pods", r.Pod, pod.ObjectMeta, pod)
	out.InputRemoved = remove("secrets", r.InputSecret, b.profileMeta(dataSecretName(ref.ID, "input"), ref.ID, hash), nil)
	out.OutputRemoved = remove("secrets", r.OutputSecret, b.profileMeta(dataSecretName(ref.ID, "output"), ref.ID, hash), nil)
	return out, errors.Join(errs...)
}

// metadataOnly asks the API server to return only object metadata, so that
// cleanup never reads token Secret data back.
const metadataOnly = "application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1"

// removeData deletes one recorded object only under its original UID and
// reports true only after a later read in this call shows that UID absent.
// A present original must still carry the expected ownership metadata want
// and, for a Pod, the complete expected profile pod; otherwise it is left
// untouched. Secret reads and every delete response carry metadata only.
func (b *Backend) removeData(ctx context.Context, resource string, r backend.DataResource, want metav1.ObjectMeta, pod *corev1.Pod) (bool, error) {
	client, ok := b.client.CoreV1().RESTClient().(*rest.RESTClient)
	if !ok || client == nil {
		return false, errors.New("kubernetes data requires a REST client")
	}
	var meta metav1.ObjectMeta
	get := func() (types.UID, error) {
		raw, err := client.Get().Namespace(b.namespace).Resource(resource).Name(r.Name).
			SetHeader("Accept", metadataOnly).Do(ctx).Raw()
		if err != nil {
			return "", err
		}
		var m metav1.PartialObjectMetadata
		if err := json.Unmarshal(raw, &m); err != nil || m.Kind != "PartialObjectMetadata" || m.Name != r.Name {
			return "", errors.New("invalid kubernetes data metadata response")
		}
		meta = m.ObjectMeta
		return m.UID, nil
	}
	uid, err := get()
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if string(uid) != r.UID {
		return false, fmt.Errorf("%w: kubernetes data %s replaced", backend.ErrIdentityMismatch, resource)
	}
	if !profileMetaMatches(want, meta) {
		return false, fmt.Errorf("%w: kubernetes data %s ownership differs", backend.ErrIdentityMismatch, resource)
	}
	if pod != nil {
		// Pods hold no credentials; their full spec is read to verify the profile.
		p, err := b.client.CoreV1().Pods(b.namespace).Get(ctx, r.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if string(p.UID) != r.UID {
			return false, fmt.Errorf("%w: kubernetes data %s replaced", backend.ErrIdentityMismatch, resource)
		}
		if err := b.dataPodMatches(pod, p); err != nil {
			return false, err
		}
	}
	expected := types.UID(r.UID)
	opts := &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &expected}}
	err = client.Delete().Namespace(b.namespace).Resource(resource).Name(r.Name).
		SetHeader("Accept", metadataOnly).Body(opts).Do(ctx).Error()
	if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return false, err
	}
	uid, err = get()
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if string(uid) != r.UID {
		return false, fmt.Errorf("%w: kubernetes data %s replaced", backend.ErrIdentityMismatch, resource)
	}
	return false, fmt.Errorf("kubernetes data %s deletion remains pending", resource)
}

// cancelData requests UID-preconditioned deletion of the original Pod. It
// leaves Secrets for RemoveData and proves nothing about stopped work.
func (b *Backend) cancelData(ctx context.Context, ref backend.Reference) (err error) {
	if err := b.dataIdentity(ref); err != nil {
		return err
	}
	want, _, err := b.expectedData(ref)
	if err != nil {
		return err
	}
	ctx, f, err := b.beginData(ctx, ref.ID, false, false)
	if err != nil {
		return err
	}
	defer func() {
		if !b.endData(ref.ID, f) {
			err = withHeld(err)
		}
	}()
	p, err := b.getData(ctx, ref, want)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
		return nil
	}
	uid := types.UID(ref.Instance)
	err = b.client.CoreV1().Pods(b.namespace).Delete(ctx, p.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// logsData reads the bounded task-container prefix of the original Pod. It
// runs as the attempt's single log frame. The original response body is
// closed exactly once; if that Close fails, no chunk is returned and the frame
// and its claim are held (see dataFrame).
func (b *Backend) logsData(ctx context.Context, ref backend.Reference, offset int64, limit int) (chunk backend.LogChunk, err error) {
	if offset < 0 || limit <= 0 || limit > backend.MaxChunkBytes {
		return backend.LogChunk{}, fmt.Errorf("invalid log offset or limit: %w", fs.ErrInvalid)
	}
	if err := b.dataIdentity(ref); err != nil {
		return backend.LogChunk{}, err
	}
	want, _, err := b.expectedData(ref)
	if err != nil {
		return backend.LogChunk{}, err
	}
	ctx, f, err := b.beginData(ctx, ref.ID, false, true)
	if err != nil {
		return backend.LogChunk{}, err
	}
	defer func() {
		if !b.endData(ref.ID, f) {
			chunk, err = backend.LogChunk{}, withHeld(err)
		}
	}()
	p, err := b.getData(ctx, ref, want)
	if apierrors.IsNotFound(err) {
		return backend.LogChunk{}, fmt.Errorf("data pod logs are no longer retained: %w", fs.ErrNotExist)
	}
	if err != nil {
		return backend.LogChunk{}, err
	}
	resp, err := b.openLogs(ctx, p.Name)
	if err != nil {
		return backend.LogChunk{}, err
	}
	stream := resp.Body
	// The frame records the Close result; endData reports a failure.
	defer stream.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		io.Copy(io.Discard, io.LimitReader(stream, 4096))
		if resp.StatusCode == http.StatusNotFound {
			return backend.LogChunk{}, fmt.Errorf("data pod logs are no longer retained: %w", fs.ErrNotExist)
		}
		return backend.LogChunk{}, fmt.Errorf("kubernetes data log request failed with status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(stream, backend.MaxLogBytes+1))
	if err != nil {
		return backend.LogChunk{}, err
	}
	// Pod logs have no UID precondition; check the original Pod again.
	if _, err := b.getData(ctx, ref, want); err != nil {
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

// openLogs sends exactly one task-container log request for the named Pod
// through the REST client's HTTP client, so the tagged body reaches the frame.
// Unlike rest.Request.Stream, it neither retries nor closes the response body
// itself: the caller owns the body for every status and closes it once.
func (b *Backend) openLogs(ctx context.Context, name string) (*http.Response, error) {
	client, ok := b.client.CoreV1().RESTClient().(*rest.RESTClient)
	if !ok || client == nil || client.Client == nil {
		return nil, errors.New("kubernetes data requires a REST client")
	}
	if l := client.GetRateLimiter(); l != nil {
		if err := l.Wait(ctx); err != nil {
			return nil, err
		}
	}
	u := client.Get().Namespace(b.namespace).Resource("pods").Name(name).SubResource("log").
		VersionedParams(&corev1.PodLogOptions{Container: "task"}, scheme.ParameterCodec).URL()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	return client.Client.Do(req)
}
