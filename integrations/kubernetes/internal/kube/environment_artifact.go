package kube

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Artifacts. The Pod of an environment whose spec has artifacts has, in
// addition to its workspace container, one init container, stage, which
// runs the target's digest-pinned helper image, and two volumes: an
// emptyDir, artifacts, which only stage mounts writable, and a downward API
// volume, instance, which holds only the file uid, the Pod's own
// metadata.uid as the kubelet writes it. The workspace container mounts
// each artifact file read-only, once for every mount path of its reference,
// by subPath. A spec without artifacts gets exactly the Pod it got before.
//
// Barrier. stage runs "skygo-data env-wait" with the manifest of every
// artifact file: name, SHA-256 and size. It records the manifest and waits
// for the release marker, which only Bootstrap's "skygo-data env-release"
// writes. It exits 0 only after the marker records the instance's UID and
// exactly the manifest and every file verified, so the kubelet starts the
// workspace container, and so the idle command and any exec, only after
// Bootstrap. Bootstrap is the one release boundary; no file, lock or
// Materialize result releases the barrier. A stage that fails or times out
// fails the Pod, which is then never Ready. Its timeout is a source policy,
// not a measured bound.
//
// Fence. pods/exec addresses a Pod by name; it has no UID or
// resourceVersion precondition, so no client read before or after it can
// fence the exec to an instance. The fence is in the helper: Materialize
// runs one "skygo-data env-put uid name sha256 size" in stage, and
// Bootstrap one "skygo-data env-release uid -- manifest", and the helper
// compares uid with the instance file of the container it runs in before
// it reads stdin, takes the staging lock or creates any file. On a
// difference, or if the file cannot be read, it exits
// environmentIdentityExit and has no effect. An env-put that finds the
// marker exits environmentSealedExit and has no effect. The Pod reads before
// and after the exec only classify its result.
//
// One operation per reference. A reference is one artifact file, a<i> for
// the i-th distinct reference in spec order. One Materialize streams its
// bytes, exactly Size of them, as the exec's stdin; the helper writes them
// to a new temporary file, requires exactly Size bytes and then end of
// input, compares the SHA-256, sets mode 0444, syncs and links the file to
// its name without replacing one. Every mount path of the reference is a
// read-only subPath mount of that one file.

const (
	environmentStageContainer = "stage"
	environmentArtifactVolume = "artifacts"
	environmentInstanceVolume = "instance"
	environmentArtifactDir    = "/run/skygo/artifacts"
	environmentInstanceDir    = "/run/skygo/instance"
	environmentPutVerb        = "env-put"
	environmentWaitVerb       = "env-wait"
	environmentReleaseVerb    = "env-release"

	// environmentIdentityExit is the helper's exit code when it refused
	// before any effect because the instance's own UID was not the
	// expected one or could not be read; environmentSealedExit, when an
	// env-put refused because the barrier was already released.
	environmentIdentityExit = 3
	environmentSealedExit   = 4

	// maxEnvironmentArtifacts bounds the mounts, and so the distinct
	// references, of one environment.
	maxEnvironmentArtifacts = environment.MaxArtifacts
	// maxEnvironmentArtifactBytes bounds one artifact. It is a source
	// policy, not a measured capacity.
	maxEnvironmentArtifactBytes = 1 << 30
)

// errEnvironmentInstanceRefused reports that the helper refused, before any
// effect, because the instance it ran in was not the incarnation. The exec
// had started and body may have been read, so it is a failed result, not
// environmentcontrol.ErrIncarnation.
var errEnvironmentInstanceRefused = errors.New("kubernetes environment helper refused the instance")

// errEnvironmentSealed reports that the helper refused an env-put, before
// any effect, because the instance's barrier was already released.
var errEnvironmentSealed = errors.New("kubernetes environment instance already released")

// errMaterializeBusy reports a Materialize of a reference whose earlier
// Materialize in this process has not settled. Nothing was started.
var errMaterializeBusy = errors.New("kubernetes environment artifact staging in progress")

// environmentArtifact is one distinct reference of a spec and the mount
// paths that serve it, sorted.
type environmentArtifact struct {
	id     string
	sha256 string
	size   int64
	paths  []string
}

// environmentArtifactName returns the file name of the i-th reference.
func environmentArtifactName(i int) string { return "a" + strconv.Itoa(i) }

// environmentArtifacts groups mounts by reference, in order of first
// occurrence, and checks them for a Pod: at most maxEnvironmentArtifacts
// mounts, one SHA-256 and size per ID, a positive size of at most
// maxEnvironmentArtifactBytes, and every mount path absolute, clean,
// distinct, neither inside another nor inside the workspace claim or a
// protected path. environment.Validate already confines mount paths to
// /skygo/artifacts/; the Pod checks are repeated here because a path inside
// the claim would make the kubelet create its mount point in the borrowed
// volume.
func environmentArtifacts(mounts []environment.ArtifactMount) ([]environmentArtifact, error) {
	if len(mounts) > maxEnvironmentArtifacts {
		return nil, errors.New("too many kubernetes environment artifact mounts")
	}
	var arts []environmentArtifact
	var all []string
	for _, m := range mounts {
		if !lowerHexDigest(m.SHA256) || m.Size == 0 || m.Size > maxEnvironmentArtifactBytes || m.ID == "" {
			return nil, errors.New("invalid kubernetes environment artifact")
		}
		size := int64(m.Size)
		p := m.MountPath
		if !path.IsAbs(p) || path.Clean(p) != p || p == "/" {
			return nil, errors.New("invalid kubernetes environment artifact mount path")
		}
		for _, q := range append(slices.Clone(all), append(protectedPaths(), workspaceMount)...) {
			if p == q || strings.HasPrefix(p, q+"/") || strings.HasPrefix(q, p+"/") {
				return nil, errors.New("kubernetes environment artifact mount path conflicts")
			}
		}
		all = append(all, p)
		i := slices.IndexFunc(arts, func(a environmentArtifact) bool { return a.id == m.ID })
		switch {
		case i < 0:
			arts = append(arts, environmentArtifact{id: m.ID, sha256: m.SHA256, size: size})
			i = len(arts) - 1
		case arts[i].sha256 != m.SHA256 || arts[i].size != size:
			return nil, errors.New("kubernetes environment artifact reference differs between mounts")
		}
		arts[i].paths = append(arts[i].paths, p)
	}
	for i := range arts {
		slices.Sort(arts[i].paths)
	}
	return arts, nil
}

// environmentManifest returns the helper's manifest operands for arts:
// a<i>:sha256:size for the i-th reference.
func environmentManifest(arts []environmentArtifact) []string {
	var m []string
	for i, a := range arts {
		m = append(m, environmentArtifactName(i)+":"+a.sha256+":"+strconv.FormatInt(a.size, 10))
	}
	return m
}

// lowerHexDigest reports whether s is a SHA-256 digest in lowercase hex.
func lowerHexDigest(s string) bool {
	return len(s) == 64 && strings.Trim(s, "0123456789abcdef") == ""
}

// addEnvironmentArtifacts adds the stage init container, the artifacts and
// instance volumes and the workspace container's read-only artifact mounts
// to spec, which environmentPod built. Every field the API server would
// default is set, so fixedPodSpecMatches compares the read-back Pod
// exactly.
func (b *Backend) addEnvironmentArtifacts(spec *corev1.PodSpec, arts []environmentArtifact) {
	var total int64
	for _, a := range arts {
		total += a.size
	}
	// Room for every file, one temporary copy of each, and the lock,
	// manifest and marker, if one writer per reference runs at a time. It
	// is an eviction policy, not reserved or measured capacity.
	limit := resource.NewQuantity(2*total+1<<20, resource.BinarySI)
	mode := int32(0o444)
	spec.Volumes = append(spec.Volumes,
		corev1.Volume{Name: environmentArtifactVolume, VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: limit}}},
		corev1.Volume{Name: environmentInstanceVolume, VolumeSource: corev1.VolumeSource{
			DownwardAPI: &corev1.DownwardAPIVolumeSource{
				Items: []corev1.DownwardAPIVolumeFile{{Path: "uid",
					FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.uid"}}},
				DefaultMode: &mode}}})
	work := &spec.Containers[0]
	// stage gets the workspace container's cpu and memory, so the Pod's
	// effective requests are unchanged, and no accelerator.
	res := corev1.ResourceRequirements{Requests: corev1.ResourceList{}, Limits: corev1.ResourceList{}}
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		if q, ok := work.Resources.Requests[name]; ok {
			res.Requests[name] = q.DeepCopy()
		}
		if q, ok := work.Resources.Limits[name]; ok {
			res.Limits[name] = q.DeepCopy()
		}
	}
	spec.InitContainers = []corev1.Container{{
		Name:      environmentStageContainer,
		Image:     b.data.helperImage,
		Command:   append([]string{helperEntry, environmentWaitVerb, "--"}, environmentManifest(arts)...),
		Resources: res,
		VolumeMounts: []corev1.VolumeMount{
			{Name: environmentArtifactVolume, MountPath: environmentArtifactDir},
			{Name: environmentInstanceVolume, MountPath: environmentInstanceDir, ReadOnly: true},
		},
		ImagePullPolicy: corev1.PullIfNotPresent,
		SecurityContext: profileContainerSecurity(),
	}}
	for i, a := range arts {
		for _, p := range a.paths {
			work.VolumeMounts = append(work.VolumeMounts, corev1.VolumeMount{Name: environmentArtifactVolume,
				MountPath: p, SubPath: environmentArtifactName(i), ReadOnly: true})
		}
	}
}

// environmentStaging reports a Pod whose stage container is running and
// whose workspace container has not started: one that has not
// bootstrapped.
func environmentStaging(pod *corev1.Pod) bool {
	started := func(s corev1.ContainerStatus) bool { return s.State.Running != nil || s.State.Terminated != nil }
	if pod.Status.Phase != corev1.PodPending || slices.ContainsFunc(pod.Status.ContainerStatuses, started) {
		return false
	}
	for _, s := range pod.Status.InitContainerStatuses {
		if s.Name == environmentStageContainer {
			return s.State.Running != nil
		}
	}
	return false
}

// environmentStageKey names one reference of one environment.
type environmentStageKey struct {
	id    string
	index int
}

// environmentMaterialization is the frame of one Materialize or Bootstrap.
// done is closed once the exec frame settled, which is after its one stdin
// goroutine, the only reader of a Materialize body, returned, and the Pod at
// the environment's name was read again. That joins the local exec only,
// not the remote helper process.
type environmentMaterialization struct {
	x       *execFrame
	body    bool // a Materialize, whose body must have been sent
	done    chan struct{}
	changed error // set before done is closed
}

func (m *environmentMaterialization) Done() <-chan struct{} { return m.done }
func (m *environmentMaterialization) Cancel()               { m.x.cancel() }

// Result reports a settled frame. Its Err is nil only if the exec's status
// reported exit 0, after, for a Materialize, exactly the artifact's size was
// sent and stdin was closed, and the recorded Pod was still at the
// environment's name afterwards. For a Materialize the one artifact file,
// which every mount binds read-only, then holds exactly Size bytes with
// SHA256 and mode 0444, and the instance has not bootstrapped, since it
// waits for Bootstrap. Any other result is unknown, even
// errEnvironmentInstanceRefused and errEnvironmentSealed, which report that
// the helper refused before any effect.
func (m *environmentMaterialization) Result() environmentcontrol.MaterializationResult {
	select {
	case <-m.done:
	default:
		return environmentcontrol.MaterializationResult{}
	}
	res := m.x.result()
	var err error
	switch {
	case res.Reported && res.Code == environmentIdentityExit:
		err = errors.Join(errEnvironmentInstanceRefused, res.Err)
	case res.Reported && res.Code == environmentSealedExit:
		err = errors.Join(errEnvironmentSealed, res.Err)
	case res.Err != nil:
		err = res.Err
	case !res.Reported:
		err = errors.New("kubernetes artifact staging exit unknown")
	case res.Code != 0:
		err = fmt.Errorf("kubernetes artifact staging exited %d", res.Code)
	case m.body && !res.BodySent:
		err = errors.New("kubernetes artifact staging input incomplete")
	}
	return environmentcontrol.MaterializationResult{Settled: true, Err: errors.Join(err, m.changed)}
}

// artifact returns the index and group of the reference that mounts names.
// mounts must be exactly the mounts of spec with that reference's ID, in
// spec order.
func (r environmentResolved) artifact(spec, mounts []environment.ArtifactMount) (int, environmentArtifact, error) {
	if len(mounts) == 0 {
		return 0, environmentArtifact{}, errors.New("kubernetes environment materialization requires mounts")
	}
	id := mounts[0].ID
	i := slices.IndexFunc(r.in.artifacts, func(a environmentArtifact) bool { return a.id == id })
	if i < 0 {
		return 0, environmentArtifact{}, errors.New("kubernetes environment artifact is not in the spec")
	}
	var want []environment.ArtifactMount
	for _, m := range spec {
		if m.ID == id {
			want = append(want, m)
		}
	}
	if !slices.Equal(mounts, want) {
		return 0, environmentArtifact{}, errors.New("kubernetes environment artifact mounts differ from the spec")
	}
	return i, r.in.artifacts[i], nil
}

// staging checks, with one consistent read, that the Pod at r's name is the
// recorded one, is incarnation, matches r and has not bootstrapped. It is a
// preflight only; the helper's own check is the fence.
func (rt *environmentRuntime) staging(ctx context.Context, r environmentResolved, incarnation string) error {
	n := r.native
	if n.Pod == nil || n.Pod.Altered || string(n.Pod.UID) != incarnation {
		return fmt.Errorf("%w: kubernetes environment pod is not the incarnation", environmentcontrol.ErrIncarnation)
	}
	o, err := rt.read(ctx, r)
	if err != nil {
		return err
	}
	identity := environmentPodIdentity{podUID: n.Pod.UID, podGeneration: n.Pod.Generation}
	if err := rt.b.matchEnvironmentPod(r.in, r.profile, identity, o.pod, o.namespace, o.claim); err != nil {
		if errors.Is(err, errEnvironmentObjectMissing) || errors.Is(err, errEnvironmentObjectUnavailable) ||
			errors.Is(err, backend.ErrIdentityMismatch) {
			return errors.Join(environmentcontrol.ErrIncarnation, err)
		}
		return err
	}
	if !environmentStaging(o.pod) {
		return fmt.Errorf("%w: kubernetes environment pod is not staging", environmentcontrol.ErrIncarnation)
	}
	return nil
}

// takeStage marks reference k as staging in this process. It reports false
// if an earlier Materialize of k has not settled.
func (rt *environmentRuntime) takeStage(k environmentStageKey) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.stages[k] {
		return false
	}
	rt.stages[k] = true
	return true
}

func (rt *environmentRuntime) dropStage(k environmentStageKey) {
	rt.mu.Lock()
	delete(rt.stages, k)
	rt.mu.Unlock()
}

// Materialize starts staging the bytes of one reference into the instance
// incarnation of ref's environment, for every mount of that reference,
// which mounts must list exactly, in spec order. An error means that no
// exec request was sent and body was never read; it wraps
// environmentcontrol.ErrIncarnation if the preflight read found that the
// Pod is not incarnation or has bootstrapped. Otherwise the returned frame
// is the only record of the call; it reads exactly the reference's size
// from body once, from one goroutine, and never reads body after Done is
// closed. Cancel does not end a Read of body in progress; Done waits for
// it. The helper's check in the instance precedes every effect, and the
// instance does not bootstrap until Bootstrap.
func (rt *environmentRuntime) Materialize(ref environmentcontrol.Ref, incarnation string,
	mounts []environment.ArtifactMount, body io.Reader) (environmentcontrol.Materialization, error) {
	r, err := rt.resolve(ref)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, errors.New("kubernetes environment artifact body is required")
	}
	i, a, err := r.artifact(ref.Spec.Artifacts, mounts)
	if err != nil {
		return nil, err
	}
	return rt.stage(r, incarnation, i, execRequest{Pod: r.name, Container: environmentStageContainer,
		Argv: []string{helperEntry, environmentPutVerb, incarnation, environmentArtifactName(i),
			a.sha256, strconv.FormatInt(a.size, 10)},
		Body: body, BodySize: a.size})
}

// Bootstrap starts releasing the barrier of the instance incarnation of
// ref's environment, which the Controller calls once, after every
// Materialize of that incarnation succeeded and its body was closed. In the
// instance, the helper checks that it is incarnation, waits for running
// env-puts, verifies every artifact against the spec's manifest and records
// a durable release marker; the stage container then verifies the marker
// and every artifact and exits, and the workspace container starts. The
// marker is never removed, and no artifact changes after it.
//
// An error means that no exec request was sent, as for Materialize. The
// frame's Result is Settled with a nil Err only if the helper reported
// that the marker records exactly incarnation and the manifest, and the
// recorded Pod was still at the environment's name afterwards. Any other
// result is unknown: the marker may exist and the workspace may start. A
// repeated Bootstrap of a released instance before the stage container
// exits succeeds; after it exits, the preflight refuses it with
// ErrIncarnation, and only Ready from Ensure shows the release.
func (rt *environmentRuntime) Bootstrap(ref environmentcontrol.Ref, incarnation string) (environmentcontrol.Materialization, error) {
	r, err := rt.resolve(ref)
	if err != nil {
		return nil, err
	}
	if len(r.in.artifacts) == 0 {
		return nil, errors.New("kubernetes environment has no artifacts")
	}
	argv := append([]string{helperEntry, environmentReleaseVerb, incarnation, "--"}, environmentManifest(r.in.artifacts)...)
	return rt.stage(r, incarnation, maxEnvironmentArtifacts, execRequest{Pod: r.name,
		Container: environmentStageContainer, Argv: argv})
}

// stage runs req, an env-put or env-release, in the stage container of the
// instance incarnation, as key index of the environment's stage keys. It
// checks the preflight first; an error means that req was not sent.
func (rt *environmentRuntime) stage(r environmentResolved, incarnation string, index int, req execRequest) (environmentcontrol.Materialization, error) {
	if r.native.Closing {
		return nil, errEnvironmentDeleting
	}
	k := environmentStageKey{r.id, index}
	if !rt.takeStage(k) {
		return nil, errMaterializeBusy
	}
	key, err := rt.reserve(r.id)
	if err != nil {
		rt.dropStage(k)
		return nil, err
	}
	fail := func(err error) (environmentcontrol.Materialization, error) {
		rt.release(r.id, key)
		rt.dropStage(k)
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rt.effects, environmentReadTimeout)
	defer cancel()
	if err := rt.staging(ctx, r, incarnation); err != nil {
		return fail(err)
	}
	if rt.isStopped() {
		return fail(errEnvironmentStopped)
	}
	x, err := rt.port.startExec(req)
	if err != nil {
		return fail(err)
	}
	rt.attach(r.id, key, x.cancel)
	m := &environmentMaterialization{x: x, body: req.Body != nil, done: make(chan struct{})}
	go func() {
		<-x.done
		ctx, cancel := context.WithTimeout(rt.effects, environmentReadTimeout)
		defer cancel()
		got, err := rt.getPod(ctx, r)
		switch {
		case err != nil:
			m.changed = errors.Join(errEnvironmentPodChanged, err)
		case got == nil || string(got.UID) != incarnation:
			m.changed = errEnvironmentPodChanged
		}
		rt.release(r.id, key)
		rt.dropStage(k)
		close(m.done)
	}()
	return m, nil
}

// environmentIncarnation returns uid as an Observation.Incarnation, or ""
// if it does not have that form.
func environmentIncarnation(uid string) string {
	if len(uid) == 0 || len(uid) > 128 {
		return ""
	}
	for _, c := range []byte(uid) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '.' || c == '_' || c == ':' || c == '-') {
			return ""
		}
	}
	return uid
}
