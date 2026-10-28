package kube

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// environmentAPIModel MODELS the Kubernetes API through clientset reactors.
// It is not a server-version capture or a native qualification. It retains
// the S14 model's UID/resourceVersion allocation, consistent reads, declared
// action hooks, replay, Update/Delete preconditions, no-op Update semantics,
// foreground finalizers and paged Pod lists. Admission, kubelet status,
// late delivery and garbage collection are explicit test actions. Service,
// Deployment, ReplicaSet, EndpointSlice and HPA behavior is not included.

type environmentAPIModel struct {
	t      *testing.T
	client *fake.Clientset

	mu    sync.Mutex
	rv    int
	uid   int
	page  int // items per List page
	objs  map[environmentAPIKey]runtime.Object
	hooks []*environmentAPIHook
	calls []environmentAPICall
}

type environmentAPIKey struct{ resource, name string }

type environmentAPIHook struct {
	verb, resource string
	n              int // remaining uses; negative is unlimited
	fn             func(a ktesting.Action, next func() (runtime.Object, error)) (runtime.Object, error)
}

type environmentAPICall struct {
	verb, resource, name string
	obj                  runtime.Object // create and update bodies, as sent
	del                  metav1.DeleteOptions
}

func newEnvironmentAPIModel(t *testing.T) *environmentAPIModel {
	f := &environmentAPIModel{t: t, client: fake.NewSimpleClientset(), rv: 100, page: 2, objs: map[environmentAPIKey]runtime.Object{}}
	f.client.PrependReactor("*", "*", f.react)
	return f
}

func (f *environmentAPIModel) react(a ktesting.Action) (bool, runtime.Object, error) {
	f.mu.Lock()
	c := environmentAPICall{verb: a.GetVerb(), resource: a.GetResource().Resource}
	switch a := a.(type) {
	case ktesting.GetActionImpl:
		c.name = a.Name
		if a.GetOptions.ResourceVersion != "" {
			f.t.Errorf("inconsistent get of %s %s: %+v", c.resource, a.Name, a.GetOptions)
		}
	case ktesting.ListActionImpl:
		if o := a.ListOptions; o.ResourceVersion != "" || o.ResourceVersionMatch != "" || o.Limit <= 0 {
			f.t.Errorf("inconsistent or unpaged list of %s: %+v", c.resource, o)
		}
	case ktesting.CreateActionImpl:
		c.obj = a.Object.DeepCopyObject()
	case ktesting.UpdateActionImpl:
		c.obj = a.Object.DeepCopyObject()
	case ktesting.DeleteActionImpl:
		c.name, c.del = a.Name, *a.DeleteOptions.DeepCopy()
	}
	f.calls = append(f.calls, c)
	var h *environmentAPIHook
	for _, x := range f.hooks {
		if x.n != 0 && x.verb == c.verb && x.resource == c.resource {
			h = x
			if x.n > 0 {
				x.n--
			}
			break
		}
	}
	f.mu.Unlock()
	next := func() (runtime.Object, error) { return f.replay(a) }
	if h == nil {
		obj, err := next()
		return true, obj, err
	}
	obj, err := h.fn(a, next)
	return true, obj, err
}

// on installs a hook for the next n matching requests.

func (f *environmentAPIModel) on(verb, resource string, n int, fn func(ktesting.Action, func() (runtime.Object, error)) (runtime.Object, error)) *environmentAPIHook {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := &environmentAPIHook{verb: verb, resource: resource, n: n, fn: fn}
	f.hooks = append(f.hooks, h)
	return h
}

func (f *environmentAPIModel) off(h *environmentAPIHook) {
	f.mu.Lock()
	h.n = 0
	f.mu.Unlock()
}

// replay applies a, which may be a request held back earlier, to the model.

func (f *environmentAPIModel) replay(a ktesting.Action) (runtime.Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res := a.GetResource().Resource
	gr := a.GetResource().GroupResource()
	switch a := a.(type) {
	case ktesting.GetActionImpl:
		o, ok := f.objs[environmentAPIKey{res, a.Name}]
		if !ok {
			return nil, apierrors.NewNotFound(gr, a.Name)
		}
		return o.DeepCopyObject(), nil
	case ktesting.ListActionImpl:
		return f.list(res, a)
	case ktesting.CreateActionImpl:
		return f.create(gr, a.Object.DeepCopyObject())
	case ktesting.UpdateActionImpl:
		return f.update(gr, a.Object.DeepCopyObject())
	case ktesting.DeleteActionImpl:
		return nil, f.delete(gr, a.Name, a.DeleteOptions)
	}
	return nil, fmt.Errorf("environmentAPIModel: unexpected %s of %s", a.GetVerb(), res)
}

func (f *environmentAPIModel) assign(m metav1.Object) {
	if m.GetUID() == "" {
		f.uid++
		m.SetUID(types.UID(fmt.Sprintf("%08x-0000-4000-8000-%012x", f.uid, f.uid)))
	}
	f.rv++
	m.SetResourceVersion(strconv.Itoa(f.rv))
}

func (f *environmentAPIModel) create(gr schema.GroupResource, obj runtime.Object) (runtime.Object, error) {
	if gr.Resource != "pods" {
		return nil, fmt.Errorf("environmentAPIModel: unexpected create of %s", gr.Resource)
	}
	m, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	k := environmentAPIKey{gr.Resource, m.GetName()}
	if _, ok := f.objs[k]; ok {
		return nil, apierrors.NewAlreadyExists(gr, m.GetName())
	}
	f.assign(m)
	f.objs[k] = obj
	return obj.DeepCopyObject(), nil
}

func (f *environmentAPIModel) update(gr schema.GroupResource, obj runtime.Object) (runtime.Object, error) {
	if gr.Resource != "pods" {
		return nil, fmt.Errorf("environmentAPIModel: unexpected update of %s", gr.Resource)
	}
	m, _ := meta.Accessor(obj)
	k := environmentAPIKey{gr.Resource, m.GetName()}
	cur, ok := f.objs[k]
	if !ok {
		return nil, apierrors.NewNotFound(gr, m.GetName())
	}
	cm, _ := meta.Accessor(cur)
	if rv := m.GetResourceVersion(); rv != "" && rv != cm.GetResourceVersion() || m.GetUID() != "" && m.GetUID() != cm.GetUID() {
		return nil, apierrors.NewConflict(gr, m.GetName(), errors.New("the object has been modified"))
	}
	m.SetUID(cm.GetUID())
	m.SetCreationTimestamp(cm.GetCreationTimestamp())
	m.SetDeletionTimestamp(cm.GetDeletionTimestamp())
	m.SetGeneration(cm.GetGeneration())
	m.SetResourceVersion(cm.GetResourceVersion())
	if equality.Semantic.DeepEqual(obj, cur) {
		return cur.DeepCopyObject(), nil
	}
	f.rv++
	m.SetResourceVersion(strconv.Itoa(f.rv))
	f.objs[k] = obj
	return obj.DeepCopyObject(), nil
}

func (f *environmentAPIModel) delete(gr schema.GroupResource, name string, o metav1.DeleteOptions) error {
	if gr.Resource != "pods" {
		return fmt.Errorf("environmentAPIModel: unexpected delete of %s", gr.Resource)
	}
	k := environmentAPIKey{gr.Resource, name}
	cur, ok := f.objs[k]
	if !ok {
		return apierrors.NewNotFound(gr, name)
	}
	cm, _ := meta.Accessor(cur)
	if p := o.Preconditions; p != nil && (p.UID != nil && *p.UID != cm.GetUID() ||
		p.ResourceVersion != nil && *p.ResourceVersion != cm.GetResourceVersion()) {
		return apierrors.NewConflict(gr, name, errors.New("precondition failed"))
	}
	fg := o.PropagationPolicy != nil && *o.PropagationPolicy == metav1.DeletePropagationForeground
	switch fins := cm.GetFinalizers(); {
	case cm.GetDeletionTimestamp() == nil && fg:
		now := metav1.Now()
		cm.SetDeletionTimestamp(&now)
		cm.SetFinalizers(append(fins, metav1.FinalizerDeleteDependents))
		f.bump(cm)
	case cm.GetDeletionTimestamp() == nil:
		delete(f.objs, k)
	case fg && !slices.Contains(fins, metav1.FinalizerDeleteDependents):
		// A repeated Foreground DELETE recomputes the GC finalizers.
		fins = slices.DeleteFunc(slices.Clone(fins), func(s string) bool { return s == metav1.FinalizerOrphanDependents })
		cm.SetFinalizers(append(fins, metav1.FinalizerDeleteDependents))
		f.bump(cm)
	}
	return nil
}

func (f *environmentAPIModel) bump(m metav1.Object) {
	f.rv++
	m.SetResourceVersion(strconv.Itoa(f.rv))
}

func (f *environmentAPIModel) list(res string, a ktesting.ListActionImpl) (runtime.Object, error) {
	if res != "pods" {
		return nil, fmt.Errorf("environmentAPIModel: unexpected list of %s", res)
	}
	var items []corev1.Pod
	for _, k := range slices.SortedFunc(maps.Keys(f.objs), func(a, b environmentAPIKey) int { return strings.Compare(a.name, b.name) }) {
		m, _ := meta.Accessor(f.objs[k])
		if sel := a.ListRestrictions.Labels; k.resource == res && (sel == nil || sel.Matches(labels.Set(m.GetLabels()))) {
			items = append(items, *f.objs[k].DeepCopyObject().(*corev1.Pod))
		}
	}
	start := 0
	if c := a.ListOptions.Continue; c != "" {
		n, err := strconv.Atoi(strings.TrimPrefix(c, "env-api-"))
		if err != nil || !strings.HasPrefix(c, "env-api-") || n < 0 || n > len(items) {
			return nil, apierrors.NewBadRequest("invalid continue token")
		}
		start = n
	}
	size := f.page
	if l := int(a.ListOptions.Limit); l > 0 && l < size {
		size = l
	}
	end := min(start+size, len(items))
	cont := ""
	if end < len(items) {
		cont = "env-api-" + strconv.Itoa(end)
	}
	return &corev1.PodList{ListMeta: metav1.ListMeta{Continue: cont, ResourceVersion: strconv.Itoa(f.rv)}, Items: items[start:end]}, nil
}

// edit models another writer, preserving the Pod generation rules of the
// retained source model while moving its resourceVersion.
func (f *environmentAPIModel) edit(resource, name string, fn func(runtime.Object)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, ok := f.objs[environmentAPIKey{resource, name}]
	if !ok {
		f.t.Fatalf("edit: no %s %s", resource, name)
	}
	fn(cur)
	m, _ := meta.Accessor(cur)
	f.bump(m)
}

func (f *environmentAPIModel) remove(resource, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objs, environmentAPIKey{resource, name})
}

func (f *environmentAPIModel) get(resource, name string) runtime.Object {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objs[environmentAPIKey{resource, name}]
	if !ok {
		return nil
	}
	return o.DeepCopyObject()
}
