package kube

import (
	"context"
	"errors"
	"maps"
	"os"
	"slices"

	"github.com/tmc/skygo/internal/backend"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// FromTarget constructs a named target. Kubeconfig must be absolute and
// Context explicit, unless InCluster is true. The target's routing, trust and
// policy are fixed for its lifetime.
//
// Without a data qualification, FromTarget does not contact the API server.
// With one, it loads the record and build receipt, validates them against the
// actual executable and target, and before returning reads the namespace,
// server version, qualified node pool and recorded PVCs under ctx. Any
// mismatch, failure or uncertain response body refuses the target. Only such
// a target reports remote data support.
func FromTarget(ctx context.Context, t TargetConfig, owner string) (backend.Target, error) {
	target, _, err := loadTarget(ctx, t, owner)
	return target, err
}

// loadTarget is FromTarget that also returns the frozen REST configuration
// the target was built from: its canonical server and CA bytes. The caller
// must not change it.
func loadTarget(ctx context.Context, t TargetConfig, owner string) (backend.Target, *rest.Config, error) {
	if err := validateTarget(t); err != nil {
		return backend.Target{}, nil, err
	}
	var config *rest.Config
	var err error
	if t.InCluster {
		config, err = rest.InClusterConfig()
	} else {
		rules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: t.Kubeconfig}
		config, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: t.Context}).ClientConfig()
	}
	if err != nil {
		return backend.Target{}, nil, errors.New("load kubernetes target configuration")
	}
	return fromRESTFrozen(ctx, t, owner, config)
}

// FromConfig constructs the legacy target named kubernetes. It uses in-cluster
// credentials when both connection arguments are empty; otherwise it retains
// the kubeconfig loading rules and optional context override. NVIDIA resources
// use nvidia.com/gpu. No node selectors are allowed by this legacy policy.
func FromConfig(kubeconfig, contextName, namespace, owner string) (backend.Target, error) {
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
		return backend.Target{}, errors.New("load kubernetes target configuration")
	}
	return fromREST(TargetConfig{
		Name: "kubernetes", Namespace: namespace,
		Resources: ResourcePolicy{Accelerators: map[string]AcceleratorPolicy{"nvidia": {ResourceName: "nvidia.com/gpu"}}},
	}, owner, config)
}

// fromREST constructs a target that makes no API request, as FromConfig
// requires. It refuses a configured data qualification.
func fromREST(t TargetConfig, owner string, config *rest.Config) (backend.Target, error) {
	if qualificationConfig(t.Data) != nil {
		return backend.Target{}, errors.New("kubernetes data qualification requires a caller context")
	}
	return fromRESTContext(context.Background(), t, owner, config)
}

// fromRESTContext constructs a target from a loaded REST configuration. Only
// a configured data qualification makes requests, under ctx.
func fromRESTContext(ctx context.Context, t TargetConfig, owner string, config *rest.Config) (backend.Target, error) {
	target, _, err := fromRESTFrozen(ctx, t, owner, config)
	return target, err
}

// fromRESTFrozen is fromRESTContext that also returns the frozen copy of
// config that built the target's client and binding. The caller must not
// change it.
func fromRESTFrozen(ctx context.Context, t TargetConfig, owner string, config *rest.Config) (backend.Target, *rest.Config, error) {
	if t.Name == "local" || len(validation.IsDNS1123Label(t.Name)) != 0 {
		return backend.Target{}, nil, errors.New("invalid kubernetes target name")
	}
	if len(validation.IsDNS1123Label(t.Namespace)) != 0 {
		return backend.Target{}, nil, errors.New("invalid kubernetes namespace")
	}
	if owner == "" || len(validation.IsValidLabelValue(owner)) != 0 {
		return backend.Target{}, nil, errors.New("invalid kubernetes owner")
	}
	if err := validateLabels(t.Labels); err != nil {
		return backend.Target{}, nil, errors.New("invalid target labels")
	}
	if err := validatePolicy(t.Resources); err != nil {
		return backend.Target{}, nil, err
	}
	if config == nil {
		return backend.Target{}, nil, errors.New("kubernetes configuration is required")
	}
	config = rest.CopyConfig(config)
	host, err := canonicalServer(config.Host)
	if err != nil {
		return backend.Target{}, nil, err
	}
	config.Host = host
	config.CAData = slices.Clone(config.CAData)
	if len(config.CAData) == 0 && config.CAFile != "" {
		config.CAData, err = os.ReadFile(config.CAFile)
		if err != nil {
			return backend.Target{}, nil, errors.New("read kubernetes certificate authority")
		}
		if len(config.CAData) == 0 {
			return backend.Target{}, nil, errors.New("empty kubernetes certificate authority")
		}
	}
	// Freeze the bytes used by both the binding and the actual transport.
	config.CAFile = ""
	data, err := newDataProfile(t.Data)
	if err != nil {
		return backend.Target{}, nil, err
	}
	t.Labels = maps.Clone(t.Labels)
	t.Resources = clonePolicy(t.Resources)
	t.Data = cloneDataConfig(t.Data)
	client, err := newClient(config, data != nil)
	if err != nil {
		return backend.Target{}, nil, errors.New("create kubernetes client")
	}
	q, err := qualifyTarget(ctx, t, config, data, client)
	if err != nil {
		return backend.Target{}, nil, err
	}
	// The record bound the binding without q; the target's binding adds it.
	binding, err := targetBinding(t, config, data, q)
	if err != nil {
		return backend.Target{}, nil, err
	}
	names := make([]string, 0, len(t.Resources.Accelerators))
	for name := range t.Resources.Accelerators {
		names = append(names, name)
	}
	slices.Sort(names)
	b := &Backend{client: client, namespace: t.Namespace, owner: owner, binding: binding,
		policy: t.Resources, canceled: make(map[string]bool), data: data, dataQual: q}
	var policy *backend.DataPolicy
	if data != nil {
		p := backend.DefaultDataPolicy()
		policy = &p
	}
	return backend.Target{
		Name: t.Name, Kind: "kubernetes", Backend: b, Binding: binding,
		Labels: maps.Clone(t.Labels), AcceleratorNames: names,
		SupportsResourceLimits: true, SupportsNodeSelector: len(t.Resources.NodeSelectors) > 0,
		SupportsRetry: true,
		OutputStorage: backend.RemoteOutput,
		Retention:     &backend.Retention{RemoveExecution: b.removeExecution},
		DataPolicy:    policy,
		// Data support comes only from an installed qualification. Staged
		// retry is not qualified in this phase.
		SupportsRemoteData:      q != nil,
		SupportsDataMounts:      q != nil && slices.ContainsFunc(q.record.Images, func(i qualificationImage) bool { return len(i.Mounts) != 0 }),
		SupportsRemoteDataRetry: false,
	}, config, nil
}

// newClient builds the clientset as kubernetes.NewForConfig does. For a data
// target, the data transport wraps the complete HTTP transport, outside the
// authentication wrappers, so that a request refused after an uncertain body
// Close causes no credential refresh either. Untagged requests pass through
// unchanged.
func newClient(config *rest.Config, data bool) (*kubernetes.Clientset, error) {
	if !data {
		return kubernetes.NewForConfig(config)
	}
	c := *config
	if c.UserAgent == "" {
		c.UserAgent = rest.DefaultKubernetesUserAgent()
	}
	hc, err := rest.HTTPClientFor(&c)
	if err != nil {
		return nil, err
	}
	hc.Transport = &dataTransport{base: hc.Transport}
	return kubernetes.NewForConfigAndClient(&c, hc)
}

func clonePolicy(p ResourcePolicy) ResourcePolicy {
	p.Accelerators = maps.Clone(p.Accelerators)
	for name, a := range p.Accelerators {
		a.RequiredNodeSelector = maps.Clone(a.RequiredNodeSelector)
		p.Accelerators[name] = a
	}
	p.NodeSelectors = maps.Clone(p.NodeSelectors)
	for name, values := range p.NodeSelectors {
		values = slices.Clone(values)
		slices.Sort(values)
		p.NodeSelectors[name] = values
	}
	return p
}
