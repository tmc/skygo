package kube

import (
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

// FromTarget constructs a named target without contacting its API server.
// Kubeconfig must be absolute and Context explicit, unless InCluster is true.
// The target's routing, trust and policy are fixed for its lifetime.
func FromTarget(t TargetConfig, owner string) (backend.Target, error) {
	if err := validateTarget(t); err != nil {
		return backend.Target{}, err
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
		return backend.Target{}, errors.New("load kubernetes target configuration")
	}
	return fromREST(t, owner, config)
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

func fromREST(t TargetConfig, owner string, config *rest.Config) (backend.Target, error) {
	if t.Name == "local" || len(validation.IsDNS1123Label(t.Name)) != 0 {
		return backend.Target{}, errors.New("invalid kubernetes target name")
	}
	if len(validation.IsDNS1123Label(t.Namespace)) != 0 {
		return backend.Target{}, errors.New("invalid kubernetes namespace")
	}
	if owner == "" || len(validation.IsValidLabelValue(owner)) != 0 {
		return backend.Target{}, errors.New("invalid kubernetes owner")
	}
	if err := validateLabels(t.Labels); err != nil {
		return backend.Target{}, errors.New("invalid target labels")
	}
	if err := validatePolicy(t.Resources); err != nil {
		return backend.Target{}, err
	}
	if config == nil {
		return backend.Target{}, errors.New("kubernetes configuration is required")
	}
	config = rest.CopyConfig(config)
	host, err := canonicalServer(config.Host)
	if err != nil {
		return backend.Target{}, err
	}
	config.Host = host
	config.CAData = slices.Clone(config.CAData)
	if len(config.CAData) == 0 && config.CAFile != "" {
		config.CAData, err = os.ReadFile(config.CAFile)
		if err != nil {
			return backend.Target{}, errors.New("read kubernetes certificate authority")
		}
		if len(config.CAData) == 0 {
			return backend.Target{}, errors.New("empty kubernetes certificate authority")
		}
	}
	// Freeze the bytes used by both the binding and the actual transport.
	config.CAFile = ""
	t.Labels = maps.Clone(t.Labels)
	t.Resources = clonePolicy(t.Resources)
	binding, err := targetBinding(t, config)
	if err != nil {
		return backend.Target{}, err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return backend.Target{}, errors.New("create kubernetes client")
	}
	names := make([]string, 0, len(t.Resources.Accelerators))
	for name := range t.Resources.Accelerators {
		names = append(names, name)
	}
	slices.Sort(names)
	b := &Backend{client: client, namespace: t.Namespace, owner: owner, binding: binding,
		policy: t.Resources, canceled: make(map[string]bool)}
	return backend.Target{
		Name: t.Name, Kind: "kubernetes", Backend: b, Binding: binding,
		Labels: maps.Clone(t.Labels), AcceleratorNames: names,
		SupportsResourceLimits: true, SupportsNodeSelector: len(t.Resources.NodeSelectors) > 0,
	}, nil
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
