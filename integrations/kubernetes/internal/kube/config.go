package kube

import (
	"bytes"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/util/validation"
)

const maxConfigBytes = 1 << 20

// Config is a validated startup configuration. Its zero value has no targets.
// ReadConfig constructs it; Targets returns independent copies.
type Config struct{ targets []TargetConfig }

// TargetConfig describes one Kubernetes target. Labels are deliberately public
// placement metadata; connection settings and resource policy remain private.
// InCluster accepts empty Kubeconfig and Context fields, whether omitted or
// explicitly empty in YAML. File-based targets require both fields.
type TargetConfig struct {
	Name       string            `yaml:"name"`
	Kubeconfig string            `yaml:"kubeconfig"`
	Context    string            `yaml:"context"`
	Namespace  string            `yaml:"namespace"`
	InCluster  bool              `yaml:"in_cluster"`
	Labels     map[string]string `yaml:"labels"`
	Resources  ResourcePolicy    `yaml:"resources"`
}

// ResourcePolicy bounds explicitly requested resources and node selectors.
// A zero maximum adds no bound. It does not reserve capacity or supply defaults.
type ResourcePolicy struct {
	MaxCPUMillis    int64                        `yaml:"max_cpu_millis"`
	MaxMemoryBytes  int64                        `yaml:"max_memory_bytes"`
	MaxAccelerators int32                        `yaml:"max_accelerators"`
	Accelerators    map[string]AcceleratorPolicy `yaml:"accelerators"`
	NodeSelectors   map[string][]string          `yaml:"node_selectors"`
}

// AcceleratorPolicy maps a portable name to a Kubernetes extended resource.
// RequiredNodeSelector must be included explicitly by a task using this name.
type AcceleratorPolicy struct {
	ResourceName         string            `yaml:"resource_name"`
	RequiredNodeSelector map[string]string `yaml:"required_node_selector"`
}

// ReadConfig reads one strict version-1 YAML document, bounded to 1 MiB and 32
// total targets including the optional built-in local target. Relative kubeconfig
// paths resolve against the configuration file's directory. It neither expands
// environment variables nor loads credentials or contacts Kubernetes.
// Errors contain fixed categories, never configuration values or filenames.
func ReadConfig(path string, includeLocal bool) (Config, error) {
	before, err := os.Stat(path)
	if err != nil {
		return Config{}, errors.New("read targets configuration")
	}
	if !before.Mode().IsRegular() {
		return Config{}, errors.New("targets configuration must be a regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return Config{}, errors.New("read targets configuration")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(before, info) {
		return Config{}, errors.New("targets configuration must be a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxConfigBytes+1))
	if err != nil {
		return Config{}, errors.New("read targets configuration")
	}
	if len(data) > maxConfigBytes {
		return Config{}, errors.New("targets configuration exceeds 1 MiB")
	}
	base, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return Config{}, errors.New("resolve targets configuration directory")
	}
	return readConfig(data, base, includeLocal)
}

func readConfig(data []byte, base string, includeLocal bool) (Config, error) {
	if len(data) > maxConfigBytes {
		return Config{}, errors.New("targets configuration exceeds 1 MiB")
	}
	var tree yaml.Node
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&tree); err != nil {
		return Config{}, errors.New("invalid targets configuration syntax")
	}
	if len(tree.Content) != 1 || tree.Content[0].Kind != yaml.MappingNode {
		return Config{}, errors.New("targets configuration must be a mapping")
	}
	if err := strictConfigNode(&tree, 0, nil); err != nil {
		return Config{}, err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		return Config{}, errors.New("targets configuration must contain one document")
	}
	var document struct {
		Version int            `yaml:"version"`
		Targets []TargetConfig `yaml:"targets"`
	}
	decoder = yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&document); err != nil {
		return Config{}, errors.New("unknown field or invalid value in targets configuration")
	}
	if document.Version != 1 {
		return Config{}, errors.New("unsupported targets configuration version")
	}
	limit := 32
	if includeLocal {
		limit--
	}
	if len(document.Targets) == 0 || len(document.Targets) > limit {
		return Config{}, errors.New("targets configuration must contain 1..32 total targets including local")
	}
	names := make(map[string]bool)
	for i := range document.Targets {
		t := &document.Targets[i]
		if len(t.Kubeconfig) > 4096 || !configString(t.Kubeconfig) {
			return Config{}, errors.New("invalid target connection setting")
		}
		if t.Kubeconfig != "" && !filepath.IsAbs(t.Kubeconfig) {
			t.Kubeconfig = filepath.Join(base, t.Kubeconfig)
		}
		if err := validateTarget(*t); err != nil {
			return Config{}, err
		}
		if names[t.Name] {
			return Config{}, errors.New("duplicate target name")
		}
		names[t.Name] = true
	}
	return Config{targets: document.Targets}, nil
}

// validateTarget also validates direct FromTarget input. File parsing resolves
// relative kubeconfig paths before this check; constructors never resolve them.
func validateTarget(t TargetConfig) error {
	if t.Name == "local" || len(validation.IsDNS1123Label(t.Name)) != 0 {
		return errors.New("invalid or reserved target name")
	}
	if len(validation.IsDNS1123Label(t.Namespace)) != 0 {
		return errors.New("invalid target namespace")
	}
	if t.InCluster {
		if t.Kubeconfig != "" || t.Context != "" {
			return errors.New("in_cluster conflicts with kubeconfig or context")
		}
	} else if t.Kubeconfig == "" || t.Context == "" {
		return errors.New("target requires kubeconfig and context or explicit in_cluster")
	}
	if len(t.Context) > 1024 || !configString(t.Context) || len(t.Kubeconfig) > 4096 || !configString(t.Kubeconfig) {
		return errors.New("invalid target connection setting")
	}
	if t.Kubeconfig != "" && !filepath.IsAbs(t.Kubeconfig) {
		return errors.New("target kubeconfig path must be absolute")
	}
	if err := validateLabels(t.Labels); err != nil {
		return errors.New("invalid target labels")
	}
	return validatePolicy(t.Resources)
}

// strictConfigNode rejects YAML features that would hide aliases, coercion or
// duplicate policy keys before the typed decoder sees them.
func strictConfigNode(n *yaml.Node, depth int, path []string) error {
	if depth > 16 {
		return errors.New("targets configuration nesting exceeds 16")
	}
	if n.Anchor != "" || n.Kind == yaml.AliasNode {
		return errors.New("targets configuration aliases and anchors are not supported")
	}
	switch n.Kind {
	case yaml.MappingNode:
		if n.Tag != "!!map" {
			return errors.New("invalid targets configuration tag")
		}
		seen := make(map[string]bool)
		for i := 0; i < len(n.Content); i += 2 {
			key := n.Content[i]
			if key.Kind != yaml.ScalarNode || key.Anchor != "" || key.Tag != "!!str" || len(key.Value) > 256 || !configString(key.Value) {
				return errors.New("invalid targets configuration key")
			}
			if seen[key.Value] {
				return errors.New("duplicate targets configuration key")
			}
			seen[key.Value] = true
			if err := strictConfigNode(n.Content[i+1], depth+1, append(path, key.Value)); err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		if n.Tag != "!!seq" {
			return errors.New("invalid targets configuration tag")
		}
		for _, child := range n.Content {
			if err := strictConfigNode(child, depth+1, path); err != nil {
				return err
			}
		}
	case yaml.DocumentNode:
		for _, child := range n.Content {
			if err := strictConfigNode(child, depth+1, path); err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		tag := "!!str"
		if len(path) == 1 && path[0] == "version" {
			tag = "!!int"
		}
		if len(path) == 2 && path[0] == "targets" && path[1] == "in_cluster" {
			tag = "!!bool"
		}
		if len(path) == 3 && path[0] == "targets" && path[1] == "resources" {
			switch path[2] {
			case "max_cpu_millis", "max_memory_bytes", "max_accelerators":
				tag = "!!int"
			}
		}
		if n.Tag != tag || len(n.Value) > 4096 || !configString(n.Value) {
			return errors.New("invalid targets configuration scalar")
		}
	default:
		return errors.New("invalid targets configuration node")
	}
	return nil
}

func configString(s string) bool { return utf8.ValidString(s) && !strings.ContainsRune(s, 0) }
func validateLabels(labels map[string]string) error {
	if len(labels) > 32 {
		return errors.New("too many labels")
	}
	for key, value := range labels {
		if len(key) > 256 || len(validation.IsQualifiedName(key)) != 0 || len(validation.IsValidLabelValue(value)) != 0 {
			return errors.New("invalid label")
		}
	}
	return nil
}
func validatePolicy(p ResourcePolicy) error {
	if p.MaxCPUMillis < 0 || p.MaxMemoryBytes < 0 || p.MaxAccelerators < 0 {
		return errors.New("resource maxima must be nonnegative")
	}
	if len(p.Accelerators) > 32 || len(p.NodeSelectors) > 32 {
		return errors.New("resource policy exceeds 32 entries")
	}
	for key, values := range p.NodeSelectors {
		if len(key) > 256 || len(validation.IsQualifiedName(key)) != 0 || len(values) == 0 || len(values) > 32 {
			return errors.New("invalid node selector allowlist")
		}
		seen := make(map[string]bool)
		for _, value := range values {
			if len(validation.IsValidLabelValue(value)) != 0 || seen[value] {
				return errors.New("invalid node selector allowlist")
			}
			seen[value] = true
		}
	}
	for name, a := range p.Accelerators {
		if name == "" || len(validation.IsValidLabelValue(name)) != 0 {
			return errors.New("invalid accelerator name")
		}
		prefix, _, qualified := strings.Cut(a.ResourceName, "/")
		if !qualified || len(validation.IsQualifiedName(a.ResourceName)) != 0 || prefix == "kubernetes.io" || strings.HasSuffix(prefix, ".kubernetes.io") || prefix == "k8s.io" || strings.HasSuffix(prefix, ".k8s.io") || strings.HasPrefix(a.ResourceName, "requests.") {
			return errors.New("invalid accelerator extended resource name")
		}
		if err := validateLabels(a.RequiredNodeSelector); err != nil {
			return errors.New("invalid accelerator required node selector")
		}
		if name != "nvidia" && len(a.RequiredNodeSelector) == 0 {
			return errors.New("model accelerator requires a node selector")
		}
		for key, value := range a.RequiredNodeSelector {
			allowed := false
			for _, candidate := range p.NodeSelectors[key] {
				if candidate == value {
					allowed = true
					break
				}
			}
			if !allowed {
				return errors.New("accelerator required node selector is not allowed")
			}
		}
	}
	return nil
}

// Targets returns independent configuration snapshots. Mutating any returned
// map, slice or entry cannot change the configuration or another snapshot.
func (c Config) Targets() []TargetConfig {
	result := slices.Clone(c.targets)
	for i := range result {
		t := &result[i]
		t.Labels = maps.Clone(t.Labels)
		t.Resources.NodeSelectors = maps.Clone(t.Resources.NodeSelectors)
		for k, v := range t.Resources.NodeSelectors {
			t.Resources.NodeSelectors[k] = slices.Clone(v)
		}
		t.Resources.Accelerators = maps.Clone(t.Resources.Accelerators)
		for k, v := range t.Resources.Accelerators {
			v.RequiredNodeSelector = maps.Clone(v.RequiredNodeSelector)
			t.Resources.Accelerators[k] = v
		}
	}
	return result
}
