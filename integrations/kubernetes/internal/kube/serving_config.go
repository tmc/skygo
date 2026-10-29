package kube

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"strings"

	"github.com/tmc/skygo/internal/store"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Bounds of a service profile file.
const (
	maxServiceConfigBytes = 1 << 20
	maxServiceGrants      = 64
	maxServiceGrantImages = 16
)

// ServiceConfig is a validated service profile file: the operator's private
// grants of images to principals on one target, the borrowed namespace and
// service account every service runs under, per-replica resource ceilings
// and the target's admission budgets. Its zero value configures no
// services; ReadServiceConfig constructs it.
type ServiceConfig struct {
	target   string
	profile  serviceProfile
	limits   store.ServiceLimits
	grants   map[string][]string // principal: digest-pinned images, sorted
	revision string
}

// serviceProfile is the borrowed identity and the bounds every service of
// the target is rendered with.
type serviceProfile struct {
	namespaceUID      types.UID
	serviceAccountUID types.UID
	pullSecrets       []string
	cpuMillis         int64 // per-replica request and limit ceiling
	memoryBytes       int64
}

// serviceConfigDocument is the version 1 service profile file.
//
//	version: 1
//	target: gpu-a
//	revision: r1
//	namespace_uid: 6f1c...
//	service_account_uid: 91d2...
//	image_pull_secrets: [registry]
//	ceilings: {cpu_millis: 4000, memory_bytes: 8589934592}
//	limits:
//	  principal_services: 4
//	  installation_services: 32
//	  replicas: 64
//	  cpu_millis: 16000
//	  memory_bytes: 34359738368
//	grants:
//	- principal: alice
//	  images: [registry.example/web@sha256:...]
type serviceConfigDocument struct {
	Version           int                  `yaml:"version"`
	Target            string               `yaml:"target"`
	Revision          string               `yaml:"revision"`
	NamespaceUID      string               `yaml:"namespace_uid"`
	ServiceAccountUID string               `yaml:"service_account_uid"`
	ImagePullSecrets  []string             `yaml:"image_pull_secrets"`
	Ceilings          serviceConfigCeiling `yaml:"ceilings"`
	Limits            serviceConfigLimits  `yaml:"limits"`
	Grants            []serviceConfigGrant `yaml:"grants"`
}

type serviceConfigCeiling struct {
	CPUMillis   int64 `yaml:"cpu_millis"`
	MemoryBytes int64 `yaml:"memory_bytes"`
}

type serviceConfigLimits struct {
	PrincipalServices    uint64 `yaml:"principal_services"`
	InstallationServices uint64 `yaml:"installation_services"`
	Replicas             uint64 `yaml:"replicas"`
	CPUMillis            uint64 `yaml:"cpu_millis"`
	MemoryBytes          uint64 `yaml:"memory_bytes"`
}

type serviceConfigGrant struct {
	Principal string   `yaml:"principal"`
	Images    []string `yaml:"images"`
}

// ReadServiceConfig reads one strict version 1 YAML service profile file,
// with the file rules of ReadEnvironmentConfig: a regular file, not a
// symbolic link, owned by the server's user, mode 0400 or 0600, of at most
// 1 MiB. It names one target and 1..64 grants, unique by principal, each of
// 1..16 digest-pinned images. Ceilings and the limits' CPU and memory
// budgets are required. It neither loads credentials nor contacts
// Kubernetes. Errors are fixed categories, never file names or values.
func ReadServiceConfig(name string) (cfg ServiceConfig, err error) {
	f, err := openQualificationFile(name)
	if err != nil {
		return ServiceConfig{}, errors.New("open service profiles")
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			cfg, err = ServiceConfig{}, errors.New("close service profiles")
		}
	}()
	before, err := environmentConfigFileIdentity(f)
	if err != nil {
		return ServiceConfig{}, errors.New("service profiles must be a private regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxServiceConfigBytes+1))
	if err != nil {
		return ServiceConfig{}, errors.New("read service profiles")
	}
	after, err := environmentConfigFileIdentity(f)
	if err != nil || before != after || int64(len(data)) != before.size {
		return ServiceConfig{}, errors.New("service profiles changed while read")
	}
	return readServiceConfig(data)
}

func readServiceConfig(data []byte) (ServiceConfig, error) {
	if len(data) == 0 || len(data) > maxServiceConfigBytes {
		return ServiceConfig{}, errors.New("service profiles must be 1 byte to 1 MiB")
	}
	var tree yaml.Node
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&tree); err != nil {
		return ServiceConfig{}, errors.New("invalid service profiles syntax")
	}
	if len(tree.Content) != 1 || tree.Content[0].Kind != yaml.MappingNode {
		return ServiceConfig{}, errors.New("service profiles must be a mapping")
	}
	if err := strictServiceNode(&tree, 0, nil); err != nil {
		return ServiceConfig{}, err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		return ServiceConfig{}, errors.New("service profiles must contain one document")
	}
	var doc serviceConfigDocument
	decoder = yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil {
		return ServiceConfig{}, errors.New("unknown field or invalid value in service profiles")
	}
	switch {
	case doc.Version != 1:
		return ServiceConfig{}, errors.New("unsupported service profiles version")
	case doc.Target == "local" || len(validation.IsDNS1123Label(doc.Target)) != 0:
		return ServiceConfig{}, errors.New("invalid service profile target")
	case doc.Revision == "" || len(doc.Revision) > 64:
		return ServiceConfig{}, errors.New("invalid service profile revision")
	case !uidString(doc.NamespaceUID) || !uidString(doc.ServiceAccountUID):
		return ServiceConfig{}, errors.New("service profile namespace and service account uids are required")
	case len(doc.ImagePullSecrets) > maxEnvironmentPullSecrets:
		return ServiceConfig{}, errors.New("too many service profile image pull secrets")
	case doc.Ceilings.CPUMillis < 1 || doc.Ceilings.MemoryBytes < 1:
		return ServiceConfig{}, errors.New("service profile ceilings are required")
	case len(doc.Grants) == 0 || len(doc.Grants) > maxServiceGrants:
		return ServiceConfig{}, errors.New("service profiles must contain 1..64 grants")
	}
	for _, s := range doc.ImagePullSecrets {
		if len(validation.IsDNS1123Subdomain(s)) != 0 {
			return ServiceConfig{}, errors.New("invalid service profile image pull secret")
		}
	}
	limits := store.ServiceLimits{
		PrincipalServices:    doc.Limits.PrincipalServices,
		InstallationServices: doc.Limits.InstallationServices,
		Replicas:             doc.Limits.Replicas,
		CPUMillis:            doc.Limits.CPUMillis,
		MemoryBytes:          doc.Limits.MemoryBytes,
	}
	if err := limits.Validate(); err != nil {
		return ServiceConfig{}, errors.New("invalid service profile limits")
	}
	cfg := ServiceConfig{
		target:   doc.Target,
		revision: doc.Revision,
		limits:   limits,
		grants:   make(map[string][]string, len(doc.Grants)),
		profile: serviceProfile{
			namespaceUID:      types.UID(doc.NamespaceUID),
			serviceAccountUID: types.UID(doc.ServiceAccountUID),
			pullSecrets:       slices.Clone(doc.ImagePullSecrets),
			cpuMillis:         doc.Ceilings.CPUMillis,
			memoryBytes:       doc.Ceilings.MemoryBytes,
		},
	}
	if cfg.profile.pullSecrets == nil {
		cfg.profile.pullSecrets = []string{}
	}
	for _, g := range doc.Grants {
		if !environmentPrincipal(g.Principal) {
			return ServiceConfig{}, errors.New("invalid service grant principal")
		}
		if _, dup := cfg.grants[g.Principal]; dup {
			return ServiceConfig{}, errors.New("duplicate service grant principal")
		}
		if len(g.Images) == 0 || len(g.Images) > maxServiceGrantImages {
			return ServiceConfig{}, errors.New("service grants must name 1..16 images")
		}
		images := slices.Clone(g.Images)
		for _, image := range images {
			if !pinnedImage(image) {
				return ServiceConfig{}, errors.New("service grant images must be digest pinned")
			}
		}
		slices.Sort(images)
		cfg.grants[g.Principal] = slices.Compact(images)
	}
	return cfg, nil
}

// Target returns the configured target name, or "" for the zero config.
func (c ServiceConfig) Target() string { return c.target }

// granted reports whether principal may run image.
func (c ServiceConfig) granted(principal, image string) bool {
	_, ok := slices.BinarySearch(c.grants[principal], image)
	return ok
}

// uidString reports whether s is a plausible Kubernetes UID: 1..128 bytes
// of letters, digits and '-'.
func uidString(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	return strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-") == ""
}

// pinnedImage reports whether s names an image by sha256 digest, without a
// tag. serving.Spec.Normalize checks the full reference grammar.
func pinnedImage(s string) bool {
	name, digest, ok := strings.Cut(s, "@sha256:")
	if !ok || name == "" || len(digest) != 64 || strings.Trim(digest, "0123456789abcdef") != "" {
		return false
	}
	last := name[strings.LastIndexByte(name, '/')+1:]
	return strings.Contains(name, "/") && !strings.Contains(last, ":")
}

// serviceIntPaths are the scalar paths that hold integers.
var serviceIntPaths = [][]string{
	{"version"},
	{"ceilings", "cpu_millis"}, {"ceilings", "memory_bytes"},
	{"limits", "principal_services"}, {"limits", "installation_services"}, {"limits", "replicas"},
	{"limits", "cpu_millis"}, {"limits", "memory_bytes"},
}

// strictServiceNode rejects YAML features that would hide aliases,
// coercion or duplicate keys before the typed decoder sees them.
func strictServiceNode(n *yaml.Node, depth int, path []string) error {
	if depth > 8 {
		return errors.New("service profiles nesting exceeds 8")
	}
	if n.Anchor != "" || n.Kind == yaml.AliasNode {
		return errors.New("service profiles aliases and anchors are not supported")
	}
	switch n.Kind {
	case yaml.DocumentNode:
		for _, child := range n.Content {
			if err := strictServiceNode(child, depth+1, path); err != nil {
				return err
			}
		}
	case yaml.MappingNode:
		if n.Tag != "!!map" {
			return errors.New("invalid service profiles tag")
		}
		seen := make(map[string]bool)
		for i := 0; i < len(n.Content); i += 2 {
			key := n.Content[i]
			if key.Kind != yaml.ScalarNode || key.Anchor != "" || key.Tag != "!!str" || len(key.Value) > 64 || !configString(key.Value) {
				return errors.New("invalid service profiles key")
			}
			if seen[key.Value] {
				return errors.New("duplicate service profiles key")
			}
			seen[key.Value] = true
			if err := strictServiceNode(n.Content[i+1], depth+1, append(slices.Clip(path), key.Value)); err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		if n.Tag != "!!seq" {
			return errors.New("invalid service profiles tag")
		}
		for _, child := range n.Content {
			if err := strictServiceNode(child, depth+1, path); err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		tag := "!!str"
		if slices.ContainsFunc(serviceIntPaths, func(p []string) bool { return slices.Equal(p, path) }) {
			tag = "!!int"
		}
		if n.Tag != tag || len(n.Value) > maxEnvironmentArgBytes || !configString(n.Value) {
			return errors.New("invalid service profiles scalar")
		}
	default:
		return errors.New("invalid service profiles node")
	}
	return nil
}
