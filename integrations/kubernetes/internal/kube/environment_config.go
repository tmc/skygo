package kube

import (
	"bytes"
	"errors"
	"io"
	"slices"

	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Bounds of an environment profile file.
const (
	maxEnvironmentConfigBytes = 1 << 20
	maxEnvironmentProfiles    = 32
)

// EnvironmentConfig is a validated environment profile file: the operator's
// private grants of borrowed storage to principals on one target. Its zero
// value configures no environments; ReadEnvironmentConfig constructs it.
type EnvironmentConfig struct {
	target    string
	artifacts bool
	profiles  []environmentPodProfile
}

// environmentConfigDocument is the version 1 profile file. Each profile is
// an explicit grant: the principal may place environments on the named
// existing claim under the storage alias. Nothing in it is derived from a
// request or from what the cluster holds. Mount path and user are fixed.
//
//	version: 1
//	artifacts: false
//	profiles:
//	- target: gpu-a
//	  revision: r1
//	  principal: alice
//	  storage_alias: home
//	  namespace_uid: 6f1c...
//	  image: registry.example/env@sha256:...
//	  idle_argv: [/bin/sleep, infinity]
//	  pvc_name: alice-home
//	  pvc_uid: 0b3e...
//	  volume_name: pv-alice-home
//	  volume_uid: 7a40...
//	  access_mode: ReadWriteOncePod
//	  service_account_uid: 91d2...
//	  image_pull_secrets: [registry]
type environmentConfigDocument struct {
	Version   int                       `yaml:"version"`
	Artifacts bool                      `yaml:"artifacts"`
	Profiles  []environmentConfigRecord `yaml:"profiles"`
}

type environmentConfigRecord struct {
	Target            string   `yaml:"target"`
	Revision          string   `yaml:"revision"`
	Principal         string   `yaml:"principal"`
	StorageAlias      string   `yaml:"storage_alias"`
	NamespaceUID      string   `yaml:"namespace_uid"`
	Image             string   `yaml:"image"`
	IdleArgv          []string `yaml:"idle_argv"`
	PVCName           string   `yaml:"pvc_name"`
	PVCUID            string   `yaml:"pvc_uid"`
	VolumeName        string   `yaml:"volume_name"`
	VolumeUID         string   `yaml:"volume_uid"`
	AccessMode        string   `yaml:"access_mode"`
	ServiceAccountUID string   `yaml:"service_account_uid"`
	ImagePullSecrets  []string `yaml:"image_pull_secrets"`
}

// ReadEnvironmentConfig reads one strict version 1 YAML profile file: a
// regular file, not a symbolic link, owned by the server's user, mode 0400
// or 0600, of at most 1 MiB. It holds 1..32 profiles, unique by principal
// and storage alias, all of one target; artifacts enables artifact mounts
// on that target. Every profile requires volume_uid, the UID of the claim's
// bound PersistentVolume. It neither loads credentials nor contacts Kubernetes.
// Errors are fixed categories, never file names or configuration values.
func ReadEnvironmentConfig(name string) (cfg EnvironmentConfig, err error) {
	f, err := openQualificationFile(name)
	if err != nil {
		return EnvironmentConfig{}, errors.New("open environment profiles")
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			cfg, err = EnvironmentConfig{}, errors.New("close environment profiles")
		}
	}()
	before, err := environmentConfigFileIdentity(f)
	if err != nil {
		return EnvironmentConfig{}, err
	}
	data, err := io.ReadAll(io.LimitReader(f, maxEnvironmentConfigBytes+1))
	if err != nil {
		return EnvironmentConfig{}, errors.New("read environment profiles")
	}
	after, err := environmentConfigFileIdentity(f)
	if err != nil || before != after || int64(len(data)) != before.size {
		return EnvironmentConfig{}, errors.New("environment profiles changed while read")
	}
	return readEnvironmentConfig(data)
}

func readEnvironmentConfig(data []byte) (EnvironmentConfig, error) {
	if len(data) == 0 || len(data) > maxEnvironmentConfigBytes {
		return EnvironmentConfig{}, errors.New("environment profiles must be 1 byte to 1 MiB")
	}
	var tree yaml.Node
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&tree); err != nil {
		return EnvironmentConfig{}, errors.New("invalid environment profiles syntax")
	}
	if len(tree.Content) != 1 || tree.Content[0].Kind != yaml.MappingNode {
		return EnvironmentConfig{}, errors.New("environment profiles must be a mapping")
	}
	if err := strictEnvironmentNode(&tree, 0, nil); err != nil {
		return EnvironmentConfig{}, err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		return EnvironmentConfig{}, errors.New("environment profiles must contain one document")
	}
	var doc environmentConfigDocument
	decoder = yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil {
		return EnvironmentConfig{}, errors.New("unknown field or invalid value in environment profiles")
	}
	if doc.Version != 1 {
		return EnvironmentConfig{}, errors.New("unsupported environment profiles version")
	}
	if len(doc.Profiles) == 0 || len(doc.Profiles) > maxEnvironmentProfiles {
		return EnvironmentConfig{}, errors.New("environment profiles must contain 1..32 profiles")
	}
	cfg := EnvironmentConfig{target: doc.Profiles[0].Target, artifacts: doc.Artifacts}
	if cfg.target == "local" || len(validation.IsDNS1123Label(cfg.target)) != 0 {
		return EnvironmentConfig{}, errors.New("invalid environment profile target")
	}
	seen := make(map[environmentProfileKey]bool)
	for _, r := range doc.Profiles {
		if r.Target != cfg.target {
			return EnvironmentConfig{}, errors.New("environment profiles name more than one target")
		}
		if !environmentPrincipal(r.Principal) {
			return EnvironmentConfig{}, errors.New("invalid environment profile principal")
		}
		k := environmentProfileKey{r.Principal, r.StorageAlias}
		if seen[k] {
			return EnvironmentConfig{}, errors.New("duplicate environment profile principal and storage alias")
		}
		seen[k] = true
		if r.VolumeUID == "" {
			return EnvironmentConfig{}, errors.New("environment profile volume uid is required")
		}
		var mode corev1.PersistentVolumeAccessMode
		switch r.AccessMode {
		case string(corev1.ReadWriteOnce), string(corev1.ReadWriteOncePod):
			mode = corev1.PersistentVolumeAccessMode(r.AccessMode)
		default:
			return EnvironmentConfig{}, errors.New("invalid environment profile access mode")
		}
		p := environmentPodProfile{
			revision:          r.Revision,
			namespaceUID:      types.UID(r.NamespaceUID),
			principal:         r.Principal,
			image:             r.Image,
			idleArgv:          slices.Clone(r.IdleArgv),
			alias:             r.StorageAlias,
			pvcName:           r.PVCName,
			pvcUID:            types.UID(r.PVCUID),
			volumeName:        r.VolumeName,
			volumeUID:         types.UID(r.VolumeUID),
			accessMode:        mode,
			mountPath:         workspaceMount,
			serviceAccountUID: types.UID(r.ServiceAccountUID),
			pullSecrets:       slices.Clone(r.ImagePullSecrets),
		}
		if p.pullSecrets == nil {
			p.pullSecrets = []string{}
		}
		if err := p.check(p.principal); err != nil {
			return EnvironmentConfig{}, err
		}
		cfg.profiles = append(cfg.profiles, p)
	}
	return cfg, nil
}

// podProfiles returns independent copies of c's profiles.
func (c EnvironmentConfig) podProfiles() []environmentPodProfile {
	out := slices.Clone(c.profiles)
	for i := range out {
		out[i].idleArgv = slices.Clone(out[i].idleArgv)
		out[i].pullSecrets = slices.Clone(out[i].pullSecrets)
	}
	return out
}

// strictEnvironmentNode rejects YAML features that would hide aliases,
// coercion or duplicate keys before the typed decoder sees them.
func strictEnvironmentNode(n *yaml.Node, depth int, path []string) error {
	if depth > 8 {
		return errors.New("environment profiles nesting exceeds 8")
	}
	if n.Anchor != "" || n.Kind == yaml.AliasNode {
		return errors.New("environment profiles aliases and anchors are not supported")
	}
	switch n.Kind {
	case yaml.DocumentNode:
		for _, child := range n.Content {
			if err := strictEnvironmentNode(child, depth+1, path); err != nil {
				return err
			}
		}
	case yaml.MappingNode:
		if n.Tag != "!!map" {
			return errors.New("invalid environment profiles tag")
		}
		seen := make(map[string]bool)
		for i := 0; i < len(n.Content); i += 2 {
			key := n.Content[i]
			if key.Kind != yaml.ScalarNode || key.Anchor != "" || key.Tag != "!!str" || len(key.Value) > 64 || !configString(key.Value) {
				return errors.New("invalid environment profiles key")
			}
			if seen[key.Value] {
				return errors.New("duplicate environment profiles key")
			}
			seen[key.Value] = true
			if err := strictEnvironmentNode(n.Content[i+1], depth+1, append(slices.Clip(path), key.Value)); err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		if n.Tag != "!!seq" {
			return errors.New("invalid environment profiles tag")
		}
		for _, child := range n.Content {
			if err := strictEnvironmentNode(child, depth+1, path); err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		tag := "!!str"
		switch {
		case slices.Equal(path, []string{"version"}):
			tag = "!!int"
		case slices.Equal(path, []string{"artifacts"}):
			tag = "!!bool"
		}
		if n.Tag != tag || len(n.Value) > maxEnvironmentArgBytes || !configString(n.Value) {
			return errors.New("invalid environment profiles scalar")
		}
	default:
		return errors.New("invalid environment profiles node")
	}
	return nil
}
