package kube

import (
	"bytes"
	"errors"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/tmc/skygo/internal/workeradmission"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const maxWorkerConfigBytes = 1 << 20

// WorkerConfig is a validated CPU worker profile for one target. It fixes the
// borrowed namespace and default service account, helper and admission budget.
// ReadWorkerConfig constructs it; the zero value configures no workers.
type WorkerConfig struct {
	target                    string
	namespaceUID              types.UID
	registry                  workerRegistry
	helperImage, helperSHA256 string
	budget                    workeradmission.Budget
}

// ReadWorkerConfig reads one strict version 1 YAML worker profile. The file
// must be a private regular file of the server's user, mode 0400 or 0600,
// without symbolic or hard links, and at most 1 MiB. It neither loads
// Kubernetes credentials nor contacts the API server.
//
// Required fields are version, target, namespace_uid, service_account_uid,
// helper_image, helper_sha256 and budget. budget requires ranks (2..2048),
// cpu_millis and memory_bytes, both positive. Optional image_pull_secrets
// lists at most 32 distinct Secret names in default service account order.
// The helper image must be digest-pinned and helper_sha256 its executable's
// 64 lowercase hexadecimal digits. Aliases, anchors, duplicate or unknown
// fields, null values and scalar coercions are refused.
func ReadWorkerConfig(name string) (cfg WorkerConfig, err error) {
	f, err := openQualificationFile(name)
	if err != nil {
		return WorkerConfig{}, errors.New("open worker profile")
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			cfg, err = WorkerConfig{}, &workerConfigCloseError{file: f, cause: errors.Join(err, closeErr)}
		}
	}()
	before, err := environmentConfigFileIdentity(f)
	if err != nil {
		return WorkerConfig{}, errors.New("worker profile must be a private regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxWorkerConfigBytes+1))
	if err != nil {
		return WorkerConfig{}, &workerConfigReadError{cause: err}
	}
	after, err := environmentConfigFileIdentity(f)
	if err != nil || before != after || int64(len(data)) != before.size {
		return WorkerConfig{}, errors.New("worker profile changed while read")
	}
	return readWorkerConfig(data)
}

// workerConfigReadError preserves the original read cause without exposing
// its possibly private file path in the returned error's text.
type workerConfigReadError struct{ cause error }

func (e *workerConfigReadError) Error() string { return "read worker profile" }
func (e *workerConfigReadError) Unwrap() error { return e.cause }

// workerConfigCloseError retains the actual failed first Close. No caller
// retries that Close or treats the file as positively released.
type workerConfigCloseError struct {
	file  *os.File
	cause error
}

func (e *workerConfigCloseError) Error() string { return "close worker profile" }
func (e *workerConfigCloseError) Unwrap() error { return e.cause }

// Target returns the configured target name, or "" for the zero config.
func (c WorkerConfig) Target() string { return c.target }

func (c WorkerConfig) clone() WorkerConfig {
	c.registry.PullSecrets = slices.Clone(c.registry.PullSecrets)
	return c
}

// check also validates manually constructed internal profiles before effects.
func (c WorkerConfig) check() error {
	switch {
	case c.target == "local" || len(validation.IsDNS1123Label(c.target)) != 0:
		return errors.New("invalid worker profile target")
	case !validInstance(string(c.namespaceUID)):
		return errors.New("invalid worker profile namespace uid")
	case !configImage(c.helperImage):
		return errors.New("worker helper image must be digest pinned")
	case !configHex(c.helperSHA256):
		return errors.New("invalid worker helper executable digest")
	case c.budget.Ranks < 2 || c.budget.Ranks > 2048 || c.budget.CPUMillis == 0 || c.budget.MemoryBytes == 0 || c.budget.Accelerators != 0:
		return errors.New("invalid cpu worker budget")
	}
	if err := c.registry.check(); err != nil {
		return errors.New("invalid worker profile service account snapshot")
	}
	return nil
}

type workerConfigDocument struct {
	Version           uint64   `yaml:"version"`
	Target            string   `yaml:"target"`
	NamespaceUID      string   `yaml:"namespace_uid"`
	ServiceAccountUID string   `yaml:"service_account_uid"`
	ImagePullSecrets  []string `yaml:"image_pull_secrets"`
	HelperImage       string   `yaml:"helper_image"`
	HelperSHA256      string   `yaml:"helper_sha256"`
	Budget            struct {
		Ranks       uint64 `yaml:"ranks"`
		CPUMillis   uint64 `yaml:"cpu_millis"`
		MemoryBytes uint64 `yaml:"memory_bytes"`
	} `yaml:"budget"`
}

func readWorkerConfig(data []byte) (WorkerConfig, error) {
	if len(data) == 0 || len(data) > maxWorkerConfigBytes {
		return WorkerConfig{}, errors.New("worker profile must be 1 byte to 1 MiB")
	}
	var tree yaml.Node
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&tree); err != nil || len(tree.Content) != 1 {
		return WorkerConfig{}, errors.New("invalid worker profile syntax")
	}
	if err := workerConfigMapping(tree.Content[0], false); err != nil {
		return WorkerConfig{}, err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		return WorkerConfig{}, errors.New("worker profile must contain one document")
	}
	var doc workerConfigDocument
	if err := tree.Decode(&doc); err != nil {
		return WorkerConfig{}, errors.New("invalid worker profile value")
	}
	if doc.Version != 1 {
		return WorkerConfig{}, errors.New("unsupported worker profile version")
	}
	cfg := WorkerConfig{
		target: doc.Target, namespaceUID: types.UID(doc.NamespaceUID),
		registry:    workerRegistry{ServiceAccountUID: types.UID(doc.ServiceAccountUID), PullSecrets: slices.Clone(doc.ImagePullSecrets)},
		helperImage: doc.HelperImage, helperSHA256: doc.HelperSHA256,
		budget: workeradmission.Budget{Ranks: doc.Budget.Ranks, CPUMillis: doc.Budget.CPUMillis, MemoryBytes: doc.Budget.MemoryBytes},
	}
	if cfg.registry.PullSecrets == nil {
		cfg.registry.PullSecrets = []string{}
	}
	if err := cfg.check(); err != nil {
		return WorkerConfig{}, err
	}
	return cfg, nil
}

// workerConfigMapping checks both structure and scalar tags before decoding.
// The only nested map is budget, so there is no recursive unbounded grammar.
func workerConfigMapping(n *yaml.Node, budget bool) error {
	if n.Kind != yaml.MappingNode || n.Tag != "!!map" || n.Anchor != "" || n.Style&yaml.TaggedStyle != 0 {
		return errors.New("worker profile must contain plain mappings")
	}
	required := []string{"version", "target", "namespace_uid", "service_account_uid", "helper_image", "helper_sha256", "budget"}
	if budget {
		required = []string{"ranks", "cpu_millis", "memory_bytes"}
	}
	seen := make(map[string]bool)
	for i := 0; i < len(n.Content); i += 2 {
		key, value := n.Content[i], n.Content[i+1]
		if !workerConfigString(key) {
			return errors.New("invalid worker profile key")
		}
		name := key.Value
		if seen[name] {
			return errors.New("duplicate worker profile field")
		}
		seen[name] = true
		if !slices.Contains(required, name) && (budget || name != "image_pull_secrets") {
			return errors.New("unknown worker profile field")
		}
		switch {
		case budget || name == "version":
			if !workerConfigInteger(value) {
				return errors.New("worker profile quantities must be decimal integers")
			}
		case name == "budget":
			if err := workerConfigMapping(value, true); err != nil {
				return err
			}
		case name == "image_pull_secrets":
			if value.Kind != yaml.SequenceNode || value.Tag != "!!seq" || value.Anchor != "" || value.Style&yaml.TaggedStyle != 0 || len(value.Content) > maxWorkerPullSecrets {
				return errors.New("invalid worker profile image pull secrets")
			}
			for _, secret := range value.Content {
				if !workerConfigString(secret) {
					return errors.New("invalid worker profile image pull secret")
				}
			}
		default:
			if !workerConfigString(value) {
				return errors.New("worker profile values must be strings")
			}
		}
	}
	for _, name := range required {
		if !seen[name] {
			return errors.New("worker profile is missing a required field")
		}
	}
	return nil
}

func workerConfigString(n *yaml.Node) bool {
	return n.Kind == yaml.ScalarNode && n.Tag == "!!str" && n.Anchor == "" &&
		(n.Style == 0 || n.Style == yaml.SingleQuotedStyle || n.Style == yaml.DoubleQuotedStyle) &&
		len(n.Value) <= 4096 && configString(n.Value)
}

func workerConfigInteger(n *yaml.Node) bool {
	return n.Kind == yaml.ScalarNode && n.Tag == "!!int" && n.Anchor == "" && n.Style == 0 &&
		n.Value != "" && (n.Value == "0" || n.Value[0] != '0') && strings.Trim(n.Value, "0123456789") == ""
}
