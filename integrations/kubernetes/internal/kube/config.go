package kube

import (
	"bytes"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
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
	Data       *DataConfig       `yaml:"data,omitempty"`
}

// DataConfig enables the fixed data layout for a target. A nil value leaves the
// target's legacy behavior and binding unchanged. Images are digest-pinned
// registry references; HelperSHA256 is the expected helper executable digest.
// Origin is the canonical HTTPS worker transfer origin and CAPEM the complete
// set of PEM certificates trusted for it, without system roots.
type DataConfig struct {
	HelperImage  string          `yaml:"helper_image"`
	HelperSHA256 string          `yaml:"helper_sha256"`
	Origin       string          `yaml:"origin"`
	CAPEM        string          `yaml:"ca_pem"`
	TaskImages   []string        `yaml:"task_images"`
	References   []DataReference `yaml:"references"`

	// Qualification optionally names the target's native qualification. Its
	// file names and operator assertions are not part of the target binding.
	Qualification *DataQualification `yaml:"qualification,omitempty"`
}

// DataQualification names an operator-reviewed native qualification record
// and the build receipt of the server executable that consumes it. Both are
// private regular files, mode 0400 or 0600, owned by the server user, and read
// once when the target is constructed. Record and BuildReceipt are absolute
// paths; ReadConfig resolves relative ones against the configuration file.
//
// PolicyRevision and StoragePolicy, keyed by PVC reference name, are the
// operator's current admission, runtime and storage policy revisions. The
// Kubernetes API cannot attest them; they are trusted configuration compared
// with the record, and changing them requires a new qualification.
type DataQualification struct {
	Record         string            `yaml:"record"`
	BuildReceipt   string            `yaml:"build_receipt"`
	PolicyRevision string            `yaml:"policy_revision"`
	StoragePolicy  map[string]string `yaml:"storage_policy"`
}

// DataReference names an existing PersistentVolumeClaim or Secret in the
// target namespace that tasks may mount at one of Destinations. Exactly one of
// PVC and Secret is set. A Secret projects exactly Keys and is always read-only;
// AllowWrite permits writable PVC mounts.
type DataReference struct {
	Name         string   `yaml:"name"`
	PVC          string   `yaml:"pvc"`
	Secret       string   `yaml:"secret"`
	Keys         []string `yaml:"keys"`
	Destinations []string `yaml:"destinations"`
	AllowWrite   bool     `yaml:"allow_write"`
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
		if q := qualificationConfig(t.Data); q != nil {
			for _, name := range []*string{&q.Record, &q.BuildReceipt} {
				if *name != "" && configString(*name) && !filepath.IsAbs(*name) {
					*name = filepath.Join(base, *name)
				}
			}
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
	if err := validatePolicy(t.Resources); err != nil {
		return err
	}
	_, err := newDataProfile(t.Data)
	return err
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
		if slices.Equal(path, []string{"targets", "data", "references", "allow_write"}) {
			tag = "!!bool"
		}
		limit := 4096
		if slices.Equal(path, []string{"targets", "data", "ca_pem"}) {
			limit = configMaxCA
		}
		if n.Tag != tag || len(n.Value) > limit || !configString(n.Value) {
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
		t.Data = cloneDataConfig(t.Data)
	}
	return result
}

func cloneDataConfig(c *DataConfig) *DataConfig {
	if c == nil {
		return nil
	}
	d := *c
	d.TaskImages = slices.Clone(c.TaskImages)
	d.References = slices.Clone(c.References)
	for i := range d.References {
		r := &d.References[i]
		r.Keys = slices.Clone(r.Keys)
		r.Destinations = slices.Clone(r.Destinations)
	}
	if c.Qualification != nil {
		q := *c.Qualification
		q.StoragePolicy = maps.Clone(q.StoragePolicy)
		d.Qualification = &q
	}
	return &d
}

func qualificationConfig(c *DataConfig) *DataQualification {
	if c == nil {
		return nil
	}
	return c.Qualification
}

const configMaxCA = 64 << 10

// newDataProfile validates c and returns a frozen, sorted copy. A nil c is a
// legacy target and returns nil. Errors name fixed categories, never values.
func newDataProfile(c *DataConfig) (*dataProfile, error) {
	if c == nil {
		return nil, nil
	}
	if !configImage(c.HelperImage) {
		return nil, errors.New("invalid data helper image")
	}
	if !configHex(c.HelperSHA256) {
		return nil, errors.New("invalid data helper digest")
	}
	if len(c.TaskImages) == 0 || len(c.TaskImages) > 32 {
		return nil, errors.New("data task images must contain 1..32 entries")
	}
	images := slices.Clone(c.TaskImages)
	slices.Sort(images)
	for i, image := range images {
		if !configImage(image) {
			return nil, errors.New("invalid data task image")
		}
		if i > 0 && images[i-1] == image {
			return nil, errors.New("duplicate data task image")
		}
	}
	origin, host, ok := configOrigin(c.Origin)
	if !ok {
		return nil, errors.New("invalid data origin")
	}
	ca := []byte(c.CAPEM)
	if !configCertificates(ca) {
		return nil, errors.New("invalid data certificate authority")
	}
	if len(c.References) > 16 {
		return nil, errors.New("data references exceed 16 entries")
	}
	refs := make(map[string]dataReference, len(c.References))
	for _, r := range c.References {
		ref, err := configReference(r)
		if err != nil {
			return nil, err
		}
		if _, ok := refs[r.Name]; ok {
			return nil, errors.New("duplicate data reference name")
		}
		refs[r.Name] = ref
	}
	if err := configQualification(c.Qualification, refs); err != nil {
		return nil, err
	}
	return &dataProfile{
		helperImage: c.HelperImage, helperSHA256: c.HelperSHA256,
		origin: origin, host: host,
		caPEM: ca, caSHA256: fmt.Sprintf("%x", sha256.Sum256(ca)),
		taskImages: images, references: refs,
	}, nil
}

// configQualification validates the optional qualification settings. A
// storage policy may name only a configured PVC reference.
func configQualification(q *DataQualification, refs map[string]dataReference) error {
	if q == nil {
		return nil
	}
	for _, name := range []string{q.Record, q.BuildReceipt} {
		if name == "" || len(name) > 4096 || !configString(name) || !filepath.IsAbs(name) {
			return errors.New("data qualification files must be absolute paths")
		}
	}
	if !qualificationText(q.PolicyRevision, 128) {
		return errors.New("invalid data qualification policy revision")
	}
	if len(q.StoragePolicy) > 16 {
		return errors.New("data qualification storage policy exceeds 16 entries")
	}
	for name, revision := range q.StoragePolicy {
		if refs[name].pvc == "" || !qualificationText(revision, 128) {
			return errors.New("invalid data qualification storage policy")
		}
	}
	return nil
}

func configReference(r DataReference) (dataReference, error) {
	if len(validation.IsDNS1123Label(r.Name)) != 0 {
		return dataReference{}, errors.New("invalid data reference name")
	}
	if (r.PVC == "") == (r.Secret == "") {
		return dataReference{}, errors.New("data reference requires exactly one of pvc or secret")
	}
	ref := dataReference{pvc: r.PVC, secret: r.Secret, allowWrite: r.AllowWrite}
	if r.PVC != "" {
		if len(validation.IsDNS1123Subdomain(r.PVC)) != 0 {
			return dataReference{}, errors.New("invalid data reference claim name")
		}
		if len(r.Keys) != 0 {
			return dataReference{}, errors.New("data claim reference cannot select keys")
		}
		ref.keys = []string{}
	} else {
		if len(validation.IsDNS1123Subdomain(r.Secret)) != 0 {
			return dataReference{}, errors.New("invalid data reference secret name")
		}
		if r.AllowWrite {
			return dataReference{}, errors.New("data secret reference cannot allow write")
		}
		if len(r.Keys) == 0 || len(r.Keys) > 32 {
			return dataReference{}, errors.New("data secret reference requires 1..32 keys")
		}
		ref.keys = slices.Clone(r.Keys)
		slices.Sort(ref.keys)
		for i, key := range ref.keys {
			if len(validation.IsConfigMapKey(key)) != 0 || strings.Contains(key, "/") {
				return dataReference{}, errors.New("invalid data secret key")
			}
			if i > 0 && ref.keys[i-1] == key {
				return dataReference{}, errors.New("duplicate data secret key")
			}
		}
	}
	if len(r.Destinations) == 0 || len(r.Destinations) > 16 {
		return dataReference{}, errors.New("data reference requires 1..16 destinations")
	}
	ref.destinations = slices.Clone(r.Destinations)
	slices.Sort(ref.destinations)
	for i, dest := range ref.destinations {
		if !configDestination(dest) {
			return dataReference{}, errors.New("invalid data reference destination")
		}
		for _, p := range protectedPaths() {
			if configOverlap(dest, p) {
				return dataReference{}, errors.New("data reference destination overlaps protected path")
			}
		}
		for _, earlier := range ref.destinations[:i] {
			if configOverlap(dest, earlier) {
				return dataReference{}, errors.New("overlapping data reference destinations")
			}
		}
	}
	return ref, nil
}

var (
	configDomain    = regexp.MustCompile(`^(?:[a-z0-9]|[a-z0-9][a-z0-9-]*[a-z0-9])(?:\.(?:[a-z0-9]|[a-z0-9][a-z0-9-]*[a-z0-9]))*(?::[1-9][0-9]{0,4})?$`)
	configComponent = regexp.MustCompile(`^[a-z0-9]+(?:(?:\.|_|__|-+)[a-z0-9]+)*$`)
)

// configImage reports whether s is a canonical digest-pinned reference
// "registry/path@sha256:hex" with an explicit lowercase registry and no tag.
func configImage(s string) bool {
	name, digest, ok := strings.Cut(s, "@")
	if !ok || len(name) > 255 || !strings.HasPrefix(digest, "sha256:") || !configHex(digest[len("sha256:"):]) {
		return false
	}
	parts := strings.Split(name, "/")
	domain := parts[0]
	if len(parts) < 2 || !configDomain.MatchString(domain) || (!strings.ContainsAny(domain, ".:") && domain != "localhost") {
		return false
	}
	if _, port, ok := strings.Cut(domain, ":"); ok {
		if n, err := strconv.Atoi(port); err != nil || n > 65535 {
			return false
		}
	}
	for _, part := range parts[1:] {
		if !configComponent.MatchString(part) {
			return false
		}
	}
	return true
}

// configHex reports whether s is 64 lowercase hexadecimal digits.
func configHex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// configOrigin returns the canonical form and hostname of an HTTPS origin.
// The only accepted rewrite is removal of an explicit default port 443.
func configOrigin(s string) (origin, host string, ok bool) {
	hostport, found := strings.CutPrefix(s, "https://")
	if !found || hostport == "" {
		return "", "", false
	}
	for _, c := range hostport {
		if c <= ' ' || c >= 0x7f || strings.ContainsRune("/?#@%\\[]", c) {
			return "", "", false
		}
	}
	host, port, hasPort := strings.Cut(hostport, ":")
	if len(validation.IsDNS1123Subdomain(host)) != 0 || net.ParseIP(host) != nil {
		return "", "", false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) > 63 {
			return "", "", false
		}
	}
	// A numeric final label may be read as an IPv4 address by some resolvers.
	if last := host[strings.LastIndex(host, ".")+1:]; last[0] >= '0' && last[0] <= '9' {
		return "", "", false
	}
	origin = "https://" + host
	if hasPort {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port {
			return "", "", false
		}
		if n != 443 {
			origin += ":" + port
		}
	}
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Hostname() != host || u.Path != "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", "", false
	}
	return origin, host, true
}

// configCertificates reports whether pemBytes holds only one or more complete
// header-free CERTIFICATE blocks, separated by whitespace, that form a pool.
func configCertificates(pemBytes []byte) bool {
	if len(pemBytes) == 0 || len(pemBytes) > configMaxCA {
		return false
	}
	pool := x509.NewCertPool()
	n := 0
	for rest := pemBytes; ; {
		rest = bytes.TrimLeft(rest, " \t\r\n")
		if len(rest) == 0 {
			break
		}
		if !bytes.HasPrefix(rest, []byte("-----BEGIN ")) {
			return false
		}
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return false
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return false
		}
		pool.AddCert(cert)
		n++
	}
	return n > 0
}

// configDestination reports whether s is a canonical absolute path under
// /mnt/skygo/, using the same shape rules as backend.ValidateDataTask.
func configDestination(s string) bool {
	if !strings.HasPrefix(s, "/mnt/skygo/") || len(s) > 240 || !utf8.ValidString(s) || strings.ContainsAny(s, "\\:\x00*?[]") {
		return false
	}
	for _, c := range s {
		if c < ' ' || c == 0x7f {
			return false
		}
	}
	parts := strings.Split(s[1:], "/")
	if len(parts) > 16 {
		return false
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func configOverlap(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
