package serving

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/artifact"
)

// Spec is the caller-owned definition bound into one revision. It has no raw
// native object fields. Zero optional fields select the defaults documented on
// their types. Mounts are always read-only. Workdir and Setup describe an
// artifact-backed service; a Runtime that does not support them refuses the
// spec in Check.
type Spec struct {
	Image        string        `json:"image"`
	Argv         []string      `json:"argv"`
	Env          []EnvVar      `json:"env,omitempty"`
	Port         int32         `json:"port"`
	Readiness    Readiness     `json:"readiness"`
	Resources    Resources     `json:"resources"`
	NodeSelector []Label       `json:"node_selector,omitempty"`
	Mounts       []Mount       `json:"mounts,omitempty"`
	Rollout      RolloutPolicy `json:"rollout"`
	Scaling      Scaling       `json:"scaling"`
	Workdir      *artifact.Ref `json:"workdir,omitempty"`
	Setup        []string      `json:"setup,omitempty"`
}

// EnvVar is one plain environment variable. Its value is visible to the
// service owner and must not carry secrets.
type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Label is one node selector requirement.
type Label struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Mount selects an operator-defined data alias, mounted read-only below
// /mnt/skygo/. It is not native volume authority.
type Mount struct {
	Reference   string `json:"reference"`
	Destination string `json:"destination"`
}

// Readiness is an HTTP GET probe of Path on the spec's port. Zero
// PeriodSeconds, TimeoutSeconds and FailureThreshold select 10, 15 and 3.
type Readiness struct {
	Path                string `json:"path"`
	PeriodSeconds       int32  `json:"period_seconds,omitempty"`
	TimeoutSeconds      int32  `json:"timeout_seconds,omitempty"`
	FailureThreshold    int32  `json:"failure_threshold,omitempty"`
	InitialDelaySeconds int32  `json:"initial_delay_seconds,omitempty"`
}

// Resources are per-replica quantities. Requests are required and a zero
// limit means none. A target translates them to its own resource policy.
type Resources struct {
	CPUMillis        int64 `json:"cpu_millis"`
	MemoryBytes      int64 `json:"memory_bytes"`
	CPULimitMillis   int64 `json:"cpu_limit_millis,omitempty"`
	MemoryLimitBytes int64 `json:"memory_limit_bytes,omitempty"`
}

// RolloutPolicy bounds one rollout. Zero fields select a 600 second progress
// deadline and a 30 second termination grace period.
type RolloutPolicy struct {
	ProgressDeadlineSeconds int32 `json:"progress_deadline_seconds,omitempty"`
	TerminationGraceSeconds int32 `json:"termination_grace_seconds,omitempty"`
}

// Scaling is the revision's replica policy: a fixed count from 1 to 8.
// A rollback restores the policy of its revision.
type Scaling struct {
	Replicas int32 `json:"replicas"`
}

const (
	maxReplicas    = 8
	maxArgs        = 64
	maxArgBytes    = 16 << 10
	maxEnv         = 64
	maxEnvBytes    = 32 << 10
	maxLabels      = 16
	maxMounts      = 16
	maxCPUMillis   = 1 << 20
	maxMemoryBytes = 1 << 44
	maxSpecBytes   = 64 << 10
	specFormat     = "skygo-serving-spec-v1"
)

// Normalize returns a copy of s with defaults applied, Env sorted by Name,
// NodeSelector by Key and Mounts by Destination, or an error if any field is
// outside the portable bounds. It does not check target capability, alias
// authorization or artifact access, and it never rewrites a nonzero value.
func (s Spec) Normalize() (Spec, error) {
	n := s.clone()
	r := &n.Readiness
	r.PeriodSeconds = orDefault(r.PeriodSeconds, 10)
	r.TimeoutSeconds = orDefault(r.TimeoutSeconds, 15)
	r.FailureThreshold = orDefault(r.FailureThreshold, 3)
	n.Rollout.ProgressDeadlineSeconds = orDefault(n.Rollout.ProgressDeadlineSeconds, 600)
	n.Rollout.TerminationGraceSeconds = orDefault(n.Rollout.TerminationGraceSeconds, 30)
	slices.SortFunc(n.Env, func(a, b EnvVar) int { return strings.Compare(a.Name, b.Name) })
	slices.SortFunc(n.NodeSelector, func(a, b Label) int { return strings.Compare(a.Key, b.Key) })
	slices.SortFunc(n.Mounts, func(a, b Mount) int { return strings.Compare(a.Destination, b.Destination) })
	if err := n.validate(); err != nil {
		return Spec{}, err
	}
	if len(n.canonical()) > maxSpecBytes {
		return Spec{}, errors.New("spec exceeds 64 KiB encoded")
	}
	return n, nil
}

// Digest returns the lowercase hexadecimal SHA-256 of the canonical encoding
// of the normalized spec. Specs that differ only in defaults or in the order
// of Env, NodeSelector or Mounts have equal digests.
func (s Spec) Digest() (string, error) {
	n, err := s.Normalize()
	if err != nil {
		return "", err
	}
	return n.digest(), nil
}

func (s Spec) digest() string {
	sum := sha256.Sum256(s.canonical())
	return hex.EncodeToString(sum[:])
}

// clone copies every slice and the workdir so that s shares no memory.
func (s Spec) clone() Spec {
	s.Argv = slices.Clone(s.Argv)
	s.Env = slices.Clone(s.Env)
	s.NodeSelector = slices.Clone(s.NodeSelector)
	s.Mounts = slices.Clone(s.Mounts)
	s.Setup = slices.Clone(s.Setup)
	if s.Workdir != nil {
		w := *s.Workdir
		s.Workdir = &w
	}
	return s
}

func orDefault(v, def int32) int32 {
	if v == 0 {
		return def
	}
	return v
}

func (s Spec) validate() error {
	if !validImage(s.Image) {
		return errors.New("image must be a digest-pinned reference")
	}
	if len(s.Argv) == 0 || s.Argv[0] == "" {
		return errors.New("argv requires a program")
	}
	if err := validArgs(s.Argv); err != nil {
		return fmt.Errorf("argv: %w", err)
	}
	if len(s.Env) > maxEnv {
		return errors.New("env exceeds 64 variables")
	}
	size := 0
	for i, v := range s.Env {
		if !envName.MatchString(v.Name) {
			return errors.New("invalid environment name")
		}
		if i > 0 && v.Name == s.Env[i-1].Name {
			return errors.New("duplicate environment name")
		}
		if !validText(v.Value) {
			return errors.New("invalid environment value")
		}
		size += len(v.Name) + len(v.Value)
	}
	if size > maxEnvBytes {
		return errors.New("env exceeds 32 KiB")
	}
	if s.Port < 1 || s.Port > 65535 {
		return errors.New("port outside 1..65535")
	}
	if err := s.Readiness.validate(); err != nil {
		return fmt.Errorf("readiness: %w", err)
	}
	if err := s.Resources.validate(); err != nil {
		return fmt.Errorf("resources: %w", err)
	}
	if len(s.NodeSelector) > maxLabels {
		return errors.New("node selector exceeds 16 labels")
	}
	for i, l := range s.NodeSelector {
		if !labelKey(l.Key) || !labelValue(l.Value) {
			return errors.New("invalid node selector label")
		}
		if i > 0 && l.Key == s.NodeSelector[i-1].Key {
			return errors.New("duplicate node selector key")
		}
	}
	if len(s.Mounts) > maxMounts {
		return errors.New("mounts exceeds 16 entries")
	}
	for i, m := range s.Mounts {
		if !alias.MatchString(m.Reference) {
			return errors.New("invalid mount reference alias")
		}
		if !absolutePath(m.Destination) || !strings.HasPrefix(m.Destination, "/mnt/skygo/") {
			return errors.New("invalid mount destination")
		}
		for _, earlier := range s.Mounts[:i] {
			if overlap(m.Destination, earlier.Destination) {
				return errors.New("overlapping mount destinations")
			}
		}
	}
	if d := s.Rollout.ProgressDeadlineSeconds; d < 1 || d > 3600 {
		return errors.New("progress deadline outside 1..3600 seconds")
	}
	if g := s.Rollout.TerminationGraceSeconds; g < 1 || g > 120 {
		return errors.New("termination grace outside 1..120 seconds")
	}
	if r := s.Scaling.Replicas; r < 1 || r > maxReplicas {
		return errors.New("replicas outside 1..8")
	}
	if s.Workdir != nil {
		if err := s.Workdir.Validate(); err != nil {
			return fmt.Errorf("workdir: %w", err)
		}
	}
	if len(s.Setup) > 0 {
		if s.Workdir == nil {
			return errors.New("setup requires workdir")
		}
		if s.Setup[0] == "" {
			return errors.New("setup requires a program")
		}
		if err := validArgs(s.Setup); err != nil {
			return fmt.Errorf("setup: %w", err)
		}
	}
	return nil
}

func (r Readiness) validate() error {
	if len(r.Path) == 0 || len(r.Path) > 256 || r.Path[0] != '/' {
		return errors.New("path must begin with / and fit 256 bytes")
	}
	for i := 0; i < len(r.Path); i++ {
		if c := r.Path[i]; c <= ' ' || c > '~' || c == '#' {
			return errors.New("invalid path byte")
		}
	}
	switch {
	case r.PeriodSeconds < 1 || r.PeriodSeconds > 60:
		return errors.New("period outside 1..60 seconds")
	case r.TimeoutSeconds < 1 || r.TimeoutSeconds > 60:
		return errors.New("timeout outside 1..60 seconds")
	case r.FailureThreshold < 1 || r.FailureThreshold > 10:
		return errors.New("failure threshold outside 1..10")
	case r.InitialDelaySeconds < 0 || r.InitialDelaySeconds > 1200:
		return errors.New("initial delay outside 0..1200 seconds")
	}
	return nil
}

func (r Resources) validate() error {
	if r.CPUMillis < 1 || r.CPUMillis > maxCPUMillis || r.MemoryBytes < 1 || r.MemoryBytes > maxMemoryBytes {
		return errors.New("cpu and memory requests must be positive and bounded")
	}
	if r.CPULimitMillis != 0 && (r.CPULimitMillis < r.CPUMillis || r.CPULimitMillis > maxCPUMillis) {
		return errors.New("cpu limit below request or unbounded")
	}
	if r.MemoryLimitBytes != 0 && (r.MemoryLimitBytes < r.MemoryBytes || r.MemoryLimitBytes > maxMemoryBytes) {
		return errors.New("memory limit below request or unbounded")
	}
	return nil
}

func validArgs(args []string) error {
	if len(args) > maxArgs {
		return errors.New("exceeds 64 arguments")
	}
	size := 0
	for _, a := range args {
		if !validText(a) {
			return errors.New("invalid argument")
		}
		size += len(a)
	}
	if size > maxArgBytes {
		return errors.New("exceeds 16 KiB")
	}
	return nil
}

func validText(s string) bool { return utf8.ValidString(s) && !strings.ContainsRune(s, 0) }

var (
	envName        = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	alias          = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
	labelName      = regexp.MustCompile(`^[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$`)
	dnsSubdomain   = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?)*$`)
	imageDomain    = regexp.MustCompile(`^(?:[a-z0-9]|[a-z0-9][a-z0-9-]*[a-z0-9])(?:\.(?:[a-z0-9]|[a-z0-9][a-z0-9-]*[a-z0-9]))*(?::[1-9][0-9]{0,4})?$`)
	imageComponent = regexp.MustCompile(`^[a-z0-9]+(?:(?:\.|_|__|-+)[a-z0-9]+)*$`)
)

func labelKey(s string) bool {
	prefix, name, ok := strings.Cut(s, "/")
	if !ok {
		return labelName.MatchString(s)
	}
	return len(prefix) <= 253 && dnsSubdomain.MatchString(prefix) && labelName.MatchString(name)
}

func labelValue(s string) bool { return s == "" || labelName.MatchString(s) }

// validImage reports whether s is a canonical digest-pinned reference
// "registry/path@sha256:hex" with an explicit lowercase registry and no tag.
func validImage(s string) bool {
	name, digest, ok := strings.Cut(s, "@")
	if !ok || len(name) > 255 || !strings.HasPrefix(digest, "sha256:") || !lowerHex(digest[len("sha256:"):], 64) {
		return false
	}
	parts := strings.Split(name, "/")
	domain := parts[0]
	if len(parts) < 2 || !imageDomain.MatchString(domain) || (!strings.ContainsAny(domain, ".:") && domain != "localhost") {
		return false
	}
	if _, port, ok := strings.Cut(domain, ":"); ok {
		if n, err := strconv.Atoi(port); err != nil || n > 65535 {
			return false
		}
	}
	for _, part := range parts[1:] {
		if !imageComponent.MatchString(part) {
			return false
		}
	}
	return true
}

func lowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func absolutePath(s string) bool {
	if len(s) < 2 || len(s) > 240 || !utf8.ValidString(s) || s[0] != '/' || strings.ContainsAny(s, "\\:\x00*?[]") {
		return false
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

func overlap(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

// canonical returns the binding encoding of a normalized spec: a format name
// followed by every field in declaration order. Strings and lists carry a
// big-endian uint64 length, integers are big-endian 64-bit two's complement,
// and the optional workdir has a presence byte.
func (s Spec) canonical() []byte {
	var e encoder
	e.str(specFormat)
	e.str(s.Image)
	e.strs(s.Argv)
	e.num(int64(len(s.Env)))
	for _, v := range s.Env {
		e.str(v.Name)
		e.str(v.Value)
	}
	e.num(int64(s.Port))
	r := s.Readiness
	e.str(r.Path)
	e.num(int64(r.PeriodSeconds))
	e.num(int64(r.TimeoutSeconds))
	e.num(int64(r.FailureThreshold))
	e.num(int64(r.InitialDelaySeconds))
	e.num(s.Resources.CPUMillis)
	e.num(s.Resources.MemoryBytes)
	e.num(s.Resources.CPULimitMillis)
	e.num(s.Resources.MemoryLimitBytes)
	e.num(int64(len(s.NodeSelector)))
	for _, l := range s.NodeSelector {
		e.str(l.Key)
		e.str(l.Value)
	}
	e.num(int64(len(s.Mounts)))
	for _, m := range s.Mounts {
		e.str(m.Reference)
		e.str(m.Destination)
	}
	e.num(int64(s.Rollout.ProgressDeadlineSeconds))
	e.num(int64(s.Rollout.TerminationGraceSeconds))
	e.num(int64(s.Scaling.Replicas))
	if s.Workdir == nil {
		e.b = append(e.b, 0)
	} else {
		e.b = append(e.b, 1)
		e.str(s.Workdir.ID)
		e.str(s.Workdir.SHA256)
		e.num(s.Workdir.Size)
	}
	e.strs(s.Setup)
	return e.b
}

type encoder struct{ b []byte }

func (e *encoder) num(v int64) { e.b = binary.BigEndian.AppendUint64(e.b, uint64(v)) }

func (e *encoder) str(s string) {
	e.num(int64(len(s)))
	e.b = append(e.b, s...)
}

func (e *encoder) strs(list []string) {
	e.num(int64(len(list)))
	for _, s := range list {
		e.str(s)
	}
}
