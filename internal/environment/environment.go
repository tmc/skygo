package environment

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"path"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	// SpecVersion is the version of the canonical Spec encoding.
	SpecVersion = 1

	// SpecVersion2 is the version of the canonical encoding of a Spec with
	// artifacts.
	SpecVersion2 = 2

	// FirstGeneration is the only generation this release accepts. An
	// environment is never replaced in place.
	FirstGeneration = 1

	// MaxSpecBytes bounds a canonical Spec encoding.
	MaxSpecBytes = 256 << 10

	// MaxCheckpointBytes bounds a Checkpoint's Data.
	MaxCheckpointBytes = 64 << 10

	// Port is the only port an environment may declare.
	Port = 8080

	// Idle timeouts other than zero lie in this range.
	MinIdleTimeoutSeconds = 5 * 60
	MaxIdleTimeoutSeconds = 7 * 24 * 3600

	// MaxArtifacts bounds Spec.Artifacts.
	MaxArtifacts = 8

	maxSelectors = 16

	// artifactPrefix contains every artifact mount path.
	artifactPrefix = "/skygo/artifacts/"
)

// Spec is the immutable request for one environment. Name, Target, Image,
// CPUMillis and MemoryBytes are required. The zero value of every other field
// means none; nothing is defaulted or implied. The zero Spec is invalid.
type Spec struct {
	// Name is 1..63 lowercase letters, digits or '-'. It starts with a
	// letter, does not end with '-' and is unique within its principal.
	Name string `json:"name"`
	// Target names one configured target: 1..128 printable ASCII bytes.
	Target string `json:"target"`
	// Image is 1..512 printable ASCII bytes without spaces.
	Image string `json:"image"`

	// CPUMillis and MemoryBytes are positive requests. A nonzero limit is
	// at least its request.
	CPUMillis        int64 `json:"cpu_millis"`
	MemoryBytes      int64 `json:"memory_bytes"`
	CPULimitMillis   int64 `json:"cpu_limit_millis,omitempty"`
	MemoryLimitBytes int64 `json:"memory_limit_bytes,omitempty"`

	// Accelerator, 1..256 bytes, and a positive AcceleratorCount are set
	// together.
	Accelerator      string `json:"accelerator,omitempty"`
	AcceleratorCount int32  `json:"accelerator_count,omitempty"`

	// NodeSelector has at most 16 entries. Keys are 1..256 and values
	// 0..4096 bytes of UTF-8 without NUL.
	NodeSelector map[string]string `json:"node_selector,omitempty"`

	// StorageAlias names one operator-configured writable volume of
	// Target: 1..63 lowercase letters, digits or '-', not starting or
	// ending with '-'.
	StorageAlias string `json:"storage_alias,omitempty"`

	// Port is zero or Port.
	Port int32 `json:"port,omitempty"`

	// IdleTimeoutSeconds is zero, disabling expiry, or
	// MinIdleTimeoutSeconds..MaxIdleTimeoutSeconds.
	IdleTimeoutSeconds int64 `json:"idle_timeout_seconds,omitempty"`

	// Artifacts are at most MaxArtifacts read-only inputs materialized into
	// each incarnation. Mount paths are distinct and none contains another.
	Artifacts []ArtifactMount `json:"artifacts,omitempty"`
}

// ArtifactMount is one read-only artifact input. ID is 32 and SHA256 is 64
// lowercase hexadecimal digits, and Size is positive; the Store checks that
// they name a complete artifact of the environment's principal. MountPath
// is a clean absolute path of at most 256 printable ASCII bytes, without
// spaces, strictly under /skygo/artifacts/.
type ArtifactMount struct {
	ID        string `json:"id"`
	SHA256    string `json:"sha256"`
	Size      uint64 `json:"size"`
	MountPath string `json:"mount_path"`
}

// canonicalSpec is the versioned encoding. Optional fields are omitted when
// zero, so a nil and an empty selector encode identically.
type canonicalSpec struct {
	Version int `json:"version"`
	Spec
}

// Validate reports whether s is a valid Spec.
func Validate(s Spec) error {
	_, err := Canonical(s)
	return err
}

// Canonical returns the canonical JSON encoding of s, or an error if s is
// invalid. The encoding starts with the version, SpecVersion2 exactly when s
// has artifacts, and sorts selector keys.
func Canonical(s Spec) ([]byte, error) {
	if err := check(s); err != nil {
		return nil, err
	}
	version := SpecVersion
	if len(s.Artifacts) != 0 {
		version = SpecVersion2
	}
	data, err := json.Marshal(canonicalSpec{version, s})
	if err != nil {
		return nil, fmt.Errorf("encode spec: %w", err)
	}
	if len(data) > MaxSpecBytes {
		return nil, fmt.Errorf("canonical spec exceeds %d bytes", MaxSpecBytes)
	}
	return data, nil
}

// Parse decodes a canonical encoding. It refuses any version other than
// SpecVersion and SpecVersion2 and any bytes that Canonical would not produce
// for the decoded Spec, so version 2 requires artifacts and version 1 refuses
// them.
func Parse(data []byte) (Spec, error) {
	if len(data) > MaxSpecBytes {
		return Spec{}, fmt.Errorf("canonical spec exceeds %d bytes", MaxSpecBytes)
	}
	var c canonicalSpec
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return Spec{}, fmt.Errorf("decode spec: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return Spec{}, errors.New("decode spec: trailing data")
	}
	if c.Version != SpecVersion && c.Version != SpecVersion2 {
		return Spec{}, fmt.Errorf("unsupported spec version %d", c.Version)
	}
	want, err := Canonical(c.Spec)
	if err != nil {
		return Spec{}, err
	}
	if !bytes.Equal(want, data) {
		return Spec{}, errors.New("spec is not canonical")
	}
	return Normalize(c.Spec)
}

// Normalize checks s and returns a copy that shares no memory with s. An
// empty selector or artifact list becomes nil.
func Normalize(s Spec) (Spec, error) {
	if err := check(s); err != nil {
		return Spec{}, err
	}
	s = s.Clone()
	if len(s.NodeSelector) == 0 {
		s.NodeSelector = nil
	}
	if len(s.Artifacts) == 0 {
		s.Artifacts = nil
	}
	return s, nil
}

// Clone returns a copy of s that shares no memory with s.
func (s Spec) Clone() Spec {
	s.NodeSelector = maps.Clone(s.NodeSelector)
	s.Artifacts = slices.Clone(s.Artifacts)
	return s
}

func check(s Spec) error {
	switch {
	case !ValidName(s.Name):
		return errors.New("invalid environment name")
	case !ValidTarget(s.Target):
		return errors.New("invalid target")
	case !ascii(s.Image, 512) || strings.ContainsRune(s.Image, ' '):
		return errors.New("invalid image")
	case s.CPUMillis <= 0 || s.MemoryBytes <= 0:
		return errors.New("cpu and memory requests must be positive")
	case s.CPULimitMillis < 0 || s.CPULimitMillis != 0 && s.CPULimitMillis < s.CPUMillis,
		s.MemoryLimitBytes < 0 || s.MemoryLimitBytes != 0 && s.MemoryLimitBytes < s.MemoryBytes:
		return errors.New("resource limit is less than its request")
	case (s.Accelerator == "") != (s.AcceleratorCount == 0) || s.AcceleratorCount < 0 || s.Accelerator != "" && !text(s.Accelerator, 256, false):
		return errors.New("accelerator name and positive count required together")
	case len(s.NodeSelector) > maxSelectors:
		return fmt.Errorf("node selector exceeds %d entries", maxSelectors)
	case s.StorageAlias != "" && !validAlias(s.StorageAlias):
		return errors.New("invalid storage alias")
	case s.Port != 0 && s.Port != Port:
		return fmt.Errorf("port must be 0 or %d", Port)
	case s.IdleTimeoutSeconds != 0 && (s.IdleTimeoutSeconds < MinIdleTimeoutSeconds || s.IdleTimeoutSeconds > MaxIdleTimeoutSeconds):
		return fmt.Errorf("idle timeout outside %d..%d seconds", MinIdleTimeoutSeconds, MaxIdleTimeoutSeconds)
	case len(s.Artifacts) > MaxArtifacts:
		return fmt.Errorf("artifacts exceed %d entries", MaxArtifacts)
	}
	for k, v := range s.NodeSelector {
		if !text(k, 256, false) || !text(v, 4096, true) {
			return errors.New("invalid node selector")
		}
	}
	for i, m := range s.Artifacts {
		if !ValidID(m.ID) || !lowerHex(m.SHA256, 64) || m.Size == 0 || !validMountPath(m.MountPath) {
			return errors.New("invalid artifact mount")
		}
		for _, n := range s.Artifacts[:i] {
			if n.MountPath == m.MountPath || strings.HasPrefix(n.MountPath, m.MountPath+"/") || strings.HasPrefix(m.MountPath, n.MountPath+"/") {
				return errors.New("artifact mount paths overlap")
			}
		}
	}
	return nil
}

// validMountPath reports whether p is a clean absolute path strictly under
// artifactPrefix. The path is a POSIX path inside the incarnation, never a
// host path.
func validMountPath(p string) bool {
	return ascii(p, 256) && !strings.ContainsRune(p, ' ') && path.Clean(p) == p && strings.HasPrefix(p, artifactPrefix) && len(p) > len(artifactPrefix)
}

// Desired is the durable intent for an environment. The zero value is
// invalid.
type Desired uint8

const (
	Present Desired = iota + 1
	Absent
)

func (d Desired) String() string {
	switch d {
	case Present:
		return "present"
	case Absent:
		return "absent"
	}
	return fmt.Sprintf("Desired(%d)", uint8(d))
}

// Phase is the last recorded phase of an environment's resources. The zero
// value is invalid.
type Phase uint8

const (
	// Creating precedes the first matching observation.
	Creating Phase = iota + 1
	// Ready is a fresh matching provider observation of the environment's
	// own resources. It is not evidence of agent execution or service
	// capacity.
	Ready
	// Deleting follows a recorded Absent intent.
	Deleting
	// Deleted is a qualified cleanup result. It is final.
	Deleted
	// Unknown keeps every reservation and proves nothing about the
	// environment's resources.
	Unknown
)

func (p Phase) String() string {
	switch p {
	case Creating:
		return "creating"
	case Ready:
		return "ready"
	case Deleting:
		return "deleting"
	case Deleted:
		return "deleted"
	case Unknown:
		return "unknown"
	}
	return fmt.Sprintf("Phase(%d)", uint8(p))
}

// State is an environment's desired state and last recorded phase. A new
// environment is Present and Creating.
type State struct {
	Desired Desired `json:"desired"`
	Phase   Phase   `json:"phase"`
}

// Valid reports whether s is a state a record may hold. Deleting and Deleted
// require Absent.
func (s State) Valid() bool {
	if s.Desired != Present && s.Desired != Absent {
		return false
	}
	switch s.Phase {
	case Creating, Ready, Unknown:
		return true
	case Deleting, Deleted:
		return s.Desired == Absent
	}
	return false
}

// Next reports an error unless a record may move from s to t. Absent never
// returns to Present, Creating is never reentered, Deleted is reached only
// from Deleting and never left.
func (s State) Next(t State) error {
	if !s.Valid() || !t.Valid() {
		return errors.New("invalid environment state")
	}
	if s.Desired == Absent && t.Desired == Present {
		return errors.New("absent environment cannot become present")
	}
	ok := false
	switch s.Phase {
	case Creating:
		ok = t.Phase != Deleted
	case Ready, Unknown:
		ok = t.Phase == Ready || t.Phase == Deleting || t.Phase == Unknown
	case Deleting:
		ok = t.Phase == Deleting || t.Phase == Deleted || t.Phase == Unknown
	case Deleted:
		ok = t.Phase == Deleted
	}
	if !ok {
		return fmt.Errorf("environment cannot move from %v to %v", s.Phase, t.Phase)
	}
	return nil
}

// Identity names one environment. Installation is the owning installation's
// identity, 1..128 printable ASCII bytes. ID is 32 lowercase hexadecimal
// digits and Generation is FirstGeneration. A native instance or Pod identity
// never substitutes for it.
type Identity struct {
	Installation string `json:"installation"`
	ID           string `json:"id"`
	Generation   uint64 `json:"generation"`
}

// Valid reports whether i is a valid Identity.
func (i Identity) Valid() bool {
	return ascii(i.Installation, 128) && ValidID(i.ID) && i.Generation == FirstGeneration
}

// ValidID reports whether s is 32 lowercase hexadecimal digits.
func ValidID(s string) bool { return lowerHex(s, 32) }

func lowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := range len(s) {
		if !('0' <= s[i] && s[i] <= '9' || 'a' <= s[i] && s[i] <= 'f') {
			return false
		}
	}
	return true
}

// Checkpoint is opaque, server-only provider state. Kind is 1..63 lowercase
// letters, digits or '-', not starting or ending with '-'; Version is
// positive; Data is 1..MaxCheckpointBytes bytes. Kind and Version are fixed
// when an environment is accepted. Data must never hold credentials or
// tokens.
type Checkpoint struct {
	Kind    string `json:"kind"`
	Version uint32 `json:"version"`
	Data    []byte `json:"data"`
}

// Validate reports whether c is a valid Checkpoint. It does not interpret
// Data.
func (c Checkpoint) Validate() error {
	switch {
	case !validAlias(c.Kind):
		return errors.New("invalid checkpoint kind")
	case c.Version == 0:
		return errors.New("invalid checkpoint version")
	case len(c.Data) == 0:
		return errors.New("empty checkpoint")
	case len(c.Data) > MaxCheckpointBytes:
		return fmt.Errorf("checkpoint exceeds %d bytes", MaxCheckpointBytes)
	}
	return nil
}

// Clone returns a copy of c that shares no memory with c.
func (c Checkpoint) Clone() Checkpoint {
	c.Data = bytes.Clone(c.Data)
	return c
}

// ValidName reports whether s is a valid Spec.Name: 1..63 lowercase letters,
// digits or '-', starting with a letter and not ending with '-'.
func ValidName(s string) bool {
	return len(s) > 0 && 'a' <= s[0] && s[0] <= 'z' && validAlias(s)
}

// ValidTarget reports whether s is a valid Spec.Target: 1..128 printable
// ASCII bytes, space included.
func ValidTarget(s string) bool { return ascii(s, 128) }

func validAlias(s string) bool {
	if len(s) == 0 || len(s) > 63 || s[0] == '-' || s[len(s)-1] == '-' {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if !('a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

func ascii(s string, max int) bool {
	if len(s) == 0 || len(s) > max {
		return false
	}
	for i := range len(s) {
		if s[i] < 32 || s[i] > 126 {
			return false
		}
	}
	return true
}

func text(s string, max int, empty bool) bool {
	return (empty || s != "") && len(s) <= max && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
