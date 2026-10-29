package ec2job

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/integrations/ssh/sshagent"
	"github.com/tmc/skygo/skycompat/tasklower"
	"github.com/tmc/skygo/skycompat/taskyaml"
	"google.golang.org/protobuf/encoding/protojson"
)

// Bounds on configuration and tasks.
const (
	configVersion = 1
	maxConfig     = 64 << 10
	maxTaskFile   = 1 << 20
	maxTaskBytes  = 64 << 10 // deterministic task encoding kept in the journal
	maxTimeout    = 7 * 24 * time.Hour
	minReserve    = time.Minute
)

// Architecture is the only VM architecture this package runs jobs on: the
// agent sshagent builds is linux/arm64.
const Architecture = "arm64"

var (
	instanceIDRE = regexp.MustCompile(`^i-([0-9a-f]{8}|[0-9a-f]{17})$`)
	imageIDRE    = regexp.MustCompile(`^ami-([0-9a-f]{8}|[0-9a-f]{17})$`)
	typeRE       = regexp.MustCompile(`^[a-z][a-z0-9-]*\.[a-z0-9]+$`)
	targetRE     = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
)

// A Config is the operator's private execution configuration for jobs on
// one already-owned running EC2 VM. It binds the VM's identity and image,
// the address an SSH session may use, the SSH host and client keys, the
// agent's source and target, and the operator-initialized installation
// the agent serves. Nothing in a task can change it.
//
// The image is bound by the operator's statement that it is arm64:
// architecture is never inferred from an instance type or image name.
type Config struct {
	InstanceID   string
	ImageID      string
	InstanceType string
	Architecture string // always Architecture

	// Public selects the VM's public address, otherwise its private one.
	// IP is the address the operator expects; a fresh description must
	// report exactly it. SSHPort is the SSH server's port.
	Public  bool
	IP      netip.Addr
	SSHPort int

	// CPUMillis and MemoryBytes are the VM's capacity as the operator
	// declares it. A task requesting more is refused.
	CPUMillis   int64
	MemoryBytes int64

	// SafetyDeadline is the absolute time by which the operator's
	// independent safety mechanism may terminate the VM, and
	// CleanupReserve the time kept before it for collecting outputs. A
	// task's timeout must end before SafetyDeadline less CleanupReserve.
	SafetyDeadline time.Time
	CleanupReserve time.Duration

	// SSH and agent settings, as sshagent.Config names them.
	User           string
	KnownHostsFile string
	KeyFile        string
	AgentSocket    string
	Fingerprint    string
	Source         string
	AllowDirty     bool

	// Prebuilt selects a local Linux arm64 agent instead of Source. All three
	// fields must be set, Source empty and AllowDirty false. ParseConfig checks
	// its shape; sshagent.Open verifies the binary before connecting.
	Prebuilt       sshagent.Binary
	IdentityDir    string
	CacheDir       string
	Target         string
	AgentPort      int
	RestartStopped bool

	Installation    sshagent.Installation
	ScopedTokenFile string

	// Shell runs SkyPilot run and setup text. Infra, if set, is the one
	// resources.infra value a SkyPilot task may name; it binds to Target.
	// MaxTimeout bounds every task's timeout.
	Shell      string
	Infra      string
	MaxTimeout time.Duration

	// Digest is the hex SHA-256 of the configuration's bytes. A job
	// journal binds it; a job is never continued under another Config.
	Digest string
}

type wireConfig struct {
	Version int `json:"version"`
	VM      struct {
		InstanceID            string `json:"instance_id"`
		ImageID               string `json:"image_id"`
		InstanceType          string `json:"instance_type"`
		Architecture          string `json:"architecture"`
		Address               string `json:"address"`
		IP                    string `json:"ip"`
		CPUMillis             int64  `json:"cpu_millis"`
		MemoryBytes           int64  `json:"memory_bytes"`
		SafetyDeadline        string `json:"safety_deadline"`
		CleanupReserveSeconds int64  `json:"cleanup_reserve_seconds"`
	} `json:"vm"`
	SSH struct {
		Port           int    `json:"port"`
		User           string `json:"user"`
		KnownHostsFile string `json:"known_hosts_file"`
		KeyFile        string `json:"key_file"`
		AgentSocket    string `json:"agent_socket"`
		Fingerprint    string `json:"fingerprint"`
	} `json:"ssh"`
	Agent struct {
		Source         string          `json:"source"`
		AllowDirty     bool            `json:"allow_dirty"`
		IdentityDir    string          `json:"identity_dir"`
		CacheDir       string          `json:"cache_dir"`
		Target         string          `json:"target"`
		Port           int             `json:"port"`
		RestartStopped bool            `json:"restart_stopped"`
		Prebuilt       json.RawMessage `json:"prebuilt,omitempty"`
	} `json:"agent"`
	Installation struct {
		DataDir         string `json:"data_dir"`
		AuthFile        string `json:"auth_file"`
		ArtifactDir     string `json:"artifact_dir"`
		ScopedTokenFile string `json:"scoped_token_file"`
	} `json:"installation"`
	Task struct {
		Shell             string `json:"shell"`
		Infra             string `json:"infra"`
		MaxTimeoutSeconds int64  `json:"max_timeout_seconds"`
	} `json:"task"`
}

// ParseConfig parses a version 1 JSON execution configuration of at most
// 64 KiB:
//
//	{
//		"version": 1,
//		"vm": {"instance_id": ..., "image_id": ..., "instance_type": ...,
//			"architecture": "arm64", "address": "private" or "public", "ip": ...,
//			"cpu_millis": ..., "memory_bytes": ...,
//			"safety_deadline": RFC 3339 time, "cleanup_reserve_seconds": ...},
//		"ssh": {"port": ..., "user": ..., "known_hosts_file": ...,
//			"key_file": ... or "agent_socket": ... and "fingerprint": ...},
//		"agent": {"source": ..., "allow_dirty": ..., "identity_dir": ...,
//			"cache_dir": ..., "target": ..., "port": ..., "restart_stopped": ...},
//		"installation": {"data_dir": ..., "auth_file": ..., "artifact_dir": ...,
//			"scoped_token_file": ...},
//		"task": {"shell": ..., "infra": ..., "max_timeout_seconds": ...}
//	}
//
// In agent, a prebuilt object with path, commit and sha256 replaces source.
// All three strings are required; source must be empty and allow_dirty false.
// Commit is the operator's assertion, not an attestation of source provenance.
//
// It refuses unknown fields, trailing data, any architecture but arm64,
// and an installation that is not complete: every job is a workspace job
// on an operator-initialized installation. Errors never quote the input.
// ParseConfig reads no file the configuration names; sshagent.Open checks
// those before it connects.
func ParseConfig(r io.Reader) (*Config, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxConfig+1))
	if err != nil {
		return nil, fmt.Errorf("job config: %w", err)
	}
	if len(data) > maxConfig {
		return nil, fmt.Errorf("job config exceeds %d bytes", maxConfig)
	}
	var w wireConfig
	if err := decodeStrict(data, &w); err != nil {
		return nil, errors.New("job config: not a JSON object of the version 1 fields")
	}
	if w.Version != configVersion {
		return nil, fmt.Errorf("job config version %d is not supported", w.Version)
	}
	// The second counts are bounded before they are multiplied: a
	// count outside 0..604800 could wrap to a plausible Duration.
	const maxSeconds = int64(maxTimeout / time.Second)
	if s := w.VM.CleanupReserveSeconds; s < 0 || s > maxSeconds {
		return nil, errors.New("job config: vm cleanup_reserve_seconds must be in 0..604800")
	}
	if s := w.Task.MaxTimeoutSeconds; s < 0 || s > maxSeconds {
		return nil, errors.New("job config: task max_timeout_seconds must be in 0..604800")
	}
	sum := sha256.Sum256(data)
	c := &Config{
		InstanceID:      w.VM.InstanceID,
		ImageID:         w.VM.ImageID,
		InstanceType:    w.VM.InstanceType,
		Architecture:    w.VM.Architecture,
		CPUMillis:       w.VM.CPUMillis,
		MemoryBytes:     w.VM.MemoryBytes,
		CleanupReserve:  time.Duration(w.VM.CleanupReserveSeconds) * time.Second,
		SSHPort:         w.SSH.Port,
		User:            w.SSH.User,
		KnownHostsFile:  w.SSH.KnownHostsFile,
		KeyFile:         w.SSH.KeyFile,
		AgentSocket:     w.SSH.AgentSocket,
		Fingerprint:     w.SSH.Fingerprint,
		Source:          w.Agent.Source,
		AllowDirty:      w.Agent.AllowDirty,
		IdentityDir:     w.Agent.IdentityDir,
		CacheDir:        w.Agent.CacheDir,
		Target:          w.Agent.Target,
		AgentPort:       w.Agent.Port,
		RestartStopped:  w.Agent.RestartStopped,
		Installation:    sshagent.Installation{DataDir: w.Installation.DataDir, AuthFile: w.Installation.AuthFile, ArtifactDir: w.Installation.ArtifactDir},
		ScopedTokenFile: w.Installation.ScopedTokenFile,
		Shell:           w.Task.Shell,
		Infra:           w.Task.Infra,
		MaxTimeout:      time.Duration(w.Task.MaxTimeoutSeconds) * time.Second,
		Digest:          hex.EncodeToString(sum[:]),
	}
	if len(w.Agent.Prebuilt) != 0 {
		var p struct {
			Path   string `json:"path"`
			Commit string `json:"commit"`
			SHA256 string `json:"sha256"`
		}
		if err := decodeStrict(w.Agent.Prebuilt, &p); err != nil || p.Path == "" || p.Commit == "" || p.SHA256 == "" {
			return nil, errors.New("job config: agent prebuilt requires path, commit and sha256")
		}
		c.Prebuilt = sshagent.Binary{Path: p.Path, Commit: p.Commit, Digest: p.SHA256}
	}
	switch w.VM.Address {
	case "public":
		c.Public = true
	case "private":
	default:
		return nil, errors.New("job config: vm address must be \"public\" or \"private\"")
	}
	if c.IP, err = netip.ParseAddr(w.VM.IP); err != nil || c.IP.String() != w.VM.IP || c.IP.Zone() != "" || c.IP.Is4In6() || !c.IP.IsGlobalUnicast() {
		return nil, errors.New("job config: vm ip must be the canonical text of a unicast address")
	}
	if c.SafetyDeadline, err = time.Parse(time.RFC3339, w.VM.SafetyDeadline); err != nil || w.VM.SafetyDeadline != c.SafetyDeadline.UTC().Format(time.RFC3339) {
		return nil, errors.New("job config: vm safety_deadline must be an RFC 3339 UTC time in whole seconds")
	}
	if err := c.check(); err != nil {
		return nil, err
	}
	return c, nil
}

// LoadConfig reads and parses, as [ParseConfig] does, the configuration in
// path, a private regular file owned by the effective user, with mode
// 0600 and one link. If the file's Close fails, the error wraps
// [ErrUnsettled] and keeps the original file; the caller must keep the
// error.
func LoadConfig(path string) (*Config, error) {
	return loadConfig(path, nil)
}

// loadConfig is LoadConfig with hook called on the file just before its
// one Close.
func loadConfig(path string, hook func(*os.File)) (*Config, error) {
	data, err := readPrivate(path, maxConfig, hook)
	if err != nil {
		return nil, fmt.Errorf("job config: %w", err)
	}
	return ParseConfig(bytes.NewReader(data))
}

// check reports the first problem with c. Errors are fixed text.
func (c *Config) check() error {
	switch {
	case !instanceIDRE.MatchString(c.InstanceID):
		return errors.New("job config: vm instance_id is malformed")
	case !imageIDRE.MatchString(c.ImageID):
		return errors.New("job config: vm image_id is malformed")
	case len(c.InstanceType) > 64 || !typeRE.MatchString(c.InstanceType):
		return errors.New("job config: vm instance_type is malformed")
	case c.Architecture != Architecture:
		return errors.New("job config: vm architecture must be arm64; the agent is built for linux/arm64 only")
	case c.Public && c.IP.IsPrivate():
		return errors.New("job config: vm ip does not match the selected address kind")
	case c.CPUMillis <= 0 || c.MemoryBytes <= 0:
		return errors.New("job config: vm cpu_millis and memory_bytes must be positive")
	case c.CleanupReserve < minReserve:
		return errors.New("job config: vm cleanup_reserve_seconds must be at least 60")
	case c.SSHPort < 1 || c.SSHPort > 65535:
		return errors.New("job config: ssh port must be in 1..65535")
	case c.User == "":
		return errors.New("job config: ssh user is required")
	case !targetRE.MatchString(c.Target):
		return errors.New("job config: agent target must be a lowercase DNS name")
	case c.Installation.IsZero() || c.ScopedTokenFile == "":
		return errors.New("job config: an operator-initialized installation and scoped token file are required")
	case !cleanAbs(c.Shell) || c.Shell == "/":
		return errors.New("job config: task shell must be a clean absolute path")
	case c.Infra != "" && (strings.TrimSpace(c.Infra) != c.Infra || strings.ContainsAny(c.Infra, "\x00\r\n")):
		return errors.New("job config: task infra is malformed")
	case c.MaxTimeout < time.Second:
		return errors.New("job config: task max_timeout_seconds must be positive")
	}
	if c.Prebuilt == (sshagent.Binary{}) {
		if !cleanAbs(c.Source) {
			return errors.New("job config: agent source must be clean and absolute")
		}
	} else {
		if c.Source != "" || c.AllowDirty {
			return errors.New("job config: agent prebuilt and source build options are exclusive")
		}
		if !cleanAbs(c.Prebuilt.Path) || !commitRE.MatchString(c.Prebuilt.Commit) || !hex64.MatchString(c.Prebuilt.Digest) {
			return errors.New("job config: agent prebuilt needs a clean absolute path, full lowercase commit and sha256")
		}
	}
	for _, p := range []string{c.KnownHostsFile, c.IdentityDir, c.CacheDir, c.ScopedTokenFile} {
		if !cleanAbs(p) {
			return errors.New("job config: every local path must be clean and absolute")
		}
	}
	return nil
}

func cleanAbs(name string) bool { return filepath.IsAbs(name) && filepath.Clean(name) == name }

// Session returns the sshagent configuration that reaches the VM at addr,
// which the caller has verified is the address c selects. Log receives
// sshagent's progress.
func (c *Config) Session(addr netip.Addr, log io.Writer) sshagent.Config {
	return sshagent.Config{
		Address:         netip.AddrPortFrom(addr, uint16(c.SSHPort)).String(),
		User:            c.User,
		KnownHostsFile:  c.KnownHostsFile,
		KeyFile:         c.KeyFile,
		AgentSocket:     c.AgentSocket,
		Fingerprint:     c.Fingerprint,
		Source:          c.Source,
		AllowDirty:      c.AllowDirty,
		Prebuilt:        c.Prebuilt,
		IdentityDir:     c.IdentityDir,
		CacheDir:        c.CacheDir,
		Target:          c.Target,
		Port:            c.AgentPort,
		RestartStopped:  c.RestartStopped,
		Installation:    c.Installation,
		ScopedTokenFile: c.ScopedTokenFile,
		Log:             log,
	}
}

// checkTimeout reports whether a task of timeout seconds started at now
// ends no later than the VM's safety deadline less the cleanup reserve.
// The count is bounded by MaxTimeout, at most 604800 seconds, before it
// is multiplied, so the Duration cannot overflow.
func (c *Config) checkTimeout(now time.Time, seconds int64) error {
	switch {
	case seconds <= 0:
		return errors.New("task timeout must be finite and positive")
	case seconds > int64(c.MaxTimeout/time.Second):
		return errors.New("task timeout exceeds the configured maximum")
	}
	d := time.Duration(seconds) * time.Second
	if now.Add(d).After(c.SafetyDeadline.Add(-c.CleanupReserve)) {
		return errors.New("task timeout does not end before the VM safety deadline less the cleanup reserve")
	}
	return nil
}

// A TaskInput is a task as the operator supplies it, before its workdir
// is uploaded: either canonical skygo task JSON or SkyPilot task YAML.
type TaskInput struct {
	// JSON is a protobuf JSON skygo.v1.Task. It must not name a workdir:
	// the workdir is the snapshot Run uploads.
	JSON []byte

	// YAML is a SkyPilot task, lowered with the Config's shell, target
	// and infra binding. Its workdir path is not read: the snapshot
	// replaces it. Outputs and TimeoutSeconds supply what SkyPilot task
	// YAML cannot express.
	YAML           []byte
	Outputs        []string
	TimeoutSeconds int64
}

// placeholderID stands for the workdir reference ID while a task is
// lowered. Lower checks only the reference's shape and copies it; Run
// replaces it with the finalized reference before anything is sent.
const placeholderID = "00000000000000000000000000000000"

// Task returns the task in in, checked against c, without its workdir.
// size and digest describe the workdir snapshot. Task refuses, before any
// effect, a task the VM cannot run as given: accelerators, images,
// resource limits, labels, selectors, retries, data mounts, a directory,
// another target, a request beyond the declared capacity, or a timeout
// that is not finite or does not fit the safety deadline at now.
func (c *Config) Task(in TaskInput, digest string, size int64, now time.Time) (*v1.Task, error) {
	var (
		t   *v1.Task
		err error
	)
	switch {
	case (in.JSON == nil) == (in.YAML == nil):
		return nil, errors.New("task: exactly one of JSON and YAML is required")
	case len(in.JSON) > maxTaskFile || len(in.YAML) > maxTaskFile:
		return nil, errors.New("task exceeds 1 MiB")
	case in.JSON != nil:
		if len(in.Outputs) != 0 || in.TimeoutSeconds != 0 {
			return nil, errors.New("task: a JSON task names its own outputs and timeout")
		}
		t = new(v1.Task)
		if err := (protojson.UnmarshalOptions{}).Unmarshal(in.JSON, t); err != nil {
			return nil, errors.New("task: not a skygo task in protobuf JSON")
		}
		if t.Workdir != nil {
			return nil, errors.New("task: a JSON task must not name a workdir; it is the uploaded snapshot")
		}
		if t.Target == "" {
			t.Target = c.Target
		}
	default:
		t, err = c.lowerYAML(in, digest, size)
		if err != nil {
			return nil, err
		}
	}
	if err := c.checkTask(t, now); err != nil {
		return nil, err
	}
	b, err := encodeTask(withWorkdir(t, &v1.ArtifactRef{Id: placeholderID, Sha256: digest, Size: size}))
	if err != nil {
		return nil, err
	}
	if len(b) > maxTaskBytes {
		return nil, fmt.Errorf("task: encoded task exceeds %d bytes", maxTaskBytes)
	}
	return t, nil
}

func (c *Config) lowerYAML(in TaskInput, digest string, size int64) (*v1.Task, error) {
	d, err := taskyaml.Parse(bytes.NewReader(in.YAML), taskyaml.Options{})
	if err != nil {
		return nil, err
	}
	if d.Task.Workdir == nil {
		return nil, errors.New("task: the SkyPilot task needs a workdir; every job uploads one")
	}
	opts := tasklower.Options{
		Shell:          c.Shell,
		Target:         c.Target,
		Workdir:        &v1.ArtifactRef{Id: placeholderID, Sha256: digest, Size: size},
		Outputs:        in.Outputs,
		TimeoutSeconds: in.TimeoutSeconds,
	}
	if c.Infra != "" {
		opts.InfraBinding = &tasklower.InfraBinding{Infra: c.Infra, Target: c.Target}
	}
	t, _, err := tasklower.Lower(d, opts)
	if err != nil {
		return nil, err
	}
	t.Workdir = nil
	return t, nil
}

// checkTask refuses what the VM cannot run as given. Errors name fields,
// never values.
func (c *Config) checkTask(t *v1.Task, now time.Time) error {
	switch {
	case len(t.Argv) == 0:
		return errors.New("task: argv must not be empty")
	case t.Target != c.Target:
		return errors.New("task: target is not the configured agent target")
	case t.Accelerator != "" || t.AcceleratorCount != 0:
		return errors.New("task: accelerators are not supported on this VM")
	case t.Image != "":
		return errors.New("task: images are not supported on this VM")
	case t.CpuMillis > c.CPUMillis || t.MemoryBytes > c.MemoryBytes || t.CpuMillis < 0 || t.MemoryBytes < 0:
		return errors.New("task: cpu or memory request exceeds the VM's declared capacity")
	case t.CpuLimitMillis != 0 || t.MemoryLimitBytes != 0:
		return errors.New("task: resource limits are not supported on this VM")
	case len(t.NodeSelector) != 0 || len(t.TargetLabels) != 0:
		return errors.New("task: node selectors and target labels are not supported on this VM")
	case t.Retry != nil:
		return errors.New("task: retry policies are not supported by this command")
	case len(t.DataMounts) != 0:
		return errors.New("task: data mounts are not supported by this command")
	case t.Directory != "":
		return errors.New("task: a workspace task has no directory")
	case len(t.Setup) > 64 || len(t.Outputs) > 16:
		return errors.New("task: setup or outputs exceed their bounds")
	}
	return c.checkTimeout(now, t.TimeoutSeconds)
}

// decodeStrict decodes exactly one JSON value from data into v, refusing
// unknown fields and trailing data.
func decodeStrict(data []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing data after the JSON value")
	}
	return nil
}
