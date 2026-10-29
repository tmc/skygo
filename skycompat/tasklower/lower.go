package tasklower

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/big"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/skycompat/taskyaml"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Options supplies the skygo semantics that SkyPilot task YAML cannot express.
// The zero value lowers no task: every task has run text, which needs Shell.
type Options struct {
	// Shell is the clean absolute path of the shell that runs run and setup
	// text as Shell -c TEXT on the target. It is never defaulted.
	Shell string
	// Target optionally names the skygo target.
	Target string
	// InfraBinding optionally supplies an operator-authorized mapping from
	// resources.infra to a fixed target. It is used only when the task
	// requests that exact infrastructure; it never selects an endpoint.
	InfraBinding *InfraBinding
	// Workdir replaces the task's workdir path. It is required exactly when
	// the task has a workdir. The path itself is never read.
	Workdir *skyv1.ArtifactRef
	// Outputs lists result roots relative to the workdir. Upstream has no
	// equivalent; outputs require a workdir.
	Outputs []string
	// TimeoutSeconds bounds each attempt, at most 604800. Zero keeps the
	// server default.
	TimeoutSeconds int64
	// AllowDrop permits dropping the nonexecuting fields "name" and
	// "_user_specified_yaml". Lower refuses them otherwise.
	AllowDrop []string
}

// InfraBinding maps one upstream infrastructure selector to a skygo target.
// The caller must obtain it from operator configuration or an authenticated
// transport, never from the task being lowered. Both fields must be nonempty.
// The zero value is not usable. A binding does not establish target capability.
type InfraBinding struct {
	Infra  string
	Target string
}

// Status reports how Lower treated one source field.
type Status string

const (
	// Lowered means the field's value is represented in the task.
	Lowered Status = "lowered"
	// Default means the field holds an upstream default or null and needs
	// no representation.
	Default Status = "default"
	// Dropped means the nonexecuting field was omitted by explicit request.
	Dropped Status = "dropped"
)

// Note describes the treatment of one recognized source field. Path is a
// taskyaml field path. Detail is fixed text that never contains source values.
type Note struct {
	Path   string
	Status Status
	Detail string
}

// paths lists every task field path taskyaml records, in a fixed order.
var paths = []string{
	"name", "_user_specified_yaml", "run", "setup", "workdir", "envs",
	"resources", "resources.cpus", "resources.memory", "resources.disk_size",
	"resources.accelerators", "resources.cloud", "resources.region",
	"resources.zone", "resources.image_id", "resources.instance_type",
	"resources.use_spot", "resources.infra", "resources.autostop",
	"resources.job_recovery", "num_nodes", "file_mounts", "volumes",
	"header.name", "execution",
}

// nullOnly lists fields taskyaml accepts only as null.
var nullOnly = []string{
	"service", "config", "job", "secrets", "managed_secrets",
	"file_mounts_mapping", "volume_mounts", "event_callback", "inputs",
	"outputs", "pool", "_metadata", "api_server_access",
	"resources.any_of", "resources.ordered",
	"resources.accelerator_args", "resources.disk_tier",
	"resources.network_tier", "resources.local_disk",
	"resources.max_hourly_cost", "resources.ports", "resources.labels",
	"resources.volumes", "resources.priority",
	"resources.priority_class", "resources._no_missing_accel_warnings",
	"resources._is_image_managed", "resources._requires_fuse",
	"resources._cluster_config_overrides", "resources._docker_login_config",
	"resources._docker_username_for_runpod",
}

// memoryUnits are upstream's binary multipliers (sky/skylet/constants.py).
var memoryUnits = map[string]int64{
	"":   1 << 30,
	"kb": 1 << 10, "ki": 1 << 10,
	"mb": 1 << 20, "mi": 1 << 20,
	"gb": 1 << 30, "gi": 1 << 30,
	"tb": 1 << 40, "ti": 1 << 40,
	"pb": 1 << 50, "pi": 1 << 50,
}

var (
	decimal = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]+)?$`)
	envName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

// Lower maps a parsed task to a skygo task and returns one note per present
// source field, in source order. The returned task is new; Lower never
// consults the process environment or the filesystem.
//
// The task environment, used by both setup and run, is the source envs plus
// SKYPILOT_NUM_NODES "1" unless envs sets it, SKYPILOT_NODE_RANK "0" and
// SKYPILOT_NUM_GPUS_PER_NODE, the requested accelerator count or 0 for no
// accelerator or a TPU, Inferentia or Trainium name. These describe the
// request, not allocated devices.
//
// Lower refuses a field it cannot represent with a *taskyaml.Error, possibly
// wrapped with a fixed hint, carrying the field path and source position.
// It refuses invalid Options with another error. Neither contains source
// or option values.
func Lower(d *taskyaml.Document, opts Options) (*skyv1.Task, []Note, error) {
	if d == nil {
		return nil, nil, errors.New("tasklower: no document")
	}
	if err := opts.check(); err != nil {
		return nil, nil, err
	}
	l := &lowering{d: d, opts: opts, task: &skyv1.Task{
		Target:         opts.Target,
		TimeoutSeconds: opts.TimeoutSeconds,
		Outputs:        slices.Clone(opts.Outputs),
	}}
	var present []string
	for _, p := range paths {
		if _, ok := d.Locations[p]; ok || l.set(p) {
			present = append(present, p)
		}
	}
	for p := range d.Locations {
		if !slices.Contains(paths, p) {
			present = append(present, p)
		}
	}
	slices.SortFunc(present, func(a, b string) int {
		x, y := d.Locations[a], d.Locations[b]
		return cmp.Or(cmp.Compare(x.Line, y.Line), cmp.Compare(x.Column, y.Column), strings.Compare(a, b))
	})
	for _, p := range present {
		if err := l.field(p); err != nil {
			return nil, nil, err
		}
	}
	if d.Task.Run == nil {
		return nil, nil, l.refuse(taskyaml.Unsupported, "run", "requires run text")
	}
	if d.Task.Workdir == nil && opts.Workdir != nil {
		return nil, nil, errors.New("tasklower: workdir reference supplied for a task without workdir")
	}
	if d.Task.Workdir == nil && len(opts.Outputs) != 0 {
		return nil, nil, errors.New("tasklower: outputs require a task workdir")
	}
	l.task.Env = runtimeEnv(l.task.Env, l.task.Accelerator, l.task.AcceleratorCount)
	return l.task, l.notes, nil
}

// runtimeEnv returns a new map holding the user environment and the
// single-node variables upstream gives a run (sky/backends/task_codegen.py):
// SKYPILOT_NUM_NODES is set before the user map, so a user value survives;
// SKYPILOT_NODE_RANK and SKYPILOT_NUM_GPUS_PER_NODE are set after it.
// As in sky/utils/accelerator_registry.py, an accelerator whose name contains
// tpu, inferentia or trainium in any case requests no GPUs.
func runtimeEnv(user map[string]string, accel string, count int32) map[string]string {
	env := map[string]string{"SKYPILOT_NUM_NODES": "1"}
	maps.Copy(env, user)
	name := strings.ToLower(accel)
	if accel == "" || strings.Contains(name, "tpu") || strings.Contains(name, "inferentia") || strings.Contains(name, "trainium") {
		count = 0
	}
	env["SKYPILOT_NODE_RANK"] = "0"
	env["SKYPILOT_NUM_GPUS_PER_NODE"] = strconv.Itoa(int(count))
	return env
}

type lowering struct {
	d     *taskyaml.Document
	opts  Options
	task  *skyv1.Task
	notes []Note
}

// set reports whether the typed document holds a non-default value at p,
// so that a document built without Locations cannot lose a field.
func (l *lowering) set(p string) bool {
	t := &l.d.Task
	r := t.Resources
	if r == nil {
		r = &taskyaml.Resources{}
	}
	switch p {
	case "name":
		return t.Name != nil
	case "_user_specified_yaml":
		return l.d.SourceYAML != nil
	case "run":
		return t.Run != nil
	case "setup":
		return t.Setup != nil
	case "workdir":
		return t.Workdir != nil
	case "envs":
		return len(t.Envs) != 0
	case "resources":
		return t.Resources != nil
	case "resources.cpus":
		return r.CPUs != nil
	case "resources.memory":
		return r.Memory != nil
	case "resources.disk_size":
		return r.DiskSize != nil
	case "resources.accelerators":
		return len(r.Accelerators) != 0
	case "resources.cloud":
		return r.Cloud != nil
	case "resources.region":
		return r.Region != nil
	case "resources.zone":
		return r.Zone != nil
	case "resources.image_id":
		return r.ImageID != nil
	case "resources.instance_type":
		return r.InstanceType != nil
	case "resources.use_spot":
		return r.UseSpot != nil
	case "resources.infra":
		return r.Infra != nil
	case "resources.autostop":
		return r.Autostop != nil
	case "resources.job_recovery":
		return r.JobRecovery != nil
	case "num_nodes":
		return t.NumNodes != 0 && t.NumNodes != 1
	case "file_mounts":
		return len(t.FileMounts) != 0
	case "volumes":
		return len(t.Volumes) != 0
	case "header.name":
		return l.d.DAGName != nil
	}
	return false
}

func (l *lowering) note(p string, s Status, detail string) error {
	l.notes = append(l.notes, Note{Path: p, Status: s, Detail: detail})
	return nil
}

func (l *lowering) refuse(code taskyaml.Code, p, hint string) error {
	pos := l.d.Locations[p]
	var err error = &taskyaml.Error{Code: code, Path: p, Line: pos.Line, Column: pos.Column}
	if hint != "" {
		err = fmt.Errorf("%w: %s", err, hint)
	}
	return err
}

func (l *lowering) field(p string) error {
	t := &l.d.Task
	r := t.Resources
	if r == nil {
		r = &taskyaml.Resources{}
	}
	switch p {
	case "name":
		return l.drop(p, t.Name != nil)
	case "_user_specified_yaml":
		return l.drop(p, l.d.SourceYAML != nil)
	case "run":
		if t.Run == nil {
			return l.refuse(taskyaml.Unsupported, p, "requires run text")
		}
		argv, err := l.shell(p, *t.Run)
		if err != nil {
			return err
		}
		l.task.Argv = argv
		return l.note(p, Lowered, "argv is shell -c text")
	case "setup":
		if t.Setup == nil {
			return l.note(p, Default, "null")
		}
		argv, err := l.shell(p, *t.Setup)
		if err != nil {
			return err
		}
		if t.Workdir == nil {
			return l.refuse(taskyaml.Unsupported, p, "requires workdir")
		}
		if b, _ := json.Marshal(argv); len(b) > 16<<10 {
			return l.refuse(taskyaml.Limit, p, "")
		}
		l.task.Setup = argv
		return l.note(p, Lowered, "setup is shell -c text")
	case "workdir":
		if t.Workdir == nil {
			return l.note(p, Default, "null")
		}
		if strings.Contains(*t.Workdir, "$") {
			return l.refuse(taskyaml.Unsupported, p, "environment substitution")
		}
		if l.opts.Workdir == nil {
			return l.refuse(taskyaml.Unsupported, p, "requires a workdir reference")
		}
		w := l.opts.Workdir
		l.task.Workdir = &skyv1.ArtifactRef{Id: w.Id, Sha256: w.Sha256, Size: w.Size}
		return l.note(p, Lowered, "path not read; replaced by the workdir reference")
	case "envs":
		if len(t.Envs) == 0 {
			return l.note(p, Default, "empty")
		}
		env := make(map[string]string, len(t.Envs))
		for k, v := range t.Envs {
			if !envName.MatchString(k) || !utf8.ValidString(v) || strings.ContainsRune(v, 0) {
				return l.refuse(taskyaml.Invalid, p, "")
			}
			env[k] = v
		}
		l.task.Env = env
		return l.note(p, Lowered, "environment")
	case "resources":
		for _, q := range paths {
			if strings.HasPrefix(q, "resources.") && l.set(q) {
				return l.note(p, Default, "fields noted separately")
			}
		}
		return l.note(p, Default, "empty")
	case "resources.cpus":
		if r.CPUs == nil {
			return l.note(p, Default, "null")
		}
		n, ok := exact(r.CPUs, 1000, math.MaxInt64)
		if !ok || r.CPUs.Unit != "" {
			return l.refuse(taskyaml.Invalid, p, "requires whole millicpus")
		}
		l.task.CpuMillis = n
		return l.note(p, Lowered, request(r.CPUs, "cpu_millis"))
	case "resources.memory":
		if r.Memory == nil {
			return l.note(p, Default, "null")
		}
		unit, ok := memoryUnits[strings.ToLower(r.Memory.Unit)]
		var n int64
		if ok {
			n, ok = exact(r.Memory, unit, math.MaxInt64)
		}
		if !ok {
			return l.refuse(taskyaml.Invalid, p, "requires whole bytes")
		}
		l.task.MemoryBytes = n
		return l.note(p, Lowered, request(r.Memory, "memory_bytes"))
	case "resources.disk_size":
		if r.DiskSize == nil {
			return l.note(p, Default, "null")
		}
		if *r.DiskSize != (taskyaml.Quantity{Value: "256"}) {
			return l.refuse(taskyaml.Unsupported, p, "")
		}
		return l.note(p, Default, "upstream unspecified default; not a disk request")
	case "resources.accelerators":
		if len(r.Accelerators) == 0 {
			return l.note(p, Default, "null")
		}
		// A choice among accelerators has no task representation, and
		// selecting one would discard the others.
		if len(r.Accelerators) != 1 {
			return l.refuse(taskyaml.Unsupported, p, "")
		}
		a := r.Accelerators[0]
		n, ok := exact(&a.Count, 1, math.MaxInt32)
		if !ok || a.Count.Unit != "" || a.Count.AtLeast || a.Name == "" || !utf8.ValidString(a.Name) || strings.ContainsRune(a.Name, 0) {
			return l.refuse(taskyaml.Unsupported, p, "requires a whole count")
		}
		l.task.Accelerator = a.Name
		l.task.AcceleratorCount = int32(n)
		return l.note(p, Lowered, "accelerator and accelerator_count")
	case "resources.cloud", "resources.region", "resources.zone", "resources.instance_type":
		v := map[string]*string{"resources.cloud": r.Cloud, "resources.region": r.Region, "resources.zone": r.Zone, "resources.instance_type": r.InstanceType}[p]
		if v != nil {
			return l.refuse(taskyaml.Unsupported, p, "select a skygo target")
		}
		return l.note(p, Default, "null")
	case "resources.image_id":
		if r.ImageID == nil {
			return l.note(p, Default, "null")
		}
		image, ok := strings.CutPrefix(*r.ImageID, "docker:")
		if !ok || image == "" || !utf8.ValidString(image) || strings.ContainsRune(image, 0) {
			return l.refuse(taskyaml.Unsupported, p, "requires docker:IMAGE")
		}
		l.task.Image = image
		return l.note(p, Lowered, "image")
	case "resources.infra":
		if r.Infra == nil {
			return l.note(p, Default, "null")
		}
		b := l.opts.InfraBinding
		if b == nil || *r.Infra != b.Infra || l.task.Target != "" && l.task.Target != b.Target {
			return l.refuse(taskyaml.Unsupported, p, "")
		}
		l.task.Target = b.Target
		return l.note(p, Lowered, "target from explicit infra binding")
	case "resources.autostop", "resources.job_recovery":
		// skygo tasks have no cluster autostop or managed job recovery; any
		// value, even false or {}, is refused.
		if l.set(p) {
			return l.refuse(taskyaml.Unsupported, p, "")
		}
		return l.note(p, Default, "null")
	case "resources.use_spot":
		if r.UseSpot != nil && *r.UseSpot {
			return l.refuse(taskyaml.Unsupported, p, "")
		}
		return l.note(p, Default, "on-demand")
	case "num_nodes":
		if t.NumNodes != 0 && t.NumNodes != 1 {
			return l.refuse(taskyaml.Unsupported, p, "")
		}
		return l.note(p, Default, "one node")
	case "file_mounts", "volumes":
		if len(t.FileMounts) != 0 && p == "file_mounts" || len(t.Volumes) != 0 && p == "volumes" {
			return l.refuse(taskyaml.Unsupported, p, "")
		}
		return l.note(p, Default, "empty")
	}
	if slices.Contains(nullOnly, p) {
		return l.note(p, Default, "null")
	}
	return l.refuse(taskyaml.Unsupported, p, "")
}

// drop refuses a set nonexecuting field unless Options.AllowDrop names it.
func (l *lowering) drop(p string, set bool) error {
	if !set {
		return l.note(p, Default, "null")
	}
	if !slices.Contains(l.opts.AllowDrop, p) {
		return l.refuse(taskyaml.Unsupported, p, "requires explicit drop")
	}
	return l.note(p, Dropped, "not executed")
}

// shell returns the argv that runs text byte-exact through Options.Shell.
func (l *lowering) shell(p, text string) ([]string, error) {
	if !utf8.ValidString(text) || strings.ContainsRune(text, 0) {
		return nil, l.refuse(taskyaml.Invalid, p, "")
	}
	if l.opts.Shell == "" {
		return nil, l.refuse(taskyaml.Unsupported, p, "requires a shell")
	}
	return []string{l.opts.Shell, "-c", text}, nil
}

// exact returns q times scale if it is a positive integer no greater than max.
func exact(q *taskyaml.Quantity, scale, max int64) (int64, bool) {
	if !decimal.MatchString(q.Value) {
		return 0, false
	}
	x, ok := new(big.Rat).SetString(q.Value)
	if !ok {
		return 0, false
	}
	x.Mul(x, new(big.Rat).SetInt64(scale))
	if x.Sign() <= 0 || !x.IsInt() || !x.Num().IsInt64() || x.Num().Int64() > max {
		return 0, false
	}
	return x.Num().Int64(), true
}

func request(q *taskyaml.Quantity, key string) string {
	if q.AtLeast {
		return key + "; minimum lowered to exact request"
	}
	return key
}

func (o *Options) check() error {
	if o.Shell != "" && (!strings.HasPrefix(o.Shell, "/") || path.Clean(o.Shell) != o.Shell || o.Shell == "/" || !utf8.ValidString(o.Shell) || strings.ContainsRune(o.Shell, 0)) {
		return errors.New("tasklower: shell must be a clean absolute path")
	}
	if !utf8.ValidString(o.Target) || strings.ContainsRune(o.Target, 0) {
		return errors.New("tasklower: invalid target")
	}
	if b := o.InfraBinding; b != nil {
		for _, s := range []string{b.Infra, b.Target} {
			if s == "" || strings.TrimSpace(s) != s || !utf8.ValidString(s) || strings.ContainsAny(s, "\x00\r\n") {
				return errors.New("tasklower: invalid infra binding")
			}
		}
	}
	if o.TimeoutSeconds < 0 || o.TimeoutSeconds > 7*24*3600 {
		return errors.New("tasklower: timeout outside 0..604800 seconds")
	}
	for _, p := range o.AllowDrop {
		if p != "name" && p != "_user_specified_yaml" {
			return errors.New("tasklower: allow-drop accepts only name and _user_specified_yaml")
		}
	}
	if w := o.Workdir; w != nil && (!lowerHex(w.Id, 32) || !lowerHex(w.Sha256, 64) || w.Size < 1 || w.Size > 32<<20) {
		return errors.New("tasklower: invalid workdir reference")
	}
	return checkOutputs(o.Outputs)
}

// checkOutputs applies skygo's output root rules (internal/backend/workspace.go).
func checkOutputs(outputs []string) error {
	if len(outputs) > 16 {
		return errors.New("tasklower: outputs exceed 16 roots")
	}
	for i, name := range outputs {
		if name == "" || len(name) > 240 || !utf8.ValidString(name) || strings.HasPrefix(name, "/") || strings.ContainsAny(name, "\\:\x00*?[]") {
			return errors.New("tasklower: invalid output root")
		}
		parts := strings.Split(name, "/")
		if len(parts) > 16 {
			return errors.New("tasklower: output root exceeds depth 16")
		}
		for _, part := range parts {
			if part == "" || part == "." || part == ".." {
				return errors.New("tasklower: invalid output root component")
			}
		}
		for _, earlier := range outputs[:i] {
			if name == earlier || strings.HasPrefix(name, earlier+"/") || strings.HasPrefix(earlier, name+"/") {
				return errors.New("tasklower: overlapping output roots")
			}
		}
	}
	return nil
}

func lowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// maxFile is the task file size `skygo run -f` reads.
const maxFile = 1 << 20

// Marshal encodes t as a skygo task file: one JSON object whose keys are
// the task file keys `skygo run -f` decodes strictly. Integers are exact
// decimal numbers and strings escape every character YAML would not read
// back unchanged. Marshal refuses a task field without a task file key
// instead of dropping it, and refuses output over 1 MiB.
func Marshal(t *skyv1.Task) ([]byte, error) {
	if t == nil {
		return nil, errors.New("tasklower: no task")
	}
	if err := fileFields(t.ProtoReflect()); err != nil {
		return nil, err
	}
	if t.Workdir != nil {
		if err := fileFields(t.Workdir.ProtoReflect()); err != nil {
			return nil, err
		}
	}
	e := &encoder{}
	e.buf.WriteByte('{')
	if len(t.Argv) != 0 {
		e.key("argv")
		e.list(t.Argv)
	}
	if len(t.Env) != 0 {
		e.key("env")
		e.buf.WriteByte('{')
		for i, k := range slices.Sorted(maps.Keys(t.Env)) {
			if i > 0 {
				e.buf.WriteByte(',')
			}
			e.buf.WriteString("\n    ")
			e.quote(k)
			e.buf.WriteString(": ")
			e.quote(t.Env[k])
		}
		e.buf.WriteString("\n  }")
	}
	if t.Image != "" {
		e.key("image")
		e.quote(t.Image)
	}
	e.number("cpu_millis", t.CpuMillis)
	e.number("memory_bytes", t.MemoryBytes)
	if t.Accelerator != "" {
		e.key("accelerator")
		e.quote(t.Accelerator)
	}
	e.number("accelerator_count", int64(t.AcceleratorCount))
	if t.Target != "" {
		e.key("target")
		e.quote(t.Target)
	}
	e.number("timeout_seconds", t.TimeoutSeconds)
	if w := t.Workdir; w != nil {
		e.key("workdir")
		e.buf.WriteString("{\n    \"id\": ")
		e.quote(w.Id)
		e.buf.WriteString(",\n    \"sha256\": ")
		e.quote(w.Sha256)
		e.buf.WriteString(",\n    \"size\": ")
		e.buf.WriteString(strconv.FormatInt(w.Size, 10))
		e.buf.WriteString("\n  }")
	}
	if len(t.Setup) != 0 {
		e.key("setup")
		e.list(t.Setup)
	}
	if len(t.Outputs) != 0 {
		e.key("outputs")
		e.list(t.Outputs)
	}
	if e.n > 0 {
		e.buf.WriteByte('\n')
	}
	e.buf.WriteString("}\n")
	if e.err != nil {
		return nil, e.err
	}
	if e.buf.Len() > maxFile {
		return nil, errors.New("tasklower: task file exceeds 1 MiB")
	}
	return e.buf.Bytes(), nil
}

// fileFields refuses a populated field that the task file cannot carry.
func fileFields(m protoreflect.Message) error {
	if len(m.GetUnknown()) != 0 {
		return errors.New("tasklower: task has unknown fields")
	}
	var err error
	m.Range(func(f protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		switch f.FullName() {
		case "skygo.v1.Task.argv", "skygo.v1.Task.env", "skygo.v1.Task.image",
			"skygo.v1.Task.cpu_millis", "skygo.v1.Task.memory_bytes",
			"skygo.v1.Task.accelerator", "skygo.v1.Task.accelerator_count",
			"skygo.v1.Task.target", "skygo.v1.Task.timeout_seconds",
			"skygo.v1.Task.workdir", "skygo.v1.Task.setup", "skygo.v1.Task.outputs",
			"skygo.v1.ArtifactRef.id", "skygo.v1.ArtifactRef.sha256", "skygo.v1.ArtifactRef.size":
			return true
		}
		err = fmt.Errorf("tasklower: task field %s has no import encoding", f.Name())
		return false
	})
	return err
}

type encoder struct {
	buf bytes.Buffer
	n   int
	err error
}

func (e *encoder) key(k string) {
	if e.n > 0 {
		e.buf.WriteByte(',')
	}
	e.n++
	e.buf.WriteString("\n  ")
	e.quote(k)
	e.buf.WriteString(": ")
}

func (e *encoder) number(k string, v int64) {
	if v != 0 {
		e.key(k)
		e.buf.WriteString(strconv.FormatInt(v, 10))
	}
}

func (e *encoder) list(v []string) {
	e.buf.WriteByte('[')
	for i, s := range v {
		if i > 0 {
			e.buf.WriteString(", ")
		}
		e.quote(s)
	}
	e.buf.WriteByte(']')
}

// quote writes s as a JSON string that YAML also reads as exactly s. YAML
// rejects or folds some characters JSON permits raw: C0 and C1 controls,
// DEL, line and paragraph separators, the byte order mark and U+FFFE/U+FFFF.
func (e *encoder) quote(s string) {
	if !utf8.ValidString(s) {
		e.err = errors.New("tasklower: task text is not UTF-8")
		return
	}
	e.buf.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"':
			e.buf.WriteString(`\"`)
		case r == '\\':
			e.buf.WriteString(`\\`)
		case r == '\n':
			e.buf.WriteString(`\n`)
		case r == '\t':
			e.buf.WriteString(`\t`)
		case r < 0x20, r >= 0x7f && r <= 0x9f, r == 0x2028, r == 0x2029, r == 0xfeff, r == 0xfffe, r == 0xffff:
			fmt.Fprintf(&e.buf, `\u%04x`, r)
		default:
			e.buf.WriteRune(r)
		}
	}
	e.buf.WriteByte('"')
}
