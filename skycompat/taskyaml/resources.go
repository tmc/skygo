package taskyaml

import (
	"math"
	"math/big"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

var (
	decimal = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]+)?$`)
	// diskSize is upstream MEMORY_SIZE_PATTERN (sky/skylet/constants.py).
	diskSize = regexp.MustCompile(`^([0-9]+)(kb|ki|mb|mi|gb|gi|tb|ti|pb|pi|KB|KI|MB|MI|GB|GI|TB|TI|PB|PI|Kb|Ki|Mb|Mi|Gb|Gi|Tb|Ti|Pb|Pi)?$`)
	// duration is upstream TIME_PATTERN with its minute multipliers.
	duration = regexp.MustCompile(`^([0-9]+)([mhdwMHDW])?$`)
	// memorySpec is an accelerator memory constraint (sky/resources.py).
	memorySpec = regexp.MustCompile(`^[0-9]+[GgMmTt][Bb]\+?$`)
)

var minutesPer = map[string]int64{"": 1, "m": 1, "h": 60, "d": 24 * 60, "w": 7 * 24 * 60}

func parseResources(d *Document, n *yaml.Node) (*Resources, error) {
	f, err := fields(n, "resources")
	if err != nil {
		return nil, err
	}
	r := &Resources{}
	for i := 0; i < len(f); i += 2 {
		k, v := f[i], f[i+1]
		p := safePath("resources", k.Value)
		location(d, p, v)
		switch k.Value {
		case "cpus":
			if !isNull(v) {
				r.CPUs, err = quantity(v, p, false)
			}
		case "memory":
			if !isNull(v) {
				r.Memory, err = quantity(v, p, true)
			}
		case "disk_size":
			if !isNull(v) {
				r.DiskSize, err = disk(v, p)
			}
		case "accelerators":
			if !isNull(v) {
				r.Accelerators, r.AcceleratorsOrdered, err = accelerators(v)
			}
		case "cloud":
			r.Cloud, err = nullableString(v, p)
		case "region":
			r.Region, err = nullableString(v, p)
		case "zone":
			r.Zone, err = nullableString(v, p)
		case "infra":
			r.Infra, err = nullableString(v, p)
		case "image_id":
			if !isNull(v) && v.Tag != "!!str" {
				err = problem(Unsupported, p, v)
			} else {
				r.ImageID, err = nullableString(v, p)
			}
		case "instance_type":
			r.InstanceType, err = nullableString(v, p)
		case "use_spot":
			if !isNull(v) {
				r.UseSpot, err = boolean(v, p)
			}
		case "autostop":
			if !isNull(v) {
				r.Autostop, err = autostop(v)
			}
		case "job_recovery":
			if !isNull(v) {
				r.JobRecovery, err = jobRecovery(v)
			}
		case "any_of", "ordered", "accelerator_args", "disk_tier", "network_tier", "local_disk", "max_hourly_cost", "ports", "labels", "volumes", "priority", "priority_class", "_no_missing_accel_warnings", "_is_image_managed", "_requires_fuse", "_cluster_config_overrides", "_docker_login_config", "_docker_username_for_runpod":
			if !isNull(v) {
				err = problem(Unsupported, p, v)
			}
		default:
			err = problem(Unsupported, "resources", k)
		}
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func quantity(n *yaml.Node, path string, memory bool) (*Quantity, error) {
	if n.Kind != yaml.ScalarNode || (n.Tag != "!!str" && n.Tag != "!!int" && n.Tag != "!!float") {
		return nil, problem(Invalid, path, n)
	}
	q := &Quantity{Value: n.Value}
	if strings.HasSuffix(q.Value, "+") {
		q.AtLeast = true
		q.Value = strings.TrimSuffix(q.Value, "+")
	}
	if memory {
		if strings.HasSuffix(q.Value, "x") {
			return nil, problem(Unsupported, path, n)
		}
		for _, unit := range []string{"kb", "ki", "mb", "mi", "gb", "gi", "tb", "ti", "pb", "pi"} {
			if strings.HasSuffix(strings.ToLower(q.Value), unit) {
				q.Unit = q.Value[len(q.Value)-2:]
				q.Value = q.Value[:len(q.Value)-2]
				break
			}
		}
	}
	if !positive(q.Value) {
		return nil, problem(Invalid, path, n)
	}
	return q, nil
}

// positive reports whether s is a canonical decimal in (0, 10^15].
func positive(s string) bool {
	if !decimal.MatchString(s) {
		return false
	}
	x, ok := new(big.Rat).SetString(s)
	return ok && x.Sign() > 0 && x.Cmp(big.NewRat(1000000000000000, 1)) <= 0
}

// disk accepts upstream's integer or MEMORY_SIZE_PATTERN string disk size.
// Fractions, minimums and ratios are invalid there.
func disk(n *yaml.Node, path string) (*Quantity, error) {
	if n.Kind != yaml.ScalarNode {
		return nil, problem(Invalid, path, n)
	}
	var q Quantity
	switch n.Tag {
	case "!!int":
		q.Value = n.Value
	case "!!str":
		m := diskSize.FindStringSubmatch(n.Value)
		if m == nil {
			return nil, problem(Invalid, path, n)
		}
		q.Value, q.Unit = m[1], m[2]
	default:
		return nil, problem(Invalid, path, n)
	}
	if !integer.MatchString(q.Value) || !positive(q.Value) {
		return nil, problem(Invalid, path, n)
	}
	return &q, nil
}

// accelerators accepts upstream's string NAME[:COUNT], a mapping from NAME
// to a number, a mapping whose null-valued keys are NAME[:COUNT] (the set
// syntax {A:1, B:1}), a standard YAML !!set, and a list of NAME[:COUNT]
// in preference order.
// The result is ordered only for a list.
func accelerators(n *yaml.Node) ([]Accelerator, bool, error) {
	const path = "resources.accelerators"
	var out []Accelerator
	add := func(a Accelerator, at *yaml.Node) error {
		if slices.Contains(out, a) {
			return problem(Invalid, path, at)
		}
		out = append(out, a)
		return nil
	}
	switch n.Kind {
	case yaml.ScalarNode:
		if n.Tag != "!!str" {
			return nil, false, problem(Invalid, path, n)
		}
		a, err := accelerator(n, n.Value)
		if err != nil {
			return nil, false, err
		}
		return []Accelerator{a}, false, nil
	case yaml.MappingNode:
		f, err := fields(n, path)
		if err != nil {
			return nil, false, err
		}
		for i := 0; i < len(f); i += 2 {
			k, v := f[i], f[i+1]
			if n.Tag == "!!set" && !isNull(v) {
				return nil, false, problem(Invalid, path, v)
			}
			var a Accelerator
			switch {
			case isNull(v):
				a, err = accelerator(k, k.Value)
			case strings.Contains(k.Value, ":"):
				err = problem(Invalid, path, k)
			case v.Kind != yaml.ScalarNode || v.Tag != "!!int" && v.Tag != "!!float":
				err = problem(Invalid, path, v)
			default:
				a, err = named(k, k.Value, v)
			}
			if err == nil {
				err = add(a, k)
			}
			if err != nil {
				return nil, false, err
			}
		}
		return out, false, nil
	case yaml.SequenceNode:
		for _, v := range n.Content {
			if v.Kind != yaml.ScalarNode || v.Tag != "!!str" {
				return nil, false, problem(Invalid, path, v)
			}
			a, err := accelerator(v, v.Value)
			if err == nil {
				err = add(a, v)
			}
			if err != nil {
				return nil, false, err
			}
		}
		return out, true, nil
	}
	return nil, false, problem(Invalid, path, n)
}

// accelerator parses NAME or NAME:COUNT. Upstream memory forms such as
// 80GB+ or nvidia:24GB:1 select devices from a catalog and are unsupported.
func accelerator(at *yaml.Node, s string) (Accelerator, error) {
	const path = "resources.accelerators"
	parts := strings.Split(s, ":")
	if slices.ContainsFunc(parts, memorySpec.MatchString) {
		return Accelerator{}, problem(Unsupported, path, at)
	}
	if len(parts) > 2 {
		return Accelerator{}, problem(Invalid, path, at)
	}
	count := "1"
	if len(parts) == 2 {
		count = parts[1]
	}
	return named(at, parts[0], &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: count, Line: at.Line, Column: at.Column})
}

func named(at *yaml.Node, name string, count *yaml.Node) (Accelerator, error) {
	const path = "resources.accelerators"
	if name == "" || strings.TrimSpace(name) != name {
		return Accelerator{}, problem(Invalid, path, at)
	}
	q, err := quantity(count, path, false)
	if err != nil {
		return Accelerator{}, err
	}
	if q.AtLeast {
		return Accelerator{}, problem(Unsupported, path, at)
	}
	return Accelerator{Name: name, Count: *q}, nil
}

// autostop accepts upstream's _AUTOSTOP_SCHEMA (sky/utils/schemas.py):
// a boolean, idle minutes as an integer or duration, or a mapping.
func autostop(n *yaml.Node) (*Autostop, error) {
	const path = "resources.autostop"
	if n.Kind == yaml.MappingNode {
		f, err := fields(n, path)
		if err != nil {
			return nil, err
		}
		a := &Autostop{Enabled: true}
		for i := 0; i < len(f); i += 2 {
			k, v := f[i], f[i+1]
			switch k.Value {
			case "idle_minutes":
				m, ok := int64Value(v)
				if !ok || m < 0 {
					return nil, problem(Invalid, path, v)
				}
				a.IdleMinutes = &m
			case "down":
				a.Down, err = boolean(v, path)
			case "wait_for":
				w := strings.ToLower(v.Value)
				if v.Kind != yaml.ScalarNode || v.Tag != "!!str" || w != "jobs_and_ssh" && w != "jobs" && w != "none" {
					return nil, problem(Invalid, path, v)
				}
				w = v.Value
				a.WaitFor = &w
			case "hook", "hook_timeout":
				// Deprecated lifecycle hooks run text; outside this profile.
				err = problem(Unsupported, path, v)
			default:
				err = problem(Invalid, path, k)
			}
			if err != nil {
				return nil, err
			}
		}
		return a, nil
	}
	if n.Kind != yaml.ScalarNode {
		return nil, problem(Invalid, path, n)
	}
	switch n.Tag {
	case "!!bool":
		b, err := boolean(n, path)
		if err != nil {
			return nil, err
		}
		return &Autostop{Enabled: *b}, nil
	case "!!int":
		m, ok := int64Value(n)
		if !ok {
			return nil, problem(Invalid, path, n)
		}
		return &Autostop{Enabled: true, IdleMinutes: &m}, nil
	case "!!str":
		m := duration.FindStringSubmatch(n.Value)
		if m == nil {
			return nil, problem(Invalid, path, n)
		}
		x, err := strconv.ParseInt(m[1], 10, 64)
		unit := minutesPer[strings.ToLower(m[2])]
		if err != nil || x > math.MaxInt64/unit {
			return nil, problem(Invalid, path, n)
		}
		x *= unit
		return &Autostop{Enabled: true, IdleMinutes: &x}, nil
	}
	return nil, problem(Invalid, path, n)
}

// jobRecovery accepts a strategy name or a mapping of the upstream
// properties. Other mapping keys are plugin properties outside this profile.
func jobRecovery(n *yaml.Node) (*JobRecovery, error) {
	const path = "resources.job_recovery"
	if n.Kind == yaml.ScalarNode && n.Tag == "!!str" {
		s := n.Value
		return &JobRecovery{Strategy: &s}, nil
	}
	f, err := fields(n, path)
	if err != nil {
		return nil, err
	}
	j := &JobRecovery{}
	for i := 0; i < len(f); i += 2 {
		k, v := f[i], f[i+1]
		switch k.Value {
		case "strategy":
			j.Strategy, err = nullableString(v, path)
		case "max_restarts_on_errors":
			m, ok := int64Value(v)
			if !ok || m < 0 {
				return nil, problem(Invalid, path, v)
			}
			j.MaxRestartsOnErrors = &m
		case "recover_on_exit_codes":
			j.RecoverOnExitCodes, err = exitCodes(v)
		default:
			err = problem(Unsupported, path, k)
		}
		if err != nil {
			return nil, err
		}
	}
	return j, nil
}

// exitCodes accepts one exit code or a list of distinct exit codes.
func exitCodes(n *yaml.Node) ([]int, error) {
	const path = "resources.job_recovery"
	items := []*yaml.Node{n}
	if n.Kind == yaml.SequenceNode {
		items = n.Content
	}
	codes := []int{}
	for _, v := range items {
		x, ok := int64Value(v)
		if !ok || x < 0 || x > 255 {
			return nil, problem(Invalid, path, v)
		}
		if slices.Contains(codes, int(x)) {
			return nil, problem(Invalid, path, v)
		}
		codes = append(codes, int(x))
	}
	return codes, nil
}

// int64Value returns a canonical decimal YAML integer.
func int64Value(n *yaml.Node) (int64, bool) {
	if n.Kind != yaml.ScalarNode || n.Tag != "!!int" || !integer.MatchString(n.Value) {
		return 0, false
	}
	x, err := strconv.ParseInt(n.Value, 10, 64)
	return x, err == nil
}

func boolean(n *yaml.Node, path string) (*bool, error) {
	var b bool
	if n.Kind != yaml.ScalarNode || n.Tag != "!!bool" || n.Decode(&b) != nil {
		return nil, problem(Invalid, path, n)
	}
	return &b, nil
}
