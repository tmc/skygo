package taskyaml

import (
	"math/big"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

var decimal = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]+)?$`)

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
				r.DiskSize, err = quantity(v, p, true)
				if err == nil && (r.DiskSize.Value != "256" || r.DiskSize.Unit != "" || r.DiskSize.AtLeast) {
					err = problem(Unsupported, p, v)
				}
			}
		case "accelerators":
			if !isNull(v) {
				r.Accelerators, err = accelerators(v)
			}
		case "cloud":
			r.Cloud, err = nullableString(v, p)
		case "region":
			r.Region, err = nullableString(v, p)
		case "zone":
			r.Zone, err = nullableString(v, p)
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
				var b bool
				err = v.Decode(&b)
				if v.Tag != "!!bool" || err != nil {
					err = problem(Invalid, p, v)
				} else {
					r.UseSpot = &b
				}
			}
		case "job_recovery", "any_of", "ordered", "infra", "accelerator_args", "disk_tier", "network_tier", "local_disk", "max_hourly_cost", "ports", "labels", "volumes", "autostop", "priority", "priority_class", "_no_missing_accel_warnings", "_is_image_managed", "_requires_fuse", "_cluster_config_overrides", "_docker_login_config", "_docker_username_for_runpod":
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
	if !decimal.MatchString(q.Value) {
		return nil, problem(Invalid, path, n)
	}
	x, ok := new(big.Rat).SetString(q.Value)
	if !ok || x.Sign() <= 0 || x.Cmp(big.NewRat(1000000000000000, 1)) > 0 {
		return nil, problem(Invalid, path, n)
	}
	return q, nil
}

func accelerators(n *yaml.Node) (map[string]Quantity, error) {
	const path = "resources.accelerators"
	var model string
	var count *yaml.Node
	if n.Kind == yaml.ScalarNode && n.Tag == "!!str" {
		parts := strings.Split(n.Value, ":")
		model = parts[0]
		value := "1"
		if len(parts) == 2 {
			value = parts[1]
		} else if len(parts) > 2 {
			return nil, problem(Invalid, path, n)
		}
		count = &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value, Line: n.Line, Column: n.Column}
	} else if n.Kind == yaml.MappingNode {
		f, err := fields(n, path)
		if err != nil {
			return nil, err
		}
		if len(f) != 2 {
			return nil, problem(Unsupported, path, n)
		}
		model = f[0].Value
		count = f[1]
	} else {
		return nil, problem(Unsupported, path, n)
	}
	if model == "" || strings.TrimSpace(model) != model {
		return nil, problem(Invalid, path, n)
	}
	q, err := quantity(count, path, false)
	if err != nil {
		return nil, err
	}
	if q.AtLeast {
		return nil, problem(Unsupported, path, n)
	}
	return map[string]Quantity{model: *q}, nil
}
