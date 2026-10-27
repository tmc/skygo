package taskyaml

import (
	"bytes"
	"io"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	maxBytes  = 1 << 20
	maxScalar = 16 << 10
	maxNodes  = 8192
	maxDepth  = 32
)

var envName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
var integer = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)

// Parse reads exactly one task document within the fixed package limits.
// Unresolved null environment values fail unless opts explicitly overrides them.
// Reader errors are returned as safe *Error values without the reader's message.
func Parse(r io.Reader, opts Options) (*Document, error) { return parse(r, opts, false) }

// ParseDAG reads exactly a name header (optionally execution: serial) followed
// by one task, as emitted by the pinned SDK. Additional tasks are unsupported.
func ParseDAG(r io.Reader, opts Options) (*Document, error) { return parse(r, opts, true) }

func problem(code Code, path string, n *yaml.Node) error {
	e := &Error{Code: code, Path: path}
	if n != nil {
		e.Line = n.Line
		e.Column = n.Column
	}
	return e
}

func parse(r io.Reader, opts Options, dag bool) (*Document, error) {
	if r == nil {
		return nil, problem(Invalid, "", nil)
	}
	if len(opts.EnvOverrides) > maxNodes {
		return nil, problem(Limit, "envs", nil)
	}
	for k, v := range opts.EnvOverrides {
		if len(k) > maxScalar || len(v) > maxScalar {
			return nil, problem(Limit, "envs", nil)
		}
		if !envName.MatchString(k) {
			return nil, problem(Invalid, "envs", nil)
		}
	}
	b, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, problem(Invalid, "", nil)
	}
	if len(b) > maxBytes {
		return nil, problem(Limit, "", nil)
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	var docs []*yaml.Node
	for {
		var n yaml.Node
		err := dec.Decode(&n)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, problem(Invalid, "", nil)
		}
		if len(n.Content) != 1 {
			return nil, problem(Invalid, "", &n)
		}
		docs = append(docs, n.Content[0])
		if len(docs) > 2 {
			return nil, problem(Unsupported, "", n.Content[0])
		}
	}
	want := 1
	if dag {
		want = 2
	}
	if len(docs) != want {
		code := Invalid
		if len(docs) > want {
			code = Unsupported
		}
		return nil, problem(code, "", nil)
	}
	d := &Document{Locations: make(map[string]Position)}
	count := 0
	for i, n := range docs {
		path := ""
		if dag && i == 0 {
			path = "header"
		}
		if err := checkNode(n, path, 0, &count); err != nil {
			return nil, err
		}
	}
	if dag {
		if err := parseHeader(d, docs[0]); err != nil {
			return nil, err
		}
	}
	if err := parseTask(d, docs[len(docs)-1], opts); err != nil {
		return nil, err
	}
	return d, nil
}

func safePath(parent, key string) string {
	if parent == "" {
		switch key {
		case "name", "run", "setup", "workdir", "envs", "resources", "num_nodes", "file_mounts", "volumes", "service", "config", "job", "secrets", "managed_secrets", "_user_specified_yaml", "file_mounts_mapping", "volume_mounts", "event_callback", "inputs", "outputs", "api_server_access", "pool", "_metadata":
			return key
		}
	}
	if parent == "resources" {
		switch key {
		case "cpus", "memory", "accelerators", "cloud", "region", "zone", "image_id", "instance_type", "use_spot", "disk_size", "job_recovery", "any_of", "ordered", "infra", "accelerator_args", "disk_tier", "network_tier", "local_disk", "max_hourly_cost", "ports", "labels", "volumes", "autostop", "priority", "priority_class", "_no_missing_accel_warnings", "_is_image_managed", "_requires_fuse", "_cluster_config_overrides", "_docker_login_config", "_docker_username_for_runpod":
			return parent + "." + key
		}
	}
	if parent == "header" {
		if key == "name" {
			return "header.name"
		}
		if key == "execution" {
			return "execution"
		}
	}
	return parent
}

func checkNode(n *yaml.Node, path string, depth int, count *int) error {
	*count++
	if *count > maxNodes || depth > maxDepth || len(n.Value) > maxScalar {
		return problem(Limit, path, n)
	}
	if n.Kind == yaml.AliasNode || n.Anchor != "" {
		return problem(Invalid, path, n)
	}
	switch n.Tag {
	case "!!map", "!!seq", "!!str", "!!null", "!!bool", "!!int", "!!float", "!!timestamp":
	case "!!set":
		if path != "resources.accelerators" || n.Kind != yaml.MappingNode {
			return problem(Invalid, path, n)
		}
	default:
		return problem(Invalid, path, n)
	}
	if n.Kind == yaml.MappingNode {
		seen := map[string]bool{}
		for i := 0; i < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			p := safePath(path, k.Value)
			if k.Kind != yaml.ScalarNode || k.Value == "<<" {
				return problem(Invalid, path, k)
			}
			if seen[k.Value] {
				return problem(Invalid, p, k)
			}
			seen[k.Value] = true
			if err := checkNode(k, p, depth+1, count); err != nil {
				return err
			}
			if err := checkNode(v, p, depth+1, count); err != nil {
				return err
			}
		}
		return nil
	}
	for _, v := range n.Content {
		if err := checkNode(v, path, depth+1, count); err != nil {
			return err
		}
	}
	return nil
}
func fields(n *yaml.Node, path string) ([]*yaml.Node, error) {
	if n.Kind != yaml.MappingNode {
		return nil, problem(Invalid, path, n)
	}
	for i := 0; i < len(n.Content); i += 2 {
		if n.Content[i].Tag != "!!str" {
			return nil, problem(Invalid, path, n.Content[i])
		}
	}
	return n.Content, nil
}
func location(d *Document, path string, n *yaml.Node) { d.Locations[path] = Position{n.Line, n.Column} }
func nullableString(n *yaml.Node, path string) (*string, error) {
	if n.Tag == "!!null" {
		return nil, nil
	}
	if n.Kind != yaml.ScalarNode || n.Tag != "!!str" {
		return nil, problem(Invalid, path, n)
	}
	v := n.Value
	return &v, nil
}
func isNull(n *yaml.Node) bool { return n.Tag == "!!null" }
func parseHeader(d *Document, n *yaml.Node) error {
	f, err := fields(n, "header")
	if err != nil {
		return err
	}
	hasName := false
	for i := 0; i < len(f); i += 2 {
		k, v := f[i], f[i+1]
		p := safePath("header", k.Value)
		location(d, p, v)
		switch k.Value {
		case "name":
			hasName = true
			d.DAGName, err = nullableString(v, p)
		case "execution":
			if v.Tag != "!!str" || v.Value != "serial" {
				err = problem(Unsupported, p, v)
			}
		default:
			err = problem(Unsupported, "header", k)
		}
		if err != nil {
			return err
		}
	}
	if !hasName {
		return problem(Invalid, "header.name", n)
	}
	return nil
}

func parseTask(d *Document, n *yaml.Node, opts Options) error {
	f, err := fields(n, "")
	if err != nil {
		return err
	}
	d.Task.NumNodes = 1
	for i := 0; i < len(f); i += 2 {
		k, v := f[i], f[i+1]
		p := safePath("", k.Value)
		location(d, p, v)
		switch k.Value {
		case "name":
			d.Task.Name, err = nullableString(v, p)
		case "run":
			d.Task.Run, err = nullableString(v, p)
		case "setup":
			d.Task.Setup, err = nullableString(v, p)
		case "workdir":
			d.Task.Workdir, err = nullableString(v, p)
		case "_user_specified_yaml":
			d.SourceYAML, err = nullableString(v, p)
		case "envs":
			d.Task.Envs, err = parseEnvs(v, opts)
		case "resources":
			if !isNull(v) {
				d.Task.Resources, err = parseResources(d, v)
			}
		case "num_nodes":
			if !isNull(v) {
				var x int64
				x, err = strconv.ParseInt(v.Value, 10, 32)
				if err != nil || v.Tag != "!!int" || !integer.MatchString(v.Value) || x < 1 {
					err = problem(Invalid, p, v)
				} else {
					d.Task.NumNodes = int(x)
				}
			}
		case "file_mounts":
			d.Task.FileMounts, err = references(v, p)
		case "volumes":
			d.Task.Volumes, err = references(v, p)
		case "service", "config", "job", "secrets", "managed_secrets", "file_mounts_mapping", "volume_mounts", "event_callback", "inputs", "outputs", "pool", "_metadata":
			if !isNull(v) {
				err = problem(Unsupported, p, v)
			}
		case "api_server_access":
			if !isNull(v) {
				err = problem(Unsupported, p, v)
			}
		default:
			err = problem(Unsupported, "", k)
		}
		if err != nil {
			return err
		}
	}
	if d.Task.Envs == nil && len(opts.EnvOverrides) > 0 {
		d.Task.Envs = map[string]string{}
		for k, v := range opts.EnvOverrides {
			d.Task.Envs[k] = v
		}
	}
	return nil
}

func parseEnvs(n *yaml.Node, opts Options) (map[string]string, error) {
	out := make(map[string]string)
	if !isNull(n) {
		if n.Kind != yaml.MappingNode {
			return nil, problem(Invalid, "envs", n)
		}
		for i := 0; i < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			key, err := envScalar(k)
			if err != nil {
				return nil, err
			}
			if !envName.MatchString(key) {
				return nil, problem(Invalid, "envs", k)
			}
			if _, ok := out[key]; ok {
				return nil, problem(Invalid, "envs", k)
			}
			if override, ok := opts.EnvOverrides[key]; ok {
				out[key] = override
				continue
			}
			if isNull(v) {
				return nil, problem(Invalid, "envs", v)
			}
			value, err := envScalar(v)
			if err != nil {
				return nil, err
			}
			out[key] = value
		}
	}
	for k, v := range opts.EnvOverrides {
		out[k] = v
	}
	return out, nil
}
func envScalar(n *yaml.Node) (string, error) {
	if n.Kind != yaml.ScalarNode {
		return "", problem(Invalid, "envs", n)
	}
	if n.Tag == "!!bool" {
		switch strings.ToLower(n.Value) {
		case "true", "yes", "on":
			return "True", nil
		case "false", "no", "off":
			return "False", nil
		}
		return "", problem(Invalid, "envs", n)
	}
	if n.Tag == "!!str" {
		if n.Style == 0 {
			switch strings.ToLower(n.Value) {
			case "yes", "on":
				return "True", nil
			case "no", "off":
				return "False", nil
			}
		}
		return n.Value, nil
	}
	if n.Tag == "!!int" {
		if !integer.MatchString(n.Value) {
			return "", problem(Unsupported, "envs", n)
		}
		if n.Value == "-0" {
			return "0", nil
		}
		return n.Value, nil
	}
	return "", problem(Unsupported, "envs", n)
}
func references(n *yaml.Node, path string) (map[string]string, error) {
	if isNull(n) {
		return nil, nil
	}
	f, err := fields(n, path)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for i := 0; i < len(f); i += 2 {
		k, v := f[i], f[i+1]
		if v.Tag != "!!str" || v.Kind != yaml.ScalarNode {
			return nil, problem(Unsupported, path, v)
		}
		out[k.Value] = v.Value
	}
	return out, nil
}
