package wire

import (
	"encoding/json"
	"io"
	"strings"
)

// LaunchBody contains the task DAG and cluster name from a selected API67
// launch request. Its zero value is invalid. ClusterName is an upstream name,
// not a canonical target, path, principal or durable alias.
type LaunchBody struct {
	DAG         string
	ClusterName string
}

// ExecBody contains the task DAG and cluster name from a selected API67 exec
// request. Its zero value is invalid. ClusterName has the same meaning as in
// LaunchBody.
type ExecBody struct {
	DAG         string
	ClusterName string
}

// DecodeLaunch reads a selected API67 launch body. It accepts only default
// launch options and bounded, discarded diagnostic context. Unknown fields,
// credentials in the body and nondefault launch effects are refused. Input is
// limited to 1 MiB, task text to 512 KiB and cluster names to 256 UTF-8 bytes.
// It does not parse YAML, resolve a cluster or authorize or submit work.
func DecodeLaunch(r io.Reader) (LaunchBody, error) { return decodeLaunch(r) }

func decodeLaunch(r io.Reader) (LaunchBody, error) {
	dag, cluster, err := decodeWorkload(r, true)
	if err != nil {
		return LaunchBody{}, err
	}
	return LaunchBody{DAG: dag, ClusterName: cluster}, nil
}

// DecodeExec reads a selected API67 exec body with the same input bounds and
// diagnostic rules as DecodeLaunch. Only default exec options are accepted.
// It does not parse YAML, resolve a cluster or authorize or submit work.
func DecodeExec(r io.Reader) (ExecBody, error) { return decodeExec(r) }

func decodeExec(r io.Reader) (ExecBody, error) {
	dag, cluster, err := decodeWorkload(r, false)
	if err != nil {
		return ExecBody{}, err
	}
	return ExecBody{DAG: dag, ClusterName: cluster}, nil
}

func decodeWorkload(r io.Reader, launch bool) (string, string, error) {
	if r == nil {
		return "", "", invalid("")
	}
	input := &workloadReader{Reader: r}
	data, err := readJSON(input)
	if input.err != nil {
		return "", "", &workloadReadError{cause: input.err}
	}
	if err != nil {
		return "", "", err
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return "", "", invalid("")
	}
	var dag, cluster string
	for key, value := range fields {
		switch key {
		case "task":
			dag, err = text(value, maxDAG, key)
		case "cluster_name":
			cluster, err = text(value, 256, key)
		case "dryrun", "down":
			err = workloadFalse(value, key)
		case "backend":
			err = workloadNull(value, key)
		case "retry_until_up", "no_setup", "fast", "quiet_optimizer", "is_launched_by_jobs_controller", "is_launched_by_sky_serve_controller", "resize", "include_credentials":
			if !launch {
				err = unsupported("")
			} else {
				err = workloadFalse(value, key)
			}
		case "idle_minutes_to_autostop", "clone_disk_from":
			if !launch {
				err = unsupported("")
			} else {
				err = workloadNull(value, key)
			}
		case "optimize_target":
			if !launch {
				err = unsupported("")
			} else if string(value) != "0" {
				err = unsupported(key)
			}
		case "extra_launch_context":
			if !launch {
				err = unsupported("")
			} else {
				err = workloadEmptyObject(value, key, false)
			}
		default:
			err = workloadContext(key, value)
		}
		if err != nil {
			return "", "", err
		}
	}
	if dag == "" {
		return "", "", invalid("task")
	}
	if strings.TrimSpace(cluster) == "" || strings.ContainsRune(cluster, 0) {
		return "", "", invalid("cluster_name")
	}
	return dag, cluster, nil
}

// Context fields follow DecodeValidate's selected profile. They never become
// task environment, server configuration or caller authority.
func workloadContext(key string, value json.RawMessage) error {
	switch key {
	case "env_vars":
		return environment(value)
	case "entrypoint", "entrypoint_command":
		_, err := text(value, 4096, key)
		return err
	case "using_remote_api_server":
		if string(value) != "true" && string(value) != "false" {
			return invalid(key)
		}
		return nil
	case "override_skypilot_config":
		return workloadEmptyObject(value, key, true)
	case "override_skypilot_config_path":
		if isNull(value) {
			return nil
		}
		_, err := text(value, 4096, key)
		return err
	case "file_mounts_blob_id", "client_api_version", "workspace_access", "request_options":
		return workloadNull(value, key)
	default:
		return unsupported("")
	}
}

func workloadFalse(value json.RawMessage, key string) error {
	if string(value) == "false" {
		return nil
	}
	if string(value) == "true" {
		return unsupported(key)
	}
	return invalid(key)
}

func workloadNull(value json.RawMessage, key string) error {
	if isNull(value) {
		return nil
	}
	return unsupported(key)
}

func workloadEmptyObject(value json.RawMessage, key string, nullable bool) error {
	if nullable && isNull(value) {
		return nil
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(value, &object) != nil || object == nil {
		return invalid(key)
	}
	if len(object) != 0 {
		return unsupported(key)
	}
	return nil
}

type workloadReader struct {
	io.Reader
	err error
}

func (r *workloadReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err != nil && err != io.EOF && r.err == nil {
		r.err = err
	}
	return n, err
}

type workloadReadError struct{ cause error }

func (e *workloadReadError) Error() string { return "wire: read workload request body" }
func (e *workloadReadError) Unwrap() error { return e.cause }
