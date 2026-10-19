package wire

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
)

// ValidateBody contains the DAG YAML accepted from an upstream validation body.
// The zero value is invalid. YAML validation is the caller's responsibility.
type ValidateBody struct{ DAG string }

// DecodeValidate reads at most 1 MiB and accepts exactly the selected API67
// validation fields. DAG text is limited to 512 KiB; JSON nesting to 32 levels
// and tokens to 65536. Duplicate and unknown fields fail. Recognized display and
// telemetry context is bounded and discarded, never used as identity or env.
// Nondefault configuration, policy, upload, or authority fields are unsupported.
func DecodeValidate(r io.Reader) (ValidateBody, error) { return decodeValidate(r) }

func decodeValidate(r io.Reader) (ValidateBody, error) {
	data, err := readJSON(r)
	if err != nil {
		return ValidateBody{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return ValidateBody{}, invalid("")
	}
	var out ValidateBody
	for key, value := range fields {
		var err error
		switch key {
		case "dag":
			out.DAG, err = text(value, maxDAG, key)
		case "env_vars":
			err = environment(value)
		case "entrypoint", "entrypoint_command":
			_, err = text(value, 4096, key)
		case "using_remote_api_server":
			if string(value) != "true" && string(value) != "false" {
				err = invalid(key)
			}
		case "override_skypilot_config":
			if !isNull(value) {
				var config map[string]json.RawMessage
				if json.Unmarshal(value, &config) != nil || config == nil {
					err = invalid(key)
				} else if len(config) != 0 {
					err = unsupported(key)
				}
			}
		case "override_skypilot_config_path", "file_mounts_blob_id", "client_api_version", "workspace_access", "request_options":
			if !isNull(value) {
				err = unsupported(key)
			}
		default:
			err = unsupported("")
		}
		if err != nil {
			return ValidateBody{}, err
		}
	}
	if out.DAG == "" {
		return ValidateBody{}, invalid("dag")
	}
	return out, nil
}

func text(raw json.RawMessage, size int, field string) (string, error) {
	var value string
	if isNull(raw) || json.Unmarshal(raw, &value) != nil {
		return "", invalid(field)
	}
	if len(value) > size {
		return "", limit(field)
	}
	return value, nil
}
func isNull(raw json.RawMessage) bool { return bytes.Equal(bytes.TrimSpace(raw), []byte("null")) }

func environment(raw json.RawMessage) error {
	var env map[string]json.RawMessage
	if json.Unmarshal(raw, &env) != nil || env == nil {
		return invalid("env_vars")
	}
	if len(env) > 5 {
		return limit("env_vars")
	}
	for name, raw := range env {
		switch name {
		case "SKYPILOT_USER_ID", "SKYPILOT_USER", "SKYPILOT_USAGE_RUN_ID", "SKYPILOT_CLIENT_USER_HASH", "SKYPILOT_DISABLE_USAGE_COLLECTION":
		default:
			return unsupported("env_vars")
		}
		value, err := text(raw, 1024, "env_vars")
		if err != nil {
			return err
		}
		if name == "SKYPILOT_DISABLE_USAGE_COLLECTION" {
			switch strings.ToLower(value) {
			case "true", "false", "0", "1":
			default:
				return unsupported("env_vars")
			}
		}
	}
	return nil
}
