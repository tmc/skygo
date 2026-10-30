package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

func workloadDecode(r io.Reader, launch bool) (string, string, error) {
	if launch {
		v, err := DecodeLaunch(r)
		return v.DAG, v.ClusterName, err
	}
	v, err := DecodeExec(r)
	return v.DAG, v.ClusterName, err
}

func TestDecodeWorkloadPreserves(t *testing.T) {
	for _, launch := range []bool{true, false} {
		name := "exec"
		if launch {
			name = "launch"
		}
		t.Run(name, func(t *testing.T) {
			for _, cluster := range []string{"example", "Case-Exact", "café", " x ", strings.Repeat("x", 256), strings.Repeat("é", 128)} {
				want := "name: inert\n---\nrun: |\n  echo 'hello'\n  printf '\\n'\n"
				data, err := json.Marshal(map[string]any{"task": want, "cluster_name": cluster})
				if err != nil {
					t.Fatal(err)
				}
				dag, got, err := workloadDecode(bytes.NewReader(data), launch)
				if err != nil || dag != want || got != cluster {
					t.Fatalf("preserved dag=%t, cluster=%t, error=%v", dag == want, got == cluster, err)
				}
			}
		})
	}
}

func TestDecodeWorkloadDefaults(t *testing.T) {
	fields := map[string]any{
		"task": "run: inert", "cluster_name": "example", "dryrun": false, "down": false, "backend": nil,
		"env_vars":   map[string]string{"SKYPILOT_USER_ID": "diagnostic-only", "SKYPILOT_USER": "diagnostic-only", "SKYPILOT_USAGE_RUN_ID": "diagnostic-only", "SKYPILOT_CLIENT_USER_HASH": "diagnostic-only", "SKYPILOT_DISABLE_USAGE_COLLECTION": "true"},
		"entrypoint": "sdk.launch", "entrypoint_command": "diagnostic command", "using_remote_api_server": true,
		"override_skypilot_config": map[string]any{}, "override_skypilot_config_path": "[client-path-provenance]",
		"file_mounts_blob_id": nil, "client_api_version": nil, "workspace_access": nil, "request_options": nil,
	}
	for _, launch := range []bool{false, true} {
		if launch {
			for _, key := range []string{"retry_until_up", "no_setup", "fast", "quiet_optimizer", "is_launched_by_jobs_controller", "is_launched_by_sky_serve_controller", "resize", "include_credentials"} {
				fields[key] = false
			}
			fields["idle_minutes_to_autostop"] = nil
			fields["clone_disk_from"] = nil
			fields["optimize_target"] = 0
			fields["extra_launch_context"] = map[string]any{}
		}
		data, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		dag, cluster, err := workloadDecode(bytes.NewReader(data), launch)
		if err != nil || dag != "run: inert" || cluster != "example" {
			t.Fatalf("launch=%t, error=%v", launch, err)
		}
	}
	for _, raw := range []string{"null", "{}", "{ }"} {
		data := `{"task":"inert","cluster_name":"example","override_skypilot_config":` + raw + `}`
		if _, err := DecodeLaunch(strings.NewReader(data)); err != nil {
			t.Fatalf("config default: %v", err)
		}
	}
}

func TestDecodeWorkloadRefusals(t *testing.T) {
	cases := []struct{ name, extra, code, field string }{
		{"unknown", `,"private-secret-marker":true`, "unsupported", ""},
		{"body_token", `,"env_vars":{"SKYPILOT_SERVICE_ACCOUNT_TOKEN":"private-secret-marker"}`, "unsupported", "env_vars"},
		{"env_null", `,"env_vars":null`, "invalid", "env_vars"},
		{"env_number", `,"env_vars":{"SKYPILOT_USER":1}`, "invalid", "env_vars"},
		{"env_config", `,"env_vars":{"SKYPILOT_CONFIG":"private-secret-marker"}`, "unsupported", "env_vars"},
		{"telemetry", `,"env_vars":{"SKYPILOT_DISABLE_USAGE_COLLECTION":"yes"}`, "unsupported", "env_vars"},
		{"config", `,"override_skypilot_config":{"private-secret-marker":true}`, "unsupported", "override_skypilot_config"},
		{"config_type", `,"override_skypilot_config":[]`, "invalid", "override_skypilot_config"},
		{"path_type", `,"override_skypilot_config_path":[]`, "invalid", "override_skypilot_config_path"},
		{"entrypoint_null", `,"entrypoint":null`, "invalid", "entrypoint"},
		{"remote_null", `,"using_remote_api_server":null`, "invalid", "using_remote_api_server"},
		{"authority", `,"workspace_access":"admin-private-secret-marker"`, "unsupported", "workspace_access"},
		{"api_version", `,"client_api_version":67`, "unsupported", "client_api_version"},
		{"upload", `,"file_mounts_blob_id":"private-secret-marker"`, "unsupported", "file_mounts_blob_id"},
		{"options", `,"request_options":{}`, "unsupported", "request_options"},
		{"backend", `,"backend":"private-secret-marker"`, "unsupported", "backend"},
		{"backend_empty", `,"backend":""`, "unsupported", "backend"},
		{"dryrun", `,"dryrun":true`, "unsupported", "dryrun"},
		{"dryrun_type", `,"dryrun":0`, "invalid", "dryrun"},
		{"down", `,"down":true`, "unsupported", "down"},
		{"down_null", `,"down":null`, "invalid", "down"},
	}
	for _, launch := range []bool{false, true} {
		name := "exec"
		if launch {
			name = "launch"
		}
		t.Run(name, func(t *testing.T) {
			for _, tt := range cases {
				t.Run(tt.name, func(t *testing.T) {
					input := `{"task":"run: inert","cluster_name":"example"` + tt.extra + `}`
					dag, cluster, err := workloadDecode(strings.NewReader(input), launch)
					var e *DecodeError
					if !errors.As(err, &e) || e.Code != tt.code || e.Field != tt.field || dag != "" || cluster != "" {
						t.Fatalf("partial=%t, error=%v, want %s/%s", dag != "" || cluster != "", err, tt.code, tt.field)
					}
					if strings.Contains(err.Error(), "private-secret-marker") {
						t.Fatal("error exposed input")
					}
				})
			}
		})
	}
}

func TestDecodeLaunchEffects(t *testing.T) {
	for _, field := range []string{"retry_until_up", "no_setup", "fast", "quiet_optimizer", "is_launched_by_jobs_controller", "is_launched_by_sky_serve_controller", "resize", "include_credentials"} {
		t.Run(field, func(t *testing.T) {
			for _, value := range []string{"true", "null", "0", `"false"`} {
				data := `{"task":"inert","cluster_name":"example","` + field + `":` + value + `}`
				if v, err := DecodeLaunch(strings.NewReader(data)); err == nil || v != (LaunchBody{}) {
					t.Fatal("accepted nondefault launch effect")
				}
			}
			data := `{"task":"inert","cluster_name":"example","` + field + `":false}`
			if v, err := DecodeExec(strings.NewReader(data)); err == nil || v != (ExecBody{}) {
				t.Fatal("exec accepted launch-only field")
			}
		})
	}
	for _, tt := range []struct{ field, value string }{
		{"idle_minutes_to_autostop", "0"}, {"clone_disk_from", `"other"`},
		{"optimize_target", "1"}, {"optimize_target", `"COST"`}, {"optimize_target", "null"}, {"optimize_target", "0.0"},
		{"extra_launch_context", `{"controller":true}`}, {"extra_launch_context", "null"}, {"extra_launch_context", "[]"},
	} {
		t.Run(tt.field+"_"+tt.value, func(t *testing.T) {
			data := `{"task":"inert","cluster_name":"example","` + tt.field + `":` + tt.value + `}`
			if _, err := DecodeLaunch(strings.NewReader(data)); err == nil {
				t.Fatal("accepted launch effect")
			}
		})
	}
	for _, field := range []string{"idle_minutes_to_autostop", "clone_disk_from", "optimize_target", "extra_launch_context"} {
		t.Run("exec_"+field, func(t *testing.T) {
			if _, err := DecodeExec(strings.NewReader(`{"task":"inert","cluster_name":"example","` + field + `":null}`)); err == nil {
				t.Fatal("exec accepted launch field")
			}
		})
	}
}

func TestDecodeWorkloadMalformed(t *testing.T) {
	for _, launch := range []bool{false, true} {
		for _, input := range []string{
			"", "null", "[]", "{}", `{"task":"inert"}`, `{"cluster_name":"example"}`,
			`{"task":null,"cluster_name":"example"}`, `{"task":1,"cluster_name":"example"}`,
			`{"task":"","cluster_name":"example"}`, `{"task":"inert","cluster_name":null}`,
			`{"task":"inert","cluster_name":3}`, `{"task":"inert","cluster_name":""}`,
			`{"task":"inert","cluster_name":"  \n "}`, `{"task":"inert","cluster_name":"a\u0000b"}`,
			`{"task":"inert","cluster_name":"a","task":"other"}`,
			`{"task":"inert","cluster_name":"a","t\u0061sk":"other"}`,
			`{"task":"inert","cluster_name":"a","extra_launch_context":{"x":1,"x":2}}`,
			`{"task":"inert","cluster_name":"a"} {}`, `{"task":"\ud800","cluster_name":"a"}`,
			"{\"task\":\"\xff\",\"cluster_name\":\"a\"}",
		} {
			dag, cluster, err := workloadDecode(strings.NewReader(input), launch)
			if err == nil || dag != "" || cluster != "" {
				t.Fatalf("launch=%t malformed accepted or partial result", launch)
			}
		}
	}
}

func TestDecodeWorkloadLimits(t *testing.T) {
	for _, tt := range []struct{ name, input string }{
		{"body", strings.Repeat(" ", maxBody+1)},
		{"task", `{"task":"` + strings.Repeat("x", maxDAG+1) + `","cluster_name":"a"}`},
		{"cluster", `{"task":"inert","cluster_name":"` + strings.Repeat("é", 129) + `"}`},
		{"entrypoint", `{"task":"inert","cluster_name":"a","entrypoint":"` + strings.Repeat("x", 4097) + `"}`},
		{"provenance", `{"task":"inert","cluster_name":"a","override_skypilot_config_path":"` + strings.Repeat("x", 4097) + `"}`},
		{"environment", `{"task":"inert","cluster_name":"a","env_vars":{"SKYPILOT_USER":"` + strings.Repeat("x", 1025) + `"}}`},
		{"depth", strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34)},
		{"tokens", "[" + strings.Repeat("0,", maxTokens) + "0]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, launch := range []bool{false, true} {
				dag, cluster, err := workloadDecode(strings.NewReader(tt.input), launch)
				var e *DecodeError
				if !errors.As(err, &e) || e.Code != "limit" || dag != "" || cluster != "" {
					t.Fatalf("launch=%t, error=%v", launch, err)
				}
			}
		})
	}
	for _, launch := range []bool{false, true} {
		input := `{"task":"` + strings.Repeat("x", maxDAG) + `","cluster_name":"a"}`
		if _, _, err := workloadDecode(strings.NewReader(input), launch); err != nil {
			t.Fatalf("boundary: %v", err)
		}
		r := &countReader{Reader: strings.NewReader(strings.Repeat(" ", maxBody*2))}
		if _, _, err := workloadDecode(r, launch); err == nil || r.count != maxBody+1 {
			t.Fatalf("bounded read: %d, error=%v", r.count, err)
		}
	}
}

func TestDecodeWorkloadReadCause(t *testing.T) {
	cause := errors.New("private-secret-marker")
	for _, launch := range []bool{false, true} {
		for _, tail := range []error{cause, errors.Join(io.EOF, cause)} {
			r := &workloadErrorReader{cause: tail}
			dag, cluster, err := workloadDecode(r, launch)
			if !errors.Is(err, cause) || dag != "" || cluster != "" || strings.Contains(err.Error(), cause.Error()) {
				t.Fatalf("lost cause, partial result or unsafe text: %v", err)
			}
		}
		if _, _, err := workloadDecode(nil, launch); err == nil {
			t.Fatal("nil reader accepted")
		}
	}
}

type workloadErrorReader struct{ cause error }

func (r *workloadErrorReader) Read(p []byte) (int, error) {
	return copy(p, `{"task":"inert","cluster_name":"example"}`), r.cause
}
