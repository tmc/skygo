package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDecodeValidateDefaults(t *testing.T) {
	for _, name := range []string{"validate-minimal.json", "validate-sdk-remote-captured.json"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("..", "testdata", "wire", name))
			if err != nil {
				t.Fatal(err)
			}
			var want struct {
				DAG string `json:"dag"`
			}
			if err := json.Unmarshal(data, &want); err != nil {
				t.Fatal(err)
			}
			got, err := DecodeValidate(bytes.NewReader(data))
			if err != nil || got.DAG != want.DAG {
				t.Fatalf("body=%+v, error=%v", got, err)
			}
		})
	}
	for _, value := range []string{"true", "TRUE", "false", "False", "0", "1"} {
		data := `{"dag":"run: true","env_vars":{"SKYPILOT_DISABLE_USAGE_COLLECTION":"` + value + `"}}`
		if _, err := DecodeValidate(strings.NewReader(data)); err != nil {
			t.Errorf("telemetry %q: %v", value, err)
		}
	}
}
func TestDecodeValidateConfigProvenance(t *testing.T) {
	const dag = "run: echo hello"
	want := ValidateBody{DAG: dag}
	// Neither an existing client file nor an absent path may be loaded or
	// merged as server configuration. Only the DAG belongs in ValidateBody.
	path := filepath.Join(t.TempDir(), "client-config.yaml")
	if err := os.WriteFile(path, []byte("secret-canary: unsupported configuration\n"), 0600); err != nil {
		t.Fatal(err)
	}
	serialized, err := json.Marshal([]any{path, nil})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{
		nil, "", string(serialized), filepath.Join(t.TempDir(), "absent.yaml"),
		"not inner JSON: {", strings.Repeat("x", 4096), strings.Repeat("é", 2048),
	} {
		data, err := json.Marshal(map[string]any{"dag": dag, "override_skypilot_config_path": value})
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeValidate(bytes.NewReader(data))
		if err != nil || got != want {
			t.Fatalf("provenance affected validation body: %v", err)
		}
	}
	// The boundary counts decoded UTF-8 bytes, not code points or JSON escapes.
	data, err := json.Marshal(map[string]any{"dag": dag, "override_skypilot_config_path": strings.Repeat("é", 2049)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = DecodeValidate(bytes.NewReader(data))
	var limit *DecodeError
	if !errors.As(err, &limit) || limit.Code != "limit" || limit.Field != "override_skypilot_config_path" {
		t.Fatalf("multibyte provenance limit: %v", err)
	}
}

func TestDecodeValidateInvalid(t *testing.T) {
	for _, tt := range []struct{ name, input, code, field string }{
		{"empty", "", "invalid", ""}, {"null", "null", "invalid", ""}, {"array", "[]", "invalid", ""},
		{"missing", "{}", "invalid", "dag"}, {"null dag", `{"dag":null}`, "invalid", "dag"},
		{"numeric dag", `{"dag":1}`, "invalid", "dag"}, {"empty dag", `{"dag":""}`, "invalid", "dag"},
		{"duplicate", `{"dag":"a","dag":"b"}`, "invalid", ""},
		{"nested duplicate", `{"dag":"a","override_skypilot_config":{"x":1,"x":2}}`, "invalid", ""},
		{"escaped duplicate", `{"dag":"a","d\u0061g":"b"}`, "invalid", ""},
		{"trailing", `{"dag":"a"} {}`, "invalid", ""},
		{"unknown", `{"dag":"a","secret-canary":true}`, "unsupported", ""},
		{"entrypoint type", `{"dag":"a","entrypoint":null}`, "invalid", "entrypoint"},
		{"remote type", `{"dag":"a","using_remote_api_server":null}`, "invalid", "using_remote_api_server"},
		{"config", `{"dag":"a","override_skypilot_config":{"secret-canary":true}}`, "unsupported", "override_skypilot_config"},
		{"config type", `{"dag":"a","override_skypilot_config":[]}`, "invalid", "override_skypilot_config"},
		{"config path number", `{"dag":"a","override_skypilot_config_path":1}`, "invalid", "override_skypilot_config_path"},
		{"config path boolean", `{"dag":"a","override_skypilot_config_path":true}`, "invalid", "override_skypilot_config_path"},
		{"config path array", `{"dag":"a","override_skypilot_config_path":["secret-canary"]}`, "invalid", "override_skypilot_config_path"},
		{"config path object", `{"dag":"a","override_skypilot_config_path":{"secret-canary":true}}`, "invalid", "override_skypilot_config_path"},
		{"config path oversized", `{"dag":"a","override_skypilot_config_path":"` + strings.Repeat("secret-canary", 342) + `"}`, "limit", "override_skypilot_config_path"},
		{"authority", `{"dag":"a","workspace_access":"admin-secret-canary"}`, "unsupported", "workspace_access"},
		{"client version", `{"dag":"a","client_api_version":67}`, "unsupported", "client_api_version"},
		{"upload", `{"dag":"a","file_mounts_blob_id":"secret-canary"}`, "unsupported", "file_mounts_blob_id"},
		{"policy", `{"dag":"a","request_options":{}}`, "unsupported", "request_options"},
		{"env type", `{"dag":"a","env_vars":null}`, "invalid", "env_vars"},
		{"env value type", `{"dag":"a","env_vars":{"SKYPILOT_USER":1}}`, "invalid", "env_vars"},
		{"unknown env", `{"dag":"a","env_vars":{"secret-canary":"v"}}`, "unsupported", "env_vars"},
		{"effectful env", `{"dag":"a","env_vars":{"SKYPILOT_CONFIG":"secret-canary"}}`, "unsupported", "env_vars"},
		{"telemetry spelling", `{"dag":"a","env_vars":{"SKYPILOT_DISABLE_USAGE_COLLECTION":"yes"}}`, "unsupported", "env_vars"},
		{"incoming pickle", `{"dag":"a","request_body":"gAROLg=="}`, "unsupported", ""},
		{"surrogate", `{"dag":"\ud800"}`, "invalid", ""},
		{"low surrogate", `{"dag":"\udc00"}`, "invalid", ""},
		{"invalid utf8", "{\"dag\":\"\xff\"}", "invalid", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeValidate(strings.NewReader(tt.input))
			var e *DecodeError
			if !errors.As(err, &e) || e.Code != tt.code || e.Field != tt.field {
				t.Fatalf("body=%+v,error=%v, want %s/%s", got, err, tt.code, tt.field)
			}
			if got.DAG != "" || strings.Contains(err.Error(), "secret-canary") {
				t.Fatalf("partial data or unsafe error: %v", err)
			}
		})
	}
	for _, name := range []string{"validate-controller-claim.json", "validate-unknown-effect.json"} {
		data, err := os.ReadFile(filepath.Join("..", "testdata", "wire", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeValidate(bytes.NewReader(data)); err == nil {
			t.Fatalf("accepted %s", name)
		}
	}
	for _, text := range []string{`{"dag":"\ud83d\ude00"}`, `{"dag":"\\ud800"}`} {
		if _, err := DecodeValidate(strings.NewReader(text)); err != nil {
			t.Fatalf("valid escaped unicode: %v", err)
		}
	}
}

func TestDecodeValidateLimits(t *testing.T) {
	for _, tt := range []struct{ name, input string }{
		{"body", strings.Repeat(" ", maxBody+1)},
		{"dag", `{"dag":"` + strings.Repeat("x", maxDAG+1) + `"}`},
		{"entrypoint", `{"dag":"a","entrypoint":"` + strings.Repeat("x", 4097) + `"}`},
		{"env value", `{"dag":"a","env_vars":{"SKYPILOT_USER":"` + strings.Repeat("x", 1025) + `"}}`},
		{"env count", `{"dag":"a","env_vars":{"a":"","b":"","c":"","d":"","e":"","f":""}}`},
		{"depth", strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34)},
		{"tokens", "[" + strings.Repeat("0,", maxTokens) + "0]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeValidate(strings.NewReader(tt.input))
			var e *DecodeError
			if !errors.As(err, &e) || e.Code != "limit" {
				t.Fatalf("got %v", err)
			}
		})
	}
	data := `{"dag":"` + strings.Repeat("x", maxDAG) + `"}`
	if _, err := DecodeValidate(strings.NewReader(data)); err != nil {
		t.Fatalf("boundary: %v", err)
	}
	r := &countReader{Reader: strings.NewReader(strings.Repeat(" ", maxBody*2))}
	if _, err := DecodeValidate(r); err == nil || r.count != maxBody+1 {
		t.Fatalf("read=%d,error=%v", r.count, err)
	}
	if _, err := DecodeValidate(errorReader{}); err == nil || strings.Contains(err.Error(), "secret-canary") {
		t.Fatalf("unsafe or missing read error: %v", err)
	}
	if _, err := DecodeValidate(nil); err == nil {
		t.Fatal("nil reader accepted")
	}
}

type countReader struct {
	io.Reader
	count int
}

func (r *countReader) Read(p []byte) (int, error) {
	n, e := r.Reader.Read(p)
	r.count += n
	return n, e
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("secret-canary") }
