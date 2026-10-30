package wire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const workloadSDKPin = "922a9f046c4a2714edf2b76fec3d1862dcd56ae3"

// This constructs actual upstream request models, without patching the SDK or
// making an HTTP request. It does not qualify sdk.launch or workload execution.
const workloadSDKModels = `import json
import sky
from sky.server.requests import payloads
assert sky.__commit__ == "922a9f046c4a2714edf2b76fec3d1862dcd56ae3"
context = dict(env_vars={"SKYPILOT_DISABLE_USAGE_COLLECTION": "true"},
    entrypoint="sdk-constructor-capture", entrypoint_command="sdk-constructor-capture",
    using_remote_api_server=True, override_skypilot_config={},
    override_skypilot_config_path=None)
models = {"launch": payloads.LaunchBody(task="run: echo inert\n", cluster_name="sdk-example", **context),
    "exec": payloads.ExecBody(task="run: echo inert\n", cluster_name="sdk-example", **context)}
print(json.dumps({name: model.model_dump_json() for name, model in models.items()}, sort_keys=True))
`

func TestWorkloadSDKFixtures(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "testdata", "wire", "workloads", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Upstream string `json:"upstream"`
		Fixtures []struct {
			File, SHA256, Kind, Status string
		}
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Upstream != workloadSDKPin || len(manifest.Fixtures) != 2 {
		t.Fatal("invalid fixture provenance")
	}
	for _, row := range manifest.Fixtures {
		t.Run(row.Kind, func(t *testing.T) {
			if filepath.Base(row.File) != row.File || row.Status != "SDK_PAYLOAD_CAPTURED" {
				t.Fatal("invalid fixture row")
			}
			body, err := os.ReadFile(filepath.Join("..", "testdata", "wire", "workloads", row.File))
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(body)
			if hex.EncodeToString(sum[:]) != row.SHA256 {
				t.Fatal("fixture hash mismatch")
			}
			if row.Kind != "launch" && row.Kind != "exec" {
				t.Fatal("unknown fixture kind")
			}
			dag, cluster, err := workloadDecode(bytes.NewReader(body), row.Kind == "launch")
			if err != nil || dag != "run: echo inert\n" || cluster != "sdk-example" {
				t.Fatalf("captured body: %v", err)
			}
		})
	}
}

func TestPinnedSDKWorkloadModels(t *testing.T) {
	python, source := os.Getenv("SKYCOMPAT_SDK_PYTHON"), os.Getenv("SKYCOMPAT_SDK_SOURCE")
	if python == "" || source == "" {
		t.Skip("set SKYCOMPAT_SDK_PYTHON and SKYCOMPAT_SDK_SOURCE for unmodified pinned SDK model qualification")
	}
	home := t.TempDir()
	env := []string{"HOME=" + home, "PATH=" + os.Getenv("PATH"), "PYTHONPATH=" + source,
		"PYTHONDONTWRITEBYTECODE=1", "SKYPILOT_DISABLE_USAGE_COLLECTION=1",
		"SKY_RUNTIME_DIR=" + filepath.Join(home, "runtime"), "XDG_CACHE_HOME=" + filepath.Join(home, "cache")}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, "-c", workloadSDKModels)
	cmd.Env = env
	cmd.WaitDelay = time.Second
	var output, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal("start pinned SDK model oracle:", err)
	}
	pid := cmd.Process.Pid
	if err := cmd.Wait(); err != nil {
		t.Fatalf("pinned SDK model oracle pid=%d: %v", pid, err)
	}
	t.Logf("pinned SDK model oracle pid=%d Wait0, stdout/stderr closed", pid)
	var models map[string]string
	if err := json.Unmarshal(output.Bytes(), &models); err != nil || len(models) != 2 {
		t.Fatal("invalid SDK model capture")
	}
	for _, kind := range []string{"launch", "exec"} {
		body, ok := models[kind]
		if !ok {
			t.Fatal("missing SDK model")
		}
		dag, cluster, err := workloadDecode(strings.NewReader(body), kind == "launch")
		if err != nil || dag != "run: echo inert\n" || cluster != "sdk-example" {
			t.Fatalf("actual SDK %s model: %v", kind, err)
		}
	}
}
