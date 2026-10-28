package kube

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// Source-only; NOT_RUN.

// environmentProfileRecord is one valid profile record for principal and
// alias, as YAML list item lines.
func environmentProfileRecord(target, principal, alias string) string {
	return fmt.Sprintf(`- target: %s
  revision: profile-1
  principal: %s
  storage_alias: %s
  namespace_uid: namespace-uid-1
  image: %s
  idle_argv: [/usr/local/bin/idle, --forever]
  pvc_name: alice-home
  pvc_uid: claim-uid-1
  volume_name: pv-alice-home
  volume_uid: pv-uid-1
  access_mode: ReadWriteOnce
  service_account_uid: service-account-uid-1
  image_pull_secrets: [registry-b, registry-a]
`, target, principal, alias, environmentTestImage)
}

func environmentConfigText(records ...string) string {
	return "version: 1\nartifacts: false\nprofiles:\n" + strings.Join(records, "")
}

func TestReadEnvironmentConfigData(t *testing.T) {
	valid := environmentConfigText(environmentProfileRecord("kube", "alice", "home"))
	cfg, err := readEnvironmentConfig([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	want := environmentTestProfile()
	want.volumeUID = "pv-uid-1"
	if cfg.target != "kube" || cfg.artifacts || len(cfg.profiles) != 1 {
		t.Fatalf("config = %+v", cfg)
	}
	got := cfg.profiles[0]
	if got.revision != want.revision || got.namespaceUID != want.namespaceUID || got.principal != want.principal ||
		got.image != want.image || !slices.Equal(got.idleArgv, want.idleArgv) || got.alias != want.alias ||
		got.pvcName != want.pvcName || got.pvcUID != want.pvcUID || got.volumeName != want.volumeName || got.volumeUID != want.volumeUID ||
		got.accessMode != want.accessMode || got.mountPath != want.mountPath ||
		got.serviceAccountUID != want.serviceAccountUID || !slices.Equal(got.pullSecrets, want.pullSecrets) {
		t.Fatalf("profile = %+v, want %+v", got, want)
	}
	// Copies are independent of the config.
	copies := cfg.podProfiles()
	copies[0].idleArgv[0] = "/changed"
	if cfg.profiles[0].idleArgv[0] == "/changed" {
		t.Fatal("podProfiles shares its argv")
	}
	noSecrets := strings.Replace(valid, "  image_pull_secrets: [registry-b, registry-a]\n", "", 1)
	if cfg, err := readEnvironmentConfig([]byte(noSecrets)); err != nil || cfg.profiles[0].pullSecrets == nil || len(cfg.profiles[0].pullSecrets) != 0 {
		t.Fatalf("absent pull secrets = %v, %v; want an empty, non-nil list", cfg.profiles, err)
	}
	two := environmentConfigText(environmentProfileRecord("kube", "alice", "home"), environmentProfileRecord("kube", "bob", "home"))
	if cfg, err := readEnvironmentConfig([]byte(two)); err != nil || len(cfg.profiles) != 2 {
		t.Fatalf("two principals = %v, %v", cfg.profiles, err)
	}

	many := make([]string, maxEnvironmentProfiles+1)
	for i := range many {
		many[i] = environmentProfileRecord("kube", "alice", fmt.Sprintf("home-%d", i))
	}
	for _, tt := range []struct {
		name, data string
	}{
		{"empty", ""},
		{"not a mapping", "- version\n"},
		{"unknown top-level field", valid + "extra: true\n"},
		{"unknown record field", strings.Replace(valid, "  revision:", "  mount_path: /x\n  revision:", 1)},
		{"duplicate key", strings.Replace(valid, "artifacts: false\n", "artifacts: false\nartifacts: true\n", 1)},
		{"duplicate record key", strings.Replace(valid, "  revision: profile-1\n", "  revision: profile-1\n  revision: profile-2\n", 1)},
		{"anchor and alias", strings.Replace(valid, "  pvc_name: alice-home", "  pvc_name: &n alice-home\n  volume_name_alias: *n", 1)},
		{"two documents", valid + "---\n" + valid},
		{"version 2", strings.Replace(valid, "version: 1", "version: 2", 1)},
		{"string version", strings.Replace(valid, "version: 1", `version: "1"`, 1)},
		{"string artifacts", strings.Replace(valid, "artifacts: false", `artifacts: "false"`, 1)},
		{"integer scalar", strings.Replace(valid, "revision: profile-1", "revision: 1", 1)},
		{"no profiles", "version: 1\nprofiles: []\n"},
		{"too many profiles", environmentConfigText(many...)},
		{"duplicate principal and alias", environmentConfigText(environmentProfileRecord("kube", "alice", "home"), environmentProfileRecord("kube", "alice", "home"))},
		{"two targets", environmentConfigText(environmentProfileRecord("kube", "alice", "home"), environmentProfileRecord("other", "bob", "home"))},
		{"local target", environmentConfigText(environmentProfileRecord("local", "alice", "home"))},
		{"invalid principal", environmentConfigText(environmentProfileRecord("kube", "Alice", "home"))},
		{"access mode", strings.Replace(valid, "ReadWriteOnce", "ReadWriteMany", 1)},
		{"missing volume uid", strings.Replace(valid, "  volume_uid: pv-uid-1\n", "", 1)},
		{"empty volume uid", strings.Replace(valid, "volume_uid: pv-uid-1", `volume_uid: ""`, 1)},
		{"oversize volume uid", strings.Replace(valid, "volume_uid: pv-uid-1", "volume_uid: "+strings.Repeat("u", 129), 1)},
		{"relative idle argv", strings.Replace(valid, "[/usr/local/bin/idle, --forever]", "[idle]", 1)},
		{"tag image", strings.Replace(valid, environmentTestImage, "registry.example.com/env/idle:latest", 1)},
		{"duplicate pull secret", strings.Replace(valid, "[registry-b, registry-a]", "[registry-a, registry-a]", 1)},
		{"oversize", valid + "# " + strings.Repeat("x", maxEnvironmentConfigBytes) + "\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if cfg, err := readEnvironmentConfig([]byte(tt.data)); err == nil {
				t.Fatalf("accepted: %+v", cfg)
			} else if strings.Contains(err.Error(), "alice-home") || strings.Contains(err.Error(), environmentTestImage) {
				t.Fatalf("error %q names a configuration value", err)
			}
		})
	}
}

func TestReadEnvironmentConfigFile(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("environment profiles require linux or darwin")
	}
	dir := t.TempDir()
	valid := []byte(environmentConfigText(environmentProfileRecord("kube", "alice", "home")))
	write := func(name string, data []byte, mode os.FileMode) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		return path
	}
	for _, mode := range []os.FileMode{0o400, 0o600} {
		path := write(fmt.Sprintf("private-%o.yaml", mode), valid, mode)
		if cfg, err := ReadEnvironmentConfig(path); err != nil || cfg.target != "kube" {
			t.Fatalf("mode %o = %v, %v", mode, cfg.target, err)
		}
	}
	private := write("profiles.yaml", valid, 0o600)
	link := filepath.Join(dir, "link.yaml")
	if err := os.Symlink(private, link); err != nil {
		t.Fatal(err)
	}
	hard := filepath.Join(dir, "hard.yaml")
	if err := os.Link(write("linked.yaml", valid, 0o600), hard); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct{ name, path string }{
		{"missing", filepath.Join(dir, "missing.yaml")},
		{"symbolic link", link},
		{"hard link", hard},
		{"group readable", write("group.yaml", valid, 0o640)},
		{"world readable", write("world.yaml", valid, 0o644)},
		{"executable", write("exec.yaml", valid, 0o700)},
		{"empty", write("empty.yaml", nil, 0o600)},
		{"directory", dir},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ReadEnvironmentConfig(tt.path); err == nil {
				t.Fatal("accepted an unsafe profile file")
			} else if strings.Contains(err.Error(), dir) {
				t.Fatalf("error %q names the file", err)
			}
		})
	}
}
