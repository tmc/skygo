package bootstrap

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestValidPath(t *testing.T) {
	for _, p := range []string{"/d", "/srv/skygo-data", "/home/User_1/.skygo/auth.json", "/a/..b", "/" + strings.Repeat("a", 199)} {
		if !ValidPath(p) {
			t.Errorf("ValidPath(%q) = false", p)
		}
	}
	for _, p := range []string{"", "/", "d", "./d", "../d", "/d/", "//d", "/d/.", "/d/..", "/a/../b", "/a/./b", "/a b", "/a;b", "/a'b", "/a$b", "/a\nb", "/é", "/" + strings.Repeat("a", 200)} {
		if ValidPath(p) {
			t.Errorf("ValidPath(%q) = true", p)
		}
	}
	if _, err := command("start", "/srv/skygo-data"); err != nil {
		t.Errorf("command refused a valid path operand: %v", err)
	}
	for _, bad := range []string{"/a/../b", "/a b", "/a'b", "/"} {
		if _, err := command("start", bad); err == nil {
			t.Errorf("command accepted operand %q", bad)
		}
	}
}

// hostInstallation makes the layout of an initialized installation on the
// test host. It is a MODEL: the bootstrap checks only that the entries
// exist; the agent and server check their contents.
func hostInstallation(t *testing.T) Installation {
	t.Helper()
	base := t.TempDir()
	in := Installation{
		DataDir:     filepath.Join(base, "data"),
		AuthFile:    filepath.Join(base, "auth.json"),
		ArtifactDir: filepath.Join(base, "artifacts"),
	}
	for _, d := range []string{in.DataDir, in.ArtifactDir} {
		if err := os.Mkdir(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{filepath.Join(in.DataDir, "jobs.db"), in.AuthFile} {
		if err := os.WriteFile(f, []byte("model\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if in.validate() != nil {
		t.Skipf("temporary directory %s is not a valid installation path", base)
	}
	return in
}

// installationAgent returns an agent serving in, without a local token.
func installationAgent(t *testing.T, binary []byte, in Installation) Agent {
	t.Helper()
	a := newAgent(t, binary)
	delete(a.Local, "token")
	a.Installation = in
	return a
}

// installationRecord returns the configuration record a's first start
// writes for an installation.
func installationRecord(a Agent) string {
	i := a.Installation
	return configRecord(a) + "data " + i.DataDir + "\nauth " + i.AuthFile + "\nartifacts " + i.ArtifactDir + "\n"
}

// installFiles places a's files remotely without a status check.
func installFiles(t *testing.T, s *execServer, a Agent) {
	t.Helper()
	for _, name := range a.files() {
		sum, err := fileSum(a.Local[name])
		if err != nil {
			t.Fatal(err)
		}
		if err := put(context.Background(), s.runner(), a, name, sum, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
}

// scriptOperands returns the operands of cmd, which must run the script
// itself with each operand one quoted word.
func scriptOperands(t *testing.T, cmd string) []string {
	t.Helper()
	rest, ok := strings.CutPrefix(cmd, "sh -c "+quote(script)+" skygo-bootstrap ")
	if !ok {
		t.Fatalf("command does not run the script: %.80q", cmd)
	}
	var ops []string
	for _, w := range strings.Split(rest, " ") {
		if len(w) < 2 || w[0] != '\'' || w[len(w)-1] != '\'' || strings.Contains(w[1:len(w)-1], "'") {
			t.Fatalf("operand %q is not one quoted word", w)
		}
		ops = append(ops, w[1:len(w)-1])
	}
	return ops
}

func TestValidateInstallation(t *testing.T) {
	in := hostInstallation(t)
	a := installationAgent(t, sleeper, in)
	if err := a.validate(); err != nil {
		t.Fatal(err)
	}
	withToken := newAgent(t, sleeper)
	withToken.Installation = in
	if err := withToken.validate(); err == nil || !strings.Contains(err.Error(), "no legacy token") {
		t.Fatalf("validate with a token = %v", err)
	}
	for _, change := range []func(*Installation){
		func(i *Installation) { i.DataDir = "" },
		func(i *Installation) { i.AuthFile = "auth.json" },
		func(i *Installation) { i.ArtifactDir += "/../x" },
	} {
		b := a
		change(&b.Installation)
		if err := b.validate(); err == nil || !strings.Contains(err.Error(), "clean absolute") {
			t.Errorf("validate(%+v) = %v", b.Installation, err)
		}
		r := &fakeRunner{}
		if _, err := Start(context.Background(), r, b, false); err == nil || len(r.cmds) != 0 {
			t.Errorf("Start(%+v) = %v after %d commands; want refusal before any", b.Installation, err, len(r.cmds))
		}
	}
}

func TestInstallInstallation(t *testing.T) {
	s := newExecServer(t)
	a := installationAgent(t, []byte("agent binary"), hostInstallation(t))
	host := tree(t, filepath.Dir(a.Installation.DataDir))
	var progress bytes.Buffer
	st, err := Install(context.Background(), s.runner(), a, &progress)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(s.puts(), " "); got != "skygo-ssh cert.pem key.pem" {
		t.Fatalf("transferred %q", got)
	}
	if st.Files["token"] != "missing" {
		t.Fatalf("token state %q", st.Files["token"])
	}
	if _, err := os.Lstat(s.remote(a, "token")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("an installation agent directory holds a token")
	}
	// Status names the installation only to compare it with the record;
	// no transfer names it, and the installation is unchanged.
	for _, r := range s.records() {
		if strings.Contains(r.command, a.Installation.DataDir) && !strings.Contains(r.command, " skygo-bootstrap 'status' ") {
			t.Errorf("install sent the installation: %q", r.command)
		}
	}
	if after := tree(t, filepath.Dir(a.Installation.DataDir)); !maps.Equal(host, after) {
		t.Fatalf("install changed the installation from\n%v\nto\n%v", host, after)
	}
}

// TestInstallInstallationRefusesLegacy refuses to serve an installation
// from a directory a legacy agent was installed in, before transferring.
func TestInstallInstallationRefusesLegacy(t *testing.T) {
	s := newExecServer(t)
	legacy := newAgent(t, []byte("agent binary"))
	install(t, s, legacy)
	a := installationAgent(t, []byte("agent binary"), hostInstallation(t))
	a.Local["cert.pem"], a.Local["key.pem"] = legacy.Local["cert.pem"], legacy.Local["key.pem"]
	before := len(s.records())
	if _, err := Install(context.Background(), s.runner(), a, io.Discard); err == nil || !strings.Contains(err.Error(), "legacy token") {
		t.Fatalf("Install = %v, want legacy token refusal", err)
	}
	if n := len(s.records()) - before; n != 1 {
		t.Fatalf("refused install ran %d commands, want one status", n)
	}
}

// TestInstallLegacyRefusesInstallation refuses to install a legacy token
// into a directory recorded for an installation, before transferring it.
func TestInstallLegacyRefusesInstallation(t *testing.T) {
	s := newExecServer(t)
	a := installationAgent(t, []byte("agent binary"), hostInstallation(t))
	installFiles(t, s, a)
	writeConfig(t, s, a, installationRecord(a))
	legacy := newAgent(t, []byte("agent binary"))
	legacy.Local["cert.pem"], legacy.Local["key.pem"] = a.Local["cert.pem"], a.Local["key.pem"]
	before := snapshot(t, s, a)
	n := len(s.records())
	if _, err := Install(context.Background(), s.runner(), legacy, io.Discard); !errors.Is(err, fs.ErrExist) || !strings.Contains(err.Error(), "serves an installation") {
		t.Fatalf("Install = %v, want refusal wrapping fs.ErrExist", err)
	}
	if got := len(s.records()) - n; got != 1 {
		t.Fatalf("refused install ran %d commands, want one status", got)
	}
	if after := snapshot(t, s, a); !maps.Equal(before, after) {
		t.Fatalf("refused install changed the directory from\n%v\nto\n%v", before, after)
	}
}

// TestInstallRefusesUnsafeConfig refuses, in either mode, to install into
// a directory whose configuration record is truncated, unknown or not a
// regular file, before transferring anything. Without the refusal both
// installs would store the missing files, and the legacy one a token.
func TestInstallRefusesUnsafeConfig(t *testing.T) {
	records := []struct {
		name   string
		record func(a Agent) string // "symlink" for a symlinked record
	}{
		{"truncated_installation", func(a Agent) string { return strings.TrimSuffix(installationRecord(a), "\n") }},
		{"truncated_legacy", func(a Agent) string { return strings.TrimSuffix(configRecord(a), "\n") }},
		{"unknown", func(a Agent) string { return configRecord(a) + "extra\n" }},
		{"symlink", func(Agent) string { return "symlink" }},
	}
	for _, rec := range records {
		for _, legacy := range []bool{false, true} {
			name := rec.name + "/installation"
			if legacy {
				name = rec.name + "/legacy"
			}
			t.Run(name, func(t *testing.T) {
				s := newExecServer(t)
				in := installationAgent(t, []byte("agent binary"), hostInstallation(t))
				// Only the binary is present, so an install would transfer the rest.
				sum, err := fileSum(in.Local["skygo-ssh"])
				if err != nil {
					t.Fatal(err)
				}
				if err := put(context.Background(), s.runner(), in, "skygo-ssh", sum, io.Discard); err != nil {
					t.Fatal(err)
				}
				if r := rec.record(in); r == "symlink" {
					if err := os.Symlink(s.remote(in, "skygo-ssh"), s.remote(in, "agent.config")); err != nil {
						t.Fatal(err)
					}
				} else {
					writeConfig(t, s, in, r)
				}
				a := in
				if legacy {
					a = newAgent(t, []byte("agent binary"))
					a.Local["cert.pem"], a.Local["key.pem"] = in.Local["cert.pem"], in.Local["key.pem"]
				}
				host := filepath.Dir(in.Installation.DataDir)
				before, beforeHost := snapshot(t, s, in), tree(t, host)
				n, puts := len(s.records()), len(s.puts())
				if _, err := Install(context.Background(), s.runner(), a, io.Discard); err == nil || !strings.Contains(err.Error(), "agent.config is unverifiable") {
					t.Fatalf("Install = %v, want unverifiable record refusal", err)
				}
				if got := len(s.records()) - n; got != 1 {
					t.Fatalf("refused install ran %d commands, want one status", got)
				}
				if got := len(s.puts()); got != puts {
					t.Fatalf("refused install transferred %q", s.puts()[puts:])
				}
				if after := snapshot(t, s, in); !maps.Equal(before, after) {
					t.Fatalf("refused install changed the directory from\n%v\nto\n%v", before, after)
				}
				if _, err := os.Lstat(s.remote(in, "token")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("refused install planted a token")
				}
				if after := tree(t, host); !maps.Equal(beforeHost, after) {
					t.Fatalf("refused install changed the installation from\n%v\nto\n%v", beforeHost, after)
				}
			})
		}
	}
}

func TestStatusConfigForm(t *testing.T) {
	a := installationAgent(t, sleeper, hostInstallation(t))
	tests := []struct {
		record string // "" for none
		want   string
		same   bool
	}{
		{"", "none", false},
		{configRecord(a), "legacy", false},
		{installationRecord(a), "installation", true},
		{"port 1\n", "unsafe", false},
		{"symlink", "unsafe", false},
	}
	for _, tt := range tests {
		s := newExecServer(t)
		installFiles(t, s, a)
		switch tt.record {
		case "":
		case "symlink":
			if err := os.Symlink(s.remote(a, "cert.pem"), s.remote(a, "agent.config")); err != nil {
				t.Fatal(err)
			}
		default:
			writeConfig(t, s, a, tt.record)
		}
		st, err := Status(context.Background(), s.runner(), a)
		if err != nil || st.Config != tt.want || st.ConfigSame != tt.same {
			t.Errorf("Status with record %q = %q same=%v, %v; want %q same=%v", tt.record, st.Config, st.ConfigSame, err, tt.want, tt.same)
		}
	}
}

// TestStartCommand checks the one command Start sends for a legacy and an
// installation agent, with and without restart, and how it reads the
// reply. The Runner is a MODEL (fakeRunner): it records the command and
// answers with fixed output, running neither the script nor an agent.
// The test therefore checks only the Go layer: the script it runs, the
// mode, and the operands that select the record form, the installation
// paths and the restart policy. It does not show what the script records
// or the argv with which it starts an agent.
func TestStartCommand(t *testing.T) {
	in := hostInstallation(t)
	legacy := newAgent(t, []byte("agent binary"))
	inst := installationAgent(t, []byte("agent binary"), in)
	tests := []struct {
		name    string
		a       Agent
		restart bool
		out     string
		pid     int  // the process Start returns, if it succeeds
		exist   bool // Start refuses with fs.ErrExist
	}{
		{"legacy", legacy, false, "agent started 4242\n", 4242, false},
		{"legacy_restart", legacy, true, "agent started 4242\n", 4242, false},
		{"legacy_running", legacy, false, "agent running 4242\n", 4242, false},
		{"legacy_mismatch", legacy, true, configMismatch, 0, true},
		{"installation", inst, false, "agent started 4242\n", 4242, false},
		{"installation_restart", inst, true, "agent started 4242\n", 4242, false},
		{"installation_running", inst, false, "agent running 4242\n", 4242, false},
		{"installation_mismatch", inst, true, configMismatch, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &fakeRunner{out: map[string]string{"start": tt.out}}
			pid, err := Start(context.Background(), r, tt.a, tt.restart)
			switch {
			case tt.exist && (pid != 0 || !errors.Is(err, fs.ErrExist)):
				t.Fatalf("Start = %d, %v; want an error wrapping fs.ErrExist", pid, err)
			case !tt.exist && (pid != tt.pid || err != nil):
				t.Fatalf("Start = %d, %v; want %d", pid, err, tt.pid)
			}
			if len(r.cmds) != 1 {
				t.Fatalf("Start ran %d commands, want one start", len(r.cmds))
			}
			flag := "0"
			if tt.restart {
				flag = "1"
			}
			want := []string{"start", tt.a.Commit, tt.a.Digest, strconv.Itoa(tt.a.Port), tt.a.Target, flag}
			if !tt.a.Installation.IsZero() {
				want = append(want, in.DataDir, in.AuthFile, in.ArtifactDir)
			}
			if got := scriptOperands(t, r.cmds[0]); !slices.Equal(got, want) {
				t.Fatalf("start operands = %q, want %q", got, want)
			}
		})
	}
}

// TestStartStoppedNeedsRestart checks, with the real script, that a start
// without restart refuses to replace a recorded process that has exited,
// in either mode, with the matching record in place, and changes nothing.
// The recorded process is a child the test ran and waited for; the agent
// binary is inert and nothing is started.
func TestStartStoppedNeedsRestart(t *testing.T) {
	in, legacy := recordAgents(t, []byte("agent binary"))
	for _, tt := range []struct {
		name   string
		a      Agent
		record string
	}{
		{"legacy", legacy, configRecord(legacy)},
		{"installation", in, installationRecord(in)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newExecServer(t)
			installFiles(t, s, tt.a)
			writeConfig(t, s, tt.a, tt.record)
			if err := os.WriteFile(s.remote(tt.a, "agent.pid"), []byte(exitedPID(t)+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			host := filepath.Dir(in.Installation.DataDir)
			before, beforeHost := identity(t, s.home), identity(t, host)
			pid, err := Start(context.Background(), s.runner(), tt.a, false)
			if pid != 0 || err == nil || !strings.Contains(err.Error(), "--restart-stopped") {
				t.Fatalf("Start = %d, %v; want a refusal naming --restart-stopped", pid, err)
			}
			checkIdentity(t, "refused start", s.home, before)
			checkIdentity(t, "refused start", host, beforeHost)
		})
	}
}

// TestStartInstallationRefused checks that a start for an installation
// that is absent, mismatched or mixed with a legacy agent changes nothing
// in the agent directory or the installation and starts no process.
func TestStartInstallationRefused(t *testing.T) {
	tests := []struct {
		name     string
		edit     func(t *testing.T, s *execServer, a Agent)
		start    func(a Agent) Agent // the agent to start, default a
		err      string
		mismatch bool
	}{
		{name: "missing_data", edit: func(t *testing.T, _ *execServer, a Agent) {
			if err := os.RemoveAll(a.Installation.DataDir); err != nil {
				t.Fatal(err)
			}
		}, err: "initialize the installation"},
		{name: "missing_state", edit: func(t *testing.T, _ *execServer, a Agent) {
			if err := os.Remove(filepath.Join(a.Installation.DataDir, "jobs.db")); err != nil {
				t.Fatal(err)
			}
		}, err: "initialize the installation"},
		{name: "missing_auth", edit: func(t *testing.T, _ *execServer, a Agent) {
			if err := os.Remove(a.Installation.AuthFile); err != nil {
				t.Fatal(err)
			}
		}, err: "initialize the installation"},
		{name: "auth_symlink", edit: func(t *testing.T, _ *execServer, a Agent) {
			other := filepath.Join(t.TempDir(), "auth.json")
			if err := os.WriteFile(other, []byte("model\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(a.Installation.AuthFile); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(other, a.Installation.AuthFile); err != nil {
				t.Fatal(err)
			}
		}, err: "initialize the installation"},
		{name: "artifacts_file", edit: func(t *testing.T, _ *execServer, a Agent) {
			if err := os.Remove(a.Installation.ArtifactDir); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(a.Installation.ArtifactDir, nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}, err: "initialize the installation"},
		{name: "legacy_token", edit: func(t *testing.T, s *execServer, a Agent) {
			if err := os.WriteFile(s.remote(a, "token"), []byte("secret\n"), 0o400); err != nil {
				t.Fatal(err)
			}
		}, err: "legacy token"},
		{name: "legacy_record", edit: func(t *testing.T, s *execServer, a Agent) {
			writeConfig(t, s, a, configRecord(a))
		}, mismatch: true},
		{name: "other_installation", edit: func(t *testing.T, s *execServer, a Agent) {
			b := a
			b.Installation.ArtifactDir = a.Installation.ArtifactDir + "-other"
			writeConfig(t, s, a, installationRecord(b))
		}, mismatch: true},
		{name: "legacy_start", edit: func(t *testing.T, s *execServer, a Agent) {
			writeConfig(t, s, a, installationRecord(a))
		}, start: func(a Agent) Agent {
			b := a
			b.Installation = Installation{}
			b.Local = maps.Clone(a.Local)
			b.Local["token"] = b.Local["cert.pem"]
			return b
		}, mismatch: true},
		{name: "unverifiable_record", edit: func(t *testing.T, s *execServer, a Agent) {
			b := a
			b.Installation.DataDir = "/a/../b"
			writeConfig(t, s, a, installationRecord(b))
		}, err: "unverifiable"},
		{name: "truncated_record", edit: func(t *testing.T, s *execServer, a Agent) {
			writeConfig(t, s, a, strings.TrimSuffix(installationRecord(a), "\n"))
		}, err: "unverifiable"},
	}
	for _, tt := range tests {
		for _, stopped := range []bool{false, true} {
			// Subtest names stay valid installation path characters, since
			// t.TempDir names directories after them.
			name := tt.name + "/none"
			if stopped {
				name = tt.name + "/stopped"
			}
			t.Run(name, func(t *testing.T) {
				s := newExecServer(t)
				a := installationAgent(t, []byte("agent binary"), hostInstallation(t))
				installFiles(t, s, a)
				if stopped {
					if err := os.WriteFile(s.remote(a, "agent.pid"), []byte(exitedPID(t)+"\n"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				tt.edit(t, s, a)
				b := a
				if tt.start != nil {
					b = tt.start(a)
				}
				host := filepath.Dir(a.Installation.DataDir)
				before, beforeHost := snapshot(t, s, a), tree(t, host)
				pid, err := Start(context.Background(), s.runner(), b, true)
				if pid != 0 || err == nil {
					t.Fatalf("Start = %d, %v; want refusal", pid, err)
				}
				if tt.mismatch != errors.Is(err, fs.ErrExist) || !tt.mismatch && !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("Start = %v; want mismatch=%v %q", err, tt.mismatch, tt.err)
				}
				if after := snapshot(t, s, a); !maps.Equal(before, after) {
					t.Fatalf("refused start changed the agent directory from\n%v\nto\n%v", before, after)
				}
				if after := tree(t, host); !maps.Equal(beforeHost, after) {
					t.Fatalf("refused start changed the installation from\n%v\nto\n%v", beforeHost, after)
				}
			})
		}
	}
}

// tree describes every entry below root without following symlinks.
func tree(t *testing.T, root string) map[string]string {
	t.Helper()
	m := map[string]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		m[rel] = info.Mode().String() + " " + strconv.FormatInt(info.Size(), 10)
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			m[rel] += " " + string(data)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}
