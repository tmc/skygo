package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// These tests run the real script through the in-process SSH server. None
// of them starts an agent process. TestRecordReadFailure replaces head
// with a MODEL; the parser is still the script's.

// recordAgents returns an agent serving a new host installation and a
// legacy agent of the same build, port, target, certificate and key, so
// that both name the same remote directory.
func recordAgents(t *testing.T, binary []byte) (in, legacy Agent) {
	t.Helper()
	in = installationAgent(t, binary, hostInstallation(t))
	legacy = newAgent(t, binary)
	legacy.Local["cert.pem"], legacy.Local["key.pem"] = in.Local["cert.pem"], in.Local["key.pem"]
	return in, legacy
}

// A recordCase is the content of agent.config, or a function making it.
type recordCase struct {
	name string
	data string
	make func(t *testing.T, path string) // if set, makes the record instead of data
}

// malformedRecords returns regular-file records the parser must report as
// unsafe for every agent of a's build.
func malformedRecords(a Agent) []recordCase {
	leg, inst := configRecord(a), installationRecord(a)
	port, i := strconv.Itoa(a.Port), a.Installation
	return []recordCase{
		// Exactly two newlines, as a legacy record has.
		{name: "two_empty_lines", data: "\n\n"},
		{name: "two_swapped", data: "target " + a.Target + "\nport " + port + "\n"},
		{name: "two_bad_key", data: "port " + port + "\ntargets " + a.Target + "\n"},
		{name: "two_leading_zero", data: "port 0" + port + "\ntarget " + a.Target + "\n"},
		{name: "two_low_port", data: "port 80\ntarget " + a.Target + "\n"},
		{name: "two_high_port", data: "port 65536\ntarget " + a.Target + "\n"},
		{name: "two_bad_target", data: "port " + port + "\ntarget DGX\n"},
		{name: "two_double_space", data: "port  " + port + "\ntarget " + a.Target + "\n"},
		{name: "two_carriage_return", data: "port " + port + "\ntarget " + a.Target + "\r\n"},
		{name: "two_nul", data: "port " + port + "\x00\ntarget " + a.Target + "\n"},
		{name: "two_one_line", data: "port " + port + " target " + a.Target + "\n\n"},

		// Exactly five newlines, as an installation record has.
		{name: "five_empty_lines", data: "\n\n\n\n\n"},
		{name: "five_legacy_repeated", data: leg + leg + "\n"},
		{name: "five_swapped", data: leg + "auth " + i.AuthFile + "\ndata " + i.DataDir + "\nartifacts " + i.ArtifactDir + "\n"},
		{name: "five_bad_key", data: leg + "data " + i.DataDir + "\nauth " + i.AuthFile + "\nartifact " + i.ArtifactDir + "\n"},
		{name: "five_relative_path", data: leg + "data data\nauth " + i.AuthFile + "\nartifacts " + i.ArtifactDir + "\n"},
		{name: "five_unclean_path", data: leg + "data " + i.DataDir + "/../x\nauth " + i.AuthFile + "\nartifacts " + i.ArtifactDir + "\n"},
		{name: "five_root_path", data: leg + "data /\nauth " + i.AuthFile + "\nartifacts " + i.ArtifactDir + "\n"},
		{name: "five_bad_port", data: strings.Replace(inst, "port "+port+"\n", "port 99999\n", 1)},
		{name: "five_bad_target", data: strings.Replace(inst, "target "+a.Target+"\n", "target a_b\n", 1)},
		{name: "five_nul", data: strings.Replace(inst, "\nauth ", "\x00\nauth ", 1)},

		// Empty, truncated, unterminated, extended or overlong.
		{name: "empty", data: ""},
		{name: "legacy_unterminated", data: strings.TrimSuffix(leg, "\n")},
		{name: "installation_unterminated", data: strings.TrimSuffix(inst, "\n")},
		{name: "installation_truncated_data", data: leg + "data " + i.DataDir + "\n"},
		{name: "installation_truncated_auth", data: leg + "data " + i.DataDir + "\nauth " + i.AuthFile + "\n"},
		{name: "legacy_trailing_byte", data: leg + "x"},
		{name: "legacy_trailing_newline", data: leg + "\n"},
		{name: "legacy_trailing_nul", data: leg + "\x00"},
		{name: "installation_trailing_byte", data: inst + "x"},
		{name: "installation_trailing_newline", data: inst + "\n"},
		{name: "installation_sixth_line", data: inst + "data " + i.DataDir + "\n"},
		{name: "legacy_1025_bytes", data: leg + strings.Repeat("#", 1025-len(leg))},
		{name: "installation_1025_bytes", data: inst + strings.Repeat("\n", 1025-len(inst))},
		{name: "legacy_1MiB", data: leg + strings.Repeat("x", 1<<20)},
	}
}

// nonRegularRecords returns records that are not regular files: a link to
// a's own installation record, a directory and a named pipe, which the
// parser must refuse without opening.
func nonRegularRecords(a Agent) []recordCase {
	return []recordCase{
		{name: "symlink", make: func(t *testing.T, path string) {
			outside := filepath.Join(t.TempDir(), "agent.config")
			if err := os.WriteFile(outside, []byte(installationRecord(a)), 0o400); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "directory", make: func(t *testing.T, path string) {
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "fifo", make: func(t *testing.T, path string) {
			if err := syscall.Mkfifo(path, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	}
}

func unsafeRecords(a Agent) []recordCase {
	return append(malformedRecords(a), nonRegularRecords(a)...)
}

func plantRecord(t *testing.T, s *execServer, a Agent, rc recordCase) {
	t.Helper()
	if rc.make != nil {
		rc.make(t, s.remote(a, "agent.config"))
		return
	}
	writeConfig(t, s, a, rc.data)
}

// noRecord leaves agent.config absent.
var noRecord = recordCase{name: "none", make: func(*testing.T, string) {}}

// putFile stores a's local file name remotely without a status check.
func putFile(t *testing.T, s *execServer, a Agent, name string) {
	t.Helper()
	sum, err := fileSum(a.Local[name])
	if err != nil {
		t.Fatal(err)
	}
	if err := put(context.Background(), s.runner(), a, name, sum, io.Discard); err != nil {
		t.Fatal(err)
	}
}

// bystander makes a file beside the skygo-test namespace in s's home.
func bystander(t *testing.T, s *execServer) {
	t.Helper()
	dir := filepath.Join(s.home, "bystander")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "keep"), []byte("bystander\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// identity describes every entry below root without following symlinks
// or opening anything but regular files: its mode, size, inode and
// modification time, a regular file's SHA-256 and a symlink's target. A
// directory's modification time changes if an entry is created in it,
// even one removed again.
func identity(t *testing.T, root string) map[string]string {
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
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("%s: no inode", p)
		}
		rel, _ := filepath.Rel(root, p)
		id := fmt.Sprintf("%v %d ino=%d mtime=%d", info.Mode(), info.Size(), st.Ino, info.ModTime().UnixNano())
		switch {
		case info.Mode().IsRegular():
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(data)
			id += " " + hex.EncodeToString(sum[:])
		case info.Mode()&fs.ModeSymlink != 0:
			target, err := os.Readlink(p)
			if err != nil {
				return err
			}
			id += " -> " + target
		}
		m[rel] = id
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func checkIdentity(t *testing.T, what, root string, before map[string]string) {
	t.Helper()
	if after := identity(t, root); !maps.Equal(before, after) {
		t.Fatalf("%s changed %s from\n%v\nto\n%v", what, root, before, after)
	}
}

// TestStatusRecordExact checks that Status classifies each record with
// the one exact parser, for an installation and a legacy agent, and
// creates or changes nothing, not even a temporary file.
func TestStatusRecordExact(t *testing.T) {
	in, legacy := recordAgents(t, sleeper)
	other := func(change func(*Agent)) Agent {
		b := in
		change(&b)
		return b
	}
	otherPort := other(func(b *Agent) { b.Port++ })
	otherTarget := other(func(b *Agent) { b.Target = "other" })
	otherData := other(func(b *Agent) { b.Installation.DataDir += "-other" })
	otherAuth := other(func(b *Agent) { b.Installation.AuthFile += "-other" })
	otherArtifacts := other(func(b *Agent) { b.Installation.ArtifactDir += "-other" })

	type want struct {
		form string
		same bool
	}
	type statusCase struct {
		rc               recordCase
		forIn, forLegacy want
	}
	tests := []statusCase{
		{noRecord, want{"none", false}, want{"none", false}},
		{recordCase{name: "legacy", data: configRecord(in)}, want{"legacy", false}, want{"legacy", true}},
		{recordCase{name: "legacy_other_port", data: configRecord(otherPort)}, want{"legacy", false}, want{"legacy", false}},
		{recordCase{name: "legacy_other_target", data: configRecord(otherTarget)}, want{"legacy", false}, want{"legacy", false}},
		{recordCase{name: "installation", data: installationRecord(in)}, want{"installation", true}, want{"installation", false}},
		{recordCase{name: "installation_other_port", data: installationRecord(otherPort)}, want{"installation", false}, want{"installation", false}},
		{recordCase{name: "installation_other_target", data: installationRecord(otherTarget)}, want{"installation", false}, want{"installation", false}},
		{recordCase{name: "installation_other_data", data: installationRecord(otherData)}, want{"installation", false}, want{"installation", false}},
		{recordCase{name: "installation_other_auth", data: installationRecord(otherAuth)}, want{"installation", false}, want{"installation", false}},
		{recordCase{name: "installation_other_artifacts", data: installationRecord(otherArtifacts)}, want{"installation", false}, want{"installation", false}},
	}
	for _, rc := range unsafeRecords(in) {
		tests = append(tests, statusCase{rc, want{"unsafe", false}, want{"unsafe", false}})
	}
	host := filepath.Dir(in.Installation.DataDir)
	for _, tt := range tests {
		t.Run(tt.rc.name, func(t *testing.T) {
			s := newExecServer(t)
			installFiles(t, s, in)
			plantRecord(t, s, in, tt.rc)
			bystander(t, s)
			before, beforeHost := identity(t, s.home), identity(t, host)
			for _, c := range []struct {
				a    Agent
				want want
			}{{in, tt.forIn}, {legacy, tt.forLegacy}} {
				st, err := Status(context.Background(), s.runner(), c.a)
				if err != nil {
					t.Fatal(err)
				}
				if st.Config != c.want.form || st.ConfigSame != c.want.same {
					t.Errorf("Status(installation=%v) = %q same=%v; want %q same=%v", !c.a.Installation.IsZero(), st.Config, st.ConfigSame, c.want.form, c.want.same)
				}
				checkIdentity(t, "Status", s.home, before)
				checkIdentity(t, "Status", host, beforeHost)
			}
		})
	}
}

// TestInstallRefusesRecordBeforePut checks that Install refuses, after its
// one status command and before any transfer, an unsafe record in either
// mode, a record of the other form whether or not a token is present, and
// a record of the same form naming another port, target or installation.
// The agent directory, the installation and a bystander stay identical.
func TestInstallRefusesRecordBeforePut(t *testing.T) {
	in, legacy := recordAgents(t, []byte("agent binary"))
	other := func(change func(*Agent)) Agent {
		b := in
		change(&b)
		return b
	}
	type refusal struct {
		rc     recordCase
		token  bool // the directory holds a token
		legacy bool // install the legacy agent, not the installation one
		exist  bool // the refusal wraps fs.ErrExist; otherwise it is unverifiable
	}
	var tests []refusal
	for _, token := range []bool{false, true} {
		tests = append(tests,
			refusal{recordCase{name: "legacy_record", data: configRecord(in)}, token, false, true},
			refusal{recordCase{name: "installation_record", data: installationRecord(in)}, token, true, true},
		)
	}
	for _, c := range []struct {
		name   string
		change func(*Agent)
	}{
		{"other_port", func(b *Agent) { b.Port++ }},
		{"other_target", func(b *Agent) { b.Target = "other" }},
		{"other_data", func(b *Agent) { b.Installation.DataDir += "-other" }},
		{"other_auth", func(b *Agent) { b.Installation.AuthFile += "-other" }},
		{"other_artifacts", func(b *Agent) { b.Installation.ArtifactDir += "-other" }},
	} {
		b := other(c.change)
		tests = append(tests, refusal{recordCase{name: "installation_" + c.name, data: installationRecord(b)}, false, false, true})
		if b.Installation == in.Installation {
			tests = append(tests, refusal{recordCase{name: "legacy_" + c.name, data: configRecord(b)}, false, true, true})
		}
	}
	for _, rc := range unsafeRecords(in) {
		tests = append(tests, refusal{rc, false, false, false}, refusal{rc, false, true, false})
	}
	host := filepath.Dir(in.Installation.DataDir)
	for _, tt := range tests {
		name := tt.rc.name + "/installation"
		if tt.legacy {
			name = tt.rc.name + "/legacy"
		}
		if tt.token {
			name += "/token"
		}
		t.Run(name, func(t *testing.T) {
			s := newExecServer(t)
			a := in
			if tt.legacy {
				a = legacy
			}
			// Only the binary is present, so an install would transfer the rest.
			putFile(t, s, in, "skygo-ssh")
			if tt.token {
				if err := os.WriteFile(s.remote(in, "token"), []byte("secret\n"), 0o400); err != nil {
					t.Fatal(err)
				}
			}
			plantRecord(t, s, in, tt.rc)
			bystander(t, s)
			before, beforeHost := identity(t, s.home), identity(t, host)
			n, puts := len(s.records()), len(s.puts())
			_, err := Install(context.Background(), s.runner(), a, io.Discard)
			switch {
			case err == nil:
				t.Fatal("Install succeeded, want refusal")
			case tt.exist && !errors.Is(err, fs.ErrExist):
				t.Fatalf("Install = %v, want an error wrapping fs.ErrExist", err)
			case !tt.exist && (errors.Is(err, fs.ErrExist) || !strings.Contains(err.Error(), "agent.config is unverifiable")):
				t.Fatalf("Install = %v, want unverifiable record refusal", err)
			}
			if got := len(s.records()) - n; got != 1 {
				t.Fatalf("refused install ran %d commands, want one status", got)
			}
			if got := s.puts(); len(got) != puts {
				t.Fatalf("refused install transferred %q", got[puts:])
			}
			checkIdentity(t, "refused install", s.home, before)
			checkIdentity(t, "refused install", host, beforeHost)
		})
	}
}

// TestInstallKeepsMatchingRecord checks that a canonical record of the
// agent's own configuration stays usable: a legacy record without a token
// is still legacy, and Install transfers the token into it, and an
// installation record of the same installation transfers no token. The
// record itself is never replaced.
func TestInstallKeepsMatchingRecord(t *testing.T) {
	in, legacy := recordAgents(t, []byte("agent binary"))
	tests := []struct {
		name   string
		a      Agent
		record string
		form   string
		puts   string
	}{
		{"legacy", legacy, configRecord(legacy), "legacy", "cert.pem key.pem token"},
		{"installation", in, installationRecord(in), "installation", "cert.pem key.pem"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newExecServer(t)
			putFile(t, s, tt.a, "skygo-ssh")
			writeConfig(t, s, tt.a, tt.record)
			record, err := os.Lstat(s.remote(tt.a, "agent.config"))
			if err != nil {
				t.Fatal(err)
			}
			puts := len(s.puts())
			st, err := Install(context.Background(), s.runner(), tt.a, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if st.Config != tt.form || !st.ConfigSame {
				t.Fatalf("Install state = %q same=%v, want %q same=true", st.Config, st.ConfigSame, tt.form)
			}
			if got := strings.Join(s.puts()[puts:], " "); got != tt.puts {
				t.Fatalf("transferred %q, want %q", got, tt.puts)
			}
			after, err := os.Lstat(s.remote(tt.a, "agent.config"))
			if err != nil || !os.SameFile(record, after) {
				t.Fatalf("configuration record was replaced: %v", err)
			}
			if data, _ := os.ReadFile(s.remote(tt.a, "agent.config")); string(data) != tt.record {
				t.Fatalf("configuration record = %q, want %q", data, tt.record)
			}
		})
	}
}

// TestStartRefusesUnsafeRecord checks that Start, in either mode and even
// with restart set, refuses every record the parser reports as unsafe,
// before recording or starting a process, and changes nothing.
func TestStartRefusesUnsafeRecord(t *testing.T) {
	in, legacy := recordAgents(t, []byte("agent binary"))
	host := filepath.Dir(in.Installation.DataDir)
	for _, rc := range unsafeRecords(in) {
		t.Run(rc.name, func(t *testing.T) {
			s := newExecServer(t)
			installFiles(t, s, in)
			plantRecord(t, s, in, rc)
			bystander(t, s)
			before, beforeHost := identity(t, s.home), identity(t, host)
			for _, a := range []Agent{in, legacy} {
				pid, err := Start(context.Background(), s.runner(), a, true)
				if pid != 0 || err == nil || errors.Is(err, fs.ErrExist) || !strings.Contains(err.Error(), "unverifiable") {
					t.Fatalf("Start(installation=%v) = %d, %v; want unverifiable configuration", !a.Installation.IsZero(), pid, err)
				}
				checkIdentity(t, "refused start", s.home, before)
				checkIdentity(t, "refused start", host, beforeHost)
			}
			for _, name := range []string{"agent.pid", "agent.log"} {
				if _, err := os.Lstat(s.remote(in, name)); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("refused start made %s: %v", name, err)
				}
			}
		})
	}
}

// headShim puts first on PATH a MODEL head that, reading agent.config,
// writes data and exits with code, and otherwise runs the real head. It
// models a read that fails after delivering bytes; it is not a native
// fault. The shim runs in the foreground of the script, which the
// execServer runs and waits for, and starts nothing else.
func headShim(t *testing.T, data string, code int) {
	t.Helper()
	head, err := exec.LookPath("head")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	out := filepath.Join(dir, "out")
	if err := os.WriteFile(out, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	shim := "#!/bin/sh\ncase ${3-} in */agent.config) /bin/cat '" + out + "'; exit " + strconv.Itoa(code) + " ;; esac\nexec '" + head + "' \"$@\"\n"
	if err := os.WriteFile(filepath.Join(dir, "head"), []byte(shim), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
}

// TestRecordReadFailure checks that the real script's parser reports as
// unsafe a canonical record whose read fails after delivering all of it,
// so Status says unsafe and Install and Start refuse it before any
// transfer or start, changing nothing. The failing read is the headShim
// MODEL. Its twin, the same shim exiting 0, shows that the record and the
// shim alone would be accepted as the agent's own.
func TestRecordReadFailure(t *testing.T) {
	in, legacy := recordAgents(t, []byte("agent binary"))
	host := filepath.Dir(in.Installation.DataDir)
	tests := []struct {
		name   string
		a      Agent
		record string
		form   string
	}{
		{"legacy", legacy, configRecord(legacy), "legacy"},
		{"installation", in, installationRecord(in), "installation"},
	}
	for _, tt := range tests {
		for _, code := range []int{0, 1} {
			t.Run(tt.name+"/exit"+strconv.Itoa(code), func(t *testing.T) {
				s := newExecServer(t)
				// Only the binary is present, so an install would transfer the rest.
				putFile(t, s, tt.a, "skygo-ssh")
				writeConfig(t, s, tt.a, tt.record)
				bystander(t, s)
				headShim(t, tt.record, code)
				before, beforeHost := identity(t, s.home), identity(t, host)
				ctx := context.Background()
				st, err := Status(ctx, s.runner(), tt.a)
				if err != nil {
					t.Fatal(err)
				}
				checkIdentity(t, "Status", s.home, before)
				checkIdentity(t, "Status", host, beforeHost)
				if code == 0 {
					if st.Config != tt.form || !st.ConfigSame {
						t.Fatalf("Status after a successful read = %q same=%v; want %q same=true", st.Config, st.ConfigSame, tt.form)
					}
					return
				}
				if st.Config != "unsafe" || st.ConfigSame {
					t.Fatalf("Status after a failed read = %q same=%v; want unsafe", st.Config, st.ConfigSame)
				}

				n, puts := len(s.records()), len(s.puts())
				_, err = Install(ctx, s.runner(), tt.a, io.Discard)
				if err == nil || errors.Is(err, fs.ErrExist) || !strings.Contains(err.Error(), "agent.config is unverifiable") {
					t.Fatalf("Install = %v, want unverifiable record refusal", err)
				}
				if got := len(s.records()) - n; got != 1 {
					t.Fatalf("refused install ran %d commands, want one status", got)
				}
				if got := s.puts(); len(got) != puts {
					t.Fatalf("refused install transferred %q", got[puts:])
				}
				checkIdentity(t, "refused install", s.home, before)
				checkIdentity(t, "refused install", host, beforeHost)

				pid, err := Start(ctx, s.runner(), tt.a, true)
				if pid != 0 || err == nil || errors.Is(err, fs.ErrExist) || !strings.Contains(err.Error(), "unverifiable") {
					t.Fatalf("Start = %d, %v; want unverifiable configuration", pid, err)
				}
				checkIdentity(t, "refused start", s.home, before)
				checkIdentity(t, "refused start", host, beforeHost)
			})
		}
	}
}
