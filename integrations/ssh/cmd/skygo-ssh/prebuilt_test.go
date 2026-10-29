package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmc/skygo/integrations/ssh/internal/agentbinary"
	"golang.org/x/tools/txtar"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

func TestPrebuiltFlags(t *testing.T) {
	prefix := []string{"--host=dgx", "--known-hosts=kh"}
	commit := strings.Repeat("a", 40)
	digest := strings.Repeat("b", 64)
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{"path only", []string{"--agent-binary=agent"}, "go together"},
		{"commit only", []string{"--agent-commit=" + commit}, "go together"},
		{"digest only", []string{"--agent-digest=" + digest}, "go together"},
		{"empty", []string{"--agent-binary="}, "go together"},
		{"source", []string{"--source=.", "--agent-binary=agent", "--agent-commit=" + commit, "--agent-digest=" + digest}, "exclusive"},
		{"empty source", []string{"--source=", "--agent-binary=agent", "--agent-commit=" + commit, "--agent-digest=" + digest}, "exclusive"},
		{"bad commit", []string{"--agent-binary=agent", "--agent-commit=HEAD", "--agent-digest=" + digest}, "commit"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := parseFlags(append(append([]string{}, prefix...), tt.args...), io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parseFlags=%v want %q", err, tt.want)
			}
		})
	}
	f, _, err := parseFlags(append(prefix, "--agent-binary=agent", "--agent-commit="+commit, "--agent-digest="+digest), io.Discard)
	if err != nil || !filepath.IsAbs(f.prebuilt.Path) || filepath.Base(f.prebuilt.Path) != "agent" {
		t.Fatalf("relative CLI input=%+v,%v", f.prebuilt, err)
	}
}

func TestPrebuiltScripts(t *testing.T) {
	scriptDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("SKYGO_TOKEN", "")
	// No checkout is present here, and Go/Git sentinels record any attempted
	// invocation. The SSH sentinel refuses ssh -G before a connection exists.
	outside := t.TempDir()
	t.Chdir(outside)
	t.Setenv("HOME", outside)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(outside, "cache"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(outside, "config"))
	trace := filepath.Join(outside, "tools-called")
	t.Setenv("SKYGO_PREBUILT_TOOL_TRACE", trace)
	bin := filepath.Join(outside, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"go", "git"} {
		data := "#!/bin/sh\nprintf '%s\\n' forbidden-tool >> \"$SKYGO_PREBUILT_TOOL_TRACE\"\nexit 99\n"
		if err := os.WriteFile(filepath.Join(bin, name), []byte(data), 0555); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte("#!/bin/sh\nprintf '%s\\n' 'MODELED ssh -G refusal' >&2\nexit 1\n"), 0555); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	// Locate scripts before leaving the original package working directory.
	// Their source is read from the file's embedded absolute path captured below.
	for _, name := range []string{"flags", "refusals", "no_tools"} {
		t.Run(name, func(t *testing.T) {
			var fixture agentbinary.Spec
			if name == "no_tools" {
				fixture = agentbinary.Spec{Path: os.Getenv("SKYGO_SSH_AGENT_BINARY"), Commit: os.Getenv("SKYGO_SSH_AGENT_COMMIT"), Digest: os.Getenv("SKYGO_SSH_AGENT_SHA256")}
				if fixture == (agentbinary.Spec{}) {
					if os.Getenv("SKYGO_REQUIRE_FIXTURES") == "1" {
						t.Fatal("required prebuilt agent fixture pins missing")
					}
					t.Skip("prebuilt agent fixture not supplied")
				}
				if err := fixture.Check(); err != nil {
					t.Fatal(err)
				}
			}
			work := filepath.Join(outside, name)
			if err := os.Mkdir(work, 0700); err != nil {
				t.Fatal(err)
			}
			e := script.NewEngine()
			e.Cmds = scripttest.DefaultCmds()
			e.Conds = scripttest.DefaultConds()
			e.Cmds["skygo-ssh"] = script.Command(script.CmdUsage{Summary: "run the real skygo-ssh argument and client path", Args: "args..."}, func(s *script.State, args ...string) (script.WaitFunc, error) {
				var stderr bytes.Buffer
				err := run(s.Context(), args, &stderr)
				if err != nil {
					fmt.Fprintln(&stderr, err)
				}
				return func(*script.State) (string, string, error) { return "", stderr.String(), err }, nil
			})
			env := []string{"WORK=" + work, "COMMIT=" + strings.Repeat("a", 40), "DIGEST=" + strings.Repeat("b", 64), "BINARY=" + fixture.Path, "REAL_COMMIT=" + fixture.Commit, "REAL_DIGEST=" + fixture.Digest}
			s, err := script.NewState(context.Background(), work, env)
			if err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(scriptDir, "testdata", "prebuilt", name+".txt")
			archive, err := txtar.ParseFile(file)
			if err != nil {
				s.CloseAndWait(io.Discard)
				t.Fatal(err)
			}
			if err := s.ExtractFiles(archive); err != nil {
				s.CloseAndWait(io.Discard)
				t.Fatal(err)
			}
			scripttest.Run(t, e, s, file, bytes.NewReader(archive.Comment))
		})
	}
	if _, err := os.Stat(trace); !os.IsNotExist(err) {
		t.Fatal("source-free path attempted Go or Git")
	}
}
