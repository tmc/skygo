//go:build linux || darwin

package store

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestPhysicalInspectionSpecialFiles(t *testing.T) {
	for _, kind := range []string{"fifo", "socket"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "jobs.db")
			if kind == "fifo" {
				if err := unix.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				// A relative bind avoids Unix socket path limits on long task roots.
				// No ancestor is parallel; the listener closes before Chdir restores.
				t.Chdir(dir)
				listener, err := net.Listen("unix", "jobs.db")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
			}
			before, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			out, err := ProbePhysicalState(context.Background(), dir)
			if err == nil || out.ProbeComplete || out.FileBytes != nil {
				t.Fatalf("special file accepted: %+v %v", out, err)
			}
			after, err := os.Lstat(path)
			if err != nil || !os.SameFile(before, after) {
				t.Fatal("inspection replaced special file", err)
			}
		})
	}
}

func TestPhysicalInspectionReadPermission(t *testing.T) {
	dir := physicalTestState(t)
	path := filepath.Join(dir, "jobs.db")
	before := physicalTestDigest(t, dir)
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(path, 0600)
	out, err := ProbePhysicalState(context.Background(), dir)
	if os.Geteuid() != 0 && (err == nil || out.ProbeComplete) {
		t.Fatal("unreadable source accepted without read authority")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if physicalTestDigest(t, dir) != before {
		t.Fatal("permission error changed source")
	}
}
