//go:build linux || darwin

package ec2ctl

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

// These tests put real FIFOs, owned by the user with mode 0600 and with no
// writer, where an input file belongs. Opening such a FIFO for reading
// without O_NONBLOCK waits for a writer, and no context or signal handler
// interrupts that wait.

const goodCredentials = `{"access_key_id": "AKIDFIXTURE00000", "secret_access_key": "fixture/secret"}`

// mkfifo makes a private FIFO named name.
func mkfifo(t *testing.T, name string) {
	t.Helper()
	must(t, syscall.Mkfifo(name, 0o600))
	must(t, os.Chmod(name, 0o600))
}

// refusePromptly calls read, which must refuse the FIFO fifo as not a
// regular file. The 30-second bound is test control only: if read is
// still blocked opening fifo by then, refusePromptly releases it by
// opening fifo as a writer, waits for read to return and fails the test,
// leaving no goroutine behind.
func refusePromptly(t *testing.T, fifo string, read func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- read() }()
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "is not a regular file") {
			t.Fatalf("error %v, want a refusal of a non-regular file", err)
		}
	case <-timer.C:
		w, err := os.OpenFile(fifo, os.O_RDWR|syscall.O_NONBLOCK, 0)
		if err != nil {
			t.Fatalf("read blocked opening the FIFO, and releasing it failed: %v", err)
		}
		<-done
		w.Close()
		t.Fatal("read blocked opening the FIFO")
	}
}

// listing describes every entry of dir.
func listing(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	must(t, err)
	m := make(map[string]string)
	for _, de := range entries {
		fi, err := de.Info()
		must(t, err)
		m[de.Name()] = fmt.Sprint(fi.Mode(), fi.Size(), fi.ModTime().UnixNano())
	}
	return m
}

func TestFIFOInputsRefused(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	creds := filepath.Join(dir, "credentials.json")
	mkfifo(t, config)
	mkfifo(t, creds)
	before := listing(t, dir)

	refusePromptly(t, config, func() error {
		_, err := LoadConfig(config)
		return err
	})
	refusePromptly(t, creds, func() error {
		_, err := LoadCredentials(creds)
		return err
	})
	if after := listing(t, dir); !reflect.DeepEqual(after, before) {
		t.Fatalf("directory changed: %v, was %v", after, before)
	}
}

func TestFIFOStateRefused(t *testing.T) {
	e := newEnv(t)
	e.create().Close()
	state := filepath.Join(e.dir, stateName)
	must(t, os.Remove(state))
	mkfifo(t, state)
	before := listing(t, e.dir)
	requests := e.srv.Total()

	refusePromptly(t, state, func() error {
		_, err := ReadStatus(e.dir, e.cfg)
		return err
	})
	refusePromptly(t, state, func() error {
		m, err := Open(e.dir, e.cfg)
		if err == nil {
			m.Close()
		}
		return err
	})
	if after := listing(t, e.dir); !reflect.DeepEqual(after, before) {
		t.Fatalf("state directory changed: %v, was %v", after, before)
	}
	if n := e.srv.Total(); n != requests {
		t.Fatalf("%d provider requests, want none", n-requests)
	}
}

func TestUnsafeInputsRefused(t *testing.T) {
	loaders := []struct {
		name string
		text string
		load func(string) error
	}{
		{"LoadConfig", goodConfig, func(p string) error { _, err := LoadConfig(p); return err }},
		{"LoadCredentials", goodCredentials, func(p string) error { _, err := LoadCredentials(p); return err }},
	}
	for _, l := range loaders {
		t.Run(l.name, func(t *testing.T) {
			dir := t.TempDir()
			good := filepath.Join(dir, "good.json")
			must(t, os.WriteFile(good, []byte(l.text), 0o600))
			must(t, os.Chmod(good, 0o600))
			if err := l.load(good); err != nil {
				t.Fatalf("regular file: %v", err)
			}

			fifo := filepath.Join(dir, "fifo")
			mkfifo(t, fifo)
			sub := filepath.Join(dir, "sub")
			must(t, os.Mkdir(sub, 0o700))
			bad := []struct{ name, target string }{
				{"symlink", good},
				{"symlink to FIFO", fifo},
				{"symlink to directory", sub},
			}
			for _, b := range bad {
				link := filepath.Join(dir, strings.ReplaceAll(b.name, " ", "-"))
				must(t, os.Symlink(b.target, link))
				if err := l.load(link); err == nil {
					t.Errorf("%s: accepted", b.name)
				}
			}
			if err := l.load(sub); err == nil || !strings.Contains(err.Error(), "is not a regular file") {
				t.Errorf("directory: error %v, want a refusal of a non-regular file", err)
			}
		})
	}
}
