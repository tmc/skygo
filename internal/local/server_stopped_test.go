package local

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// These are identity policy vectors, not native cessation evidence.
func TestStoppedLifetimePolicy(t *testing.T) {
	current := ServerLifetime{
		Host: "11111111111111111111111111111111", Boot: "22222222222222222222222222222222",
		NamespaceDevice: 1, NamespaceInode: 2, PID: 3, StartTicks: 4,
	}
	original := current
	original.PID++
	tests := []struct {
		name string
		edit func(*ServerLifetime)
	}{
		{"zero", func(v *ServerLifetime) { *v = ServerLifetime{} }},
		{"host", func(v *ServerLifetime) { v.Host = "33333333333333333333333333333333" }},
		{"boot", func(v *ServerLifetime) { v.Boot = "33333333333333333333333333333333" }},
		{"device", func(v *ServerLifetime) { v.NamespaceDevice++ }},
		{"namespace", func(v *ServerLifetime) { v.NamespaceInode++ }},
		{"zero device", func(v *ServerLifetime) { v.NamespaceDevice = 0 }},
		{"zero start", func(v *ServerLifetime) { v.StartTicks = 0 }},
		{"invalid pid", func(v *ServerLifetime) { v.PID = 2147483648 }},
		{"malformed host", func(v *ServerLifetime) { v.Host += "\n" }},
		{"self", func(v *ServerLifetime) { *v = current }},
	}
	if !stoppedLifetimePair(current, original) {
		t.Fatal("strict same-domain pair rejected")
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := original
			test.edit(&value)
			if stoppedLifetimePair(current, value) || stoppedLifetimePair(value, current) {
				t.Fatal("unqualified pair accepted")
			}
			if stopped, err := ObserveServerStopped(current, value); stopped || !errors.Is(err, ErrServerLifetimeUnavailable) {
				t.Fatalf("ObserveServerStopped = %v, %v", stopped, err)
			}
		})
	}
}

var stoppedPolicyKeepers struct {
	sync.Mutex
	files       []*os.File
	errors      []error
	directories []string
}

func keepStoppedPolicy(file *os.File, err error, directory string) {
	stoppedPolicyKeepers.Lock()
	defer stoppedPolicyKeepers.Unlock()
	stoppedPolicyKeepers.files = append(stoppedPolicyKeepers.files, file)
	stoppedPolicyKeepers.errors = append(stoppedPolicyKeepers.errors, err)
	stoppedPolicyKeepers.directories = append(stoppedPolicyKeepers.directories, directory)
}

// Deliberately preclosing a file tests custody policy, not a spontaneous kernel
// Close failure. The finisher must retain this exact File and original cause.
func TestStoppedQueryClosePolicy(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(home, "tmp", "skypilot", "20261020-stopped-reader-tests")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(root, "query-policy-")
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(filepath.Join(directory, "query"))
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		keepStoppedPolicy(file, err, directory)
		t.Fatal(err)
	}
	// This original Close succeeded, so removing the known-closed fixture is safe.
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Error(err)
		}
	})
	stopped, err := closeStoppedQuery(file)
	keepStoppedPolicy(file, err, directory)
	var closeErr *ServerStoppedCloseError
	if stopped || !errors.As(err, &closeErr) {
		t.Fatalf("closeStoppedQuery = %v, %v", stopped, err)
	}
	if closeErr.file != file || closeErr.closeErr == nil || errors.Unwrap(closeErr) != closeErr.closeErr {
		t.Fatal("original close frame lost")
	}
	if !errors.Is(err, os.ErrClosed) || errors.Is(err, ErrServerLifetimeUnavailable) {
		t.Fatalf("close uncertainty misclassified: %v", err)
	}
}
