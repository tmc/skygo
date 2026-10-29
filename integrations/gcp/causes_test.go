//go:build unix

package gce

import (
	"errors"
	"strings"
	"testing"
)

// Simultaneous failures keep every original cause. Each injected cause
// carries secret text; every returned error has a fixed message that
// carries none of it, while errors.Is and errors.As reach each cause. The
// machine keeps the actual call, and no body is closed twice. MODELED
// (see compute_test.go and bodyFault); NOT_RUN by the author.
func TestDualFailureCauses(t *testing.T) {
	errRead := errors.New("injected read failure secret-read ya29.read")
	errClose := errors.New("injected close failure secret-close ya29.close")
	errCommit := errors.New("injected commit failure secret-commit ya29.commit")
	tests := []struct {
		name   string
		method string
		read   bool // body Read fails
		close  bool // first body Close fails
		commit bool // the commit of a mutation's reply fails
		status func(*fakeCompute)
		want   []error
	}{
		{"get read and close", "GET", true, true, false, nil, []error{errRead, errClose, errTransient}},
		{"mutation read and close", "POST", true, true, false, nil, []error{errRead, errClose}},
		{"get classify and close", "GET", false, true, false, func(f *fakeCompute) { f.getStatus = 403 }, []error{errAuth, errClose}},
		{"mutation classify and close", "POST", false, true, false, func(f *fakeCompute) { f.mutStatus = 400 }, []error{errRefused, errClose}},
		{"mutation commit and close", "POST", false, true, true, nil, []error{errClose, errCommit}},
		{"mutation classify and commit", "POST", false, false, true, func(f *fakeCompute) { f.mutStatus = 400 }, []error{errRefused, errCommit}},
		{"mutation read and commit", "POST", true, false, true, nil, []error{errRead, errCommit}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			on := false
			injectSync(t, &on, errCommit)
			f := newFake(t)
			if tt.status != nil {
				tt.status(f)
			}
			b := &bodyFault{method: tt.method}
			if tt.read {
				b.readErr = errRead
			}
			if tt.close {
				b.err = errClose
			}
			m, _ := newTestMachineWith(t, f, f.faultClient(b))
			m.cut = func(p string) error {
				if p == "receipt" && tt.commit {
					on = true // the reply's commit fails after its rename
				}
				return nil
			}
			_, err := drive(t, m, 50)
			cerr := m.Close()
			on = false
			if err == nil || cerr == nil {
				t.Fatalf("Drive = %v, Close = %v; want both to fail", err, cerr)
			}
			// Drive reports every cause. Close reports what still fences
			// the machine: the kept call with all its causes, or the
			// commit it retried.
			for _, w := range tt.want {
				if !errors.Is(err, w) {
					t.Errorf("Drive: cause %q not reachable", w)
				}
				if (tt.close || w == errCommit) && !errors.Is(cerr, w) {
					t.Errorf("Close: cause %q not reachable", w)
				}
			}
			for _, e := range []error{err, cerr} {
				var ce *causeError
				if !errors.As(e, &ce) {
					t.Errorf("%v: no fixed-message error", e)
				}
				for _, s := range []string{"secret-", "ya29", secretText, testToken} {
					if strings.Contains(e.Error(), s) {
						t.Errorf("error message carries %q: %s", s, e.Error())
					}
				}
			}
			if tt.close {
				if m.held == nil || m.held.closeErr != errClose || m.held.resp == nil {
					t.Errorf("machine does not keep the call: %+v", m.held)
				}
				var ce *causeError
				if !errors.As(err, &ce) || ce.c != m.held {
					t.Error("the error does not keep the actual call")
				}
			}
			if n := b.closes.Load(); n > 1 {
				t.Errorf("body closed %d times", n)
			}
			if tt.method == "GET" && f.mutations() != 0 {
				t.Error("a mutation followed the failed read")
			}
		})
	}
}
