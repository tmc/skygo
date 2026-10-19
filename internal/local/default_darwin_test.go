//go:build darwin

package local

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestDefaultTerminalRecord(t *testing.T) {
	const pid = 123
	for _, tt := range []struct {
		name              string
		info              defaultSiginfo
		terminal, invalid bool
	}{
		{"exited", defaultSiginfo{signo: 20, pid: pid, code: 1}, true, false},
		{"killed", defaultSiginfo{signo: 20, pid: pid, code: 2}, true, false},
		{"dumped", defaultSiginfo{signo: 20, pid: pid, code: 3}, true, false},
		{"trapped", defaultSiginfo{signo: 20, pid: pid, code: 4}, false, false},
		{"stopped", defaultSiginfo{signo: 20, pid: pid, code: 5}, false, false},
		{"continued", defaultSiginfo{signo: 20, pid: pid, code: 6}, false, false},
		{"zero", defaultSiginfo{}, false, true},
		{"wrong-child", defaultSiginfo{signo: 20, pid: pid + 1, code: 1}, false, true},
		{"wrong-signal", defaultSiginfo{signo: int32(unix.SIGTERM), pid: pid, code: 1}, false, true},
		{"record-errno", defaultSiginfo{signo: 20, pid: pid, code: 1, errno: 1}, false, true},
		{"unknown-code", defaultSiginfo{signo: 20, pid: pid, code: 7}, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			terminal, err := defaultTerminalRecord(tt.info, pid)
			if terminal != tt.terminal || (err != nil) != tt.invalid {
				t.Fatalf("terminal=%v error=%v", terminal, err)
			}
		})
	}
}
