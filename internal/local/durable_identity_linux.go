//go:build linux

package local

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
)

func bootIdentity() (string, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("read boot id: %w", err)
	}
	id := strings.TrimSpace(string(data))
	if len(id) != 36 {
		return "", errors.New("malformed boot id")
	}
	return id, nil
}

// processStart returns the starttime field of /proc/<pid>/stat, in clock
// ticks since boot. Zombies have exited and are reported as gone.
func processStart(pid int) (uint64, error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if errors.Is(err, fs.ErrNotExist) {
		return 0, errProcessGone
	}
	if err != nil {
		return 0, err
	}
	// The command name may contain spaces and parentheses; it ends at the last ')'.
	i := bytes.LastIndexByte(data, ')')
	if i < 0 {
		return 0, errors.New("malformed process stat")
	}
	fields := strings.Fields(string(data[i+1:]))
	if len(fields) < 20 {
		return 0, errors.New("malformed process stat")
	}
	if fields[0] == "Z" || fields[0] == "X" {
		return 0, errProcessGone
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || start == 0 {
		return 0, errors.New("malformed process start time")
	}
	return start, nil
}
