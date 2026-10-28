package local

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// hostMemory reports MemTotal from /proc/meminfo: installed memory usable by
// the kernel, not memory currently free.
func hostMemory() (int64, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return parseMeminfo(io.LimitReader(f, 64<<10))
}

func parseMeminfo(r io.Reader) (int64, error) {
	s := bufio.NewScanner(r)
	for s.Scan() {
		rest, ok := strings.CutPrefix(s.Text(), "MemTotal:")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) != 2 || fields[1] != "kB" {
			return 0, fmt.Errorf("invalid MemTotal %q", rest)
		}
		kb, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil || kb <= 0 || kb > (1<<63-1)/1024 {
			return 0, fmt.Errorf("invalid MemTotal %q", rest)
		}
		return kb * 1024, nil
	}
	if err := s.Err(); err != nil {
		return 0, err
	}
	return 0, errors.New("MemTotal not found")
}
