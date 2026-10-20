package local

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ServerLifetime identifies the original server. Its zero value means unavailable.
// A known value is identity evidence, not proof of death or descriptor closure.
// NamespaceDevice and StartTicks may be zero in a known value.
type ServerLifetime struct {
	Host            string
	Boot            string
	NamespaceDevice uint64
	NamespaceInode  uint64
	PID             uint32
	StartTicks      uint64
}

// ErrServerLifetimeUnavailable reports that self identity could not be captured.
// An ordinary unavailable error owns no unsettled descriptor. Callers may retain
// the error and continue live transfers using the zero ServerLifetime.
var ErrServerLifetimeUnavailable = errors.New("server lifetime unavailable")

// ServerLifetimeCloseError retains the original capture descriptor whose Close
// returned an error. The caller must retain this error in its resource frame.
// It must not retry Close, reconstruct a descriptor, or treat this as an ordinary
// unavailable capture. Retention is not a claim that the descriptor remains open.
type ServerLifetimeCloseError struct {
	file       *os.File
	closeErr   error
	captureErr error
}

func (e *ServerLifetimeCloseError) Error() string {
	return fmt.Sprintf("capture server lifetime: close %s: %v", e.file.Name(), e.closeErr)
}

// Unwrap returns the original Close error, without converting it to unavailable.
func (e *ServerLifetimeCloseError) Unwrap() error { return e.closeErr }

func lifetimeUnavailable(err error) error {
	return fmt.Errorf("%w: %v", ErrServerLifetimeUnavailable, err)
}

func lifetimeID(data []byte, uuid bool) (string, error) {
	if len(data) > 128 {
		return "", os.ErrInvalid
	}
	s := string(data)
	if strings.HasSuffix(s, "\n") {
		s = s[:len(s)-1]
	}
	if uuid {
		if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
			return "", os.ErrInvalid
		}
		s = s[:8] + s[9:13] + s[14:18] + s[19:23] + s[24:]
	}
	if len(s) != 32 || s == "00000000000000000000000000000000" {
		return "", os.ErrInvalid
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return "", os.ErrInvalid
		}
	}
	return s, nil
}

// lifetimeStart reads field22 of a bounded /proc/self/stat record. The command
// in parentheses may itself contain spaces, parentheses and newlines.
func lifetimeStart(data []byte, pid uint32) (uint64, error) {
	if len(data) > 4096 || len(data) == 0 || data[len(data)-1] != '\n' {
		return 0, os.ErrInvalid
	}
	s := string(data[:len(data)-1])
	prefix := strconv.FormatUint(uint64(pid), 10) + " ("
	end := strings.LastIndex(s, ") ")
	if !strings.HasPrefix(s, prefix) || end < len(prefix) || pid == 0 || pid > 2147483647 {
		return 0, os.ErrInvalid
	}
	tail := s[end+2:]
	if strings.ContainsAny(tail, "\n\r\t") {
		return 0, os.ErrInvalid
	}
	fields := strings.Fields(tail)
	if len(fields) < 20 || len(fields[0]) != 1 {
		return 0, os.ErrInvalid
	}
	value := fields[19]
	if value == "" {
		return 0, os.ErrInvalid
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return 0, os.ErrInvalid
		}
	}
	return strconv.ParseUint(value, 10, 64)
}
