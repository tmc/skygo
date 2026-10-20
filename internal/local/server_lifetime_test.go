package local

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestLifetimeID(t *testing.T) {
	const id = "0123456789abcdef0123456789abcdef"
	for _, test := range []struct {
		name  string
		input string
		uuid  bool
		want  string
	}{
		{"machine", id + "\n", false, id},
		{"machine-no-newline", id, false, id},
		{"boot", "01234567-89ab-cdef-0123-456789abcdef\n", true, id},
		{"empty", "", false, ""},
		{"zero", strings.Repeat("0", 32), false, ""},
		{"upper", strings.ToUpper(id), false, ""},
		{"space", id + " ", false, ""},
		{"extra-newline", id + "\n\n", false, ""},
		{"unbounded", strings.Repeat("a", 129), false, ""},
		{"boot-no-hyphens", id, true, ""},
		{"boot-wrong-hyphens", "0123456-789ab-cdef-0123-456789abcdef", true, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := lifetimeID([]byte(test.input), test.uuid)
			if test.want == "" {
				if err == nil {
					t.Fatal("accepted invalid identity")
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("got %q, %v; want %q", got, err, test.want)
			}
		})
	}
}

func TestLifetimeStart(t *testing.T) {
	// Literal fields3..22: field20=threads,21=itrealvalue,22=starttime.
	const tail = "R 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 "
	for _, test := range []struct {
		name  string
		input string
		pid   uint32
		want  uint64
		valid bool
	}{
		{"ordinary", "123 (server) " + tail + "42\n", 123, 42, true},
		{"command-delimiters", "123 (a) b (c)\nlast) " + tail + "0\n", 123, 0, true},
		{"maximum", "123 (server) " + tail + "18446744073709551615 999\n", 123, ^uint64(0), true},
		{"other-pid", "124 (server) " + tail + "42\n", 123, 0, false},
		{"zero-pid", "0 (server) " + tail + "42\n", 0, 0, false},
		{"large-pid", "2147483648 (server) " + tail + "42\n", 2147483648, 0, false},
		{"overflow", "123 (server) " + tail + "18446744073709551616\n", 123, 0, false},
		{"negative", "123 (server) " + tail + "-1\n", 123, 0, false},
		{"plus", "123 (server) " + tail + "+1\n", 123, 0, false},
		{"truncated", "123 (server) R 1 2\n", 123, 0, false},
		{"no-eof-newline", "123 (server) " + tail + "42", 123, 0, false},
		{"oversize", "123 (" + strings.Repeat("a", 4096) + ") " + tail + "42\n", 123, 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := lifetimeStart([]byte(test.input), test.pid)
			if test.valid && (err != nil || got != test.want) {
				t.Fatalf("got %d, %v; want %d", got, err, test.want)
			}
			if !test.valid && err == nil {
				t.Fatal("accepted invalid stat record")
			}
		})
	}
}

func TestLifetimeCloseOwner(t *testing.T) {
	// Structural retention oracle only: no descriptor, injected Close syscall,
	// kernel uncertainty or death observation is represented by this fixture.
	file := new(os.File)
	closeErr := errors.New("original close uncertainty")
	readErr := errors.New("original read failure")
	err := &ServerLifetimeCloseError{file: file, closeErr: closeErr, captureErr: readErr}
	var retained *ServerLifetimeCloseError
	if !errors.As(err, &retained) || retained.file != file || retained.closeErr != closeErr || retained.captureErr != readErr {
		t.Fatal("lost original owner or errors")
	}
	if !errors.Is(err, closeErr) || errors.Is(err, ErrServerLifetimeUnavailable) {
		t.Fatal("close uncertainty converted to ordinary unavailable")
	}
	if !errors.Is(lifetimeUnavailable(readErr), ErrServerLifetimeUnavailable) {
		t.Fatal("missing unavailable classification")
	}
}
