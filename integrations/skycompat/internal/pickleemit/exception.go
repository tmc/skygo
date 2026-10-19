package pickleemit

import (
	"encoding/binary"
	"errors"
	"unicode/utf8"
)

// Exception emits a protocol-4 dictionary for one allowlisted builtin error.
// The only variable data is a valid UTF-8 message of at most 4096 bytes.
func Exception(kind, message string) ([]byte, error) {
	if kind != "RuntimeError" && kind != "ValueError" {
		return nil, errors.New("unsupported exception type")
	}
	if len(message) > 4096 || !utf8.ValidString(message) {
		return nil, errors.New("invalid exception message")
	}
	return exception(kind, message), nil
}

func exception(kind, message string) []byte {
	// PROTO 4, EMPTY_DICT, MARK. No memo, frame, globals or object constructors.
	out := []byte{0x80, 4, '}', '('}
	for _, s := range []string{"type", kind, "message", message, "args", message} {
		out = stringValue(out, s)
	}
	out = append(out, 0x85) // TUPLE1: args is (message,).
	out = stringValue(out, "attributes")
	out = append(out, '}') // EMPTY_DICT
	out = stringValue(out, "stacktrace")
	return append(out, 'N', 'u', '.') // NONE, SETITEMS, STOP
}

func stringValue(out []byte, s string) []byte {
	if len(s) < 256 {
		out = append(out, 0x8c, byte(len(s))) // SHORT_BINUNICODE
	} else {
		out = append(out, 'X') // BINUNICODE
		out = binary.LittleEndian.AppendUint32(out, uint32(len(s)))
	}
	return append(out, s...)
}
