package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/backend"
)

// A stored job or cleanup header holds a task that may not request workers.
// backend.Task has no Workers field, so json.Unmarshal would silently drop
// such a request; storedTask refuses the raw bytes before they are decoded.
var (
	errStoredJobWorkers    = fmt.Errorf("stored job requests workers: %w", errWorkerRoute)
	errStoredHeaderWorkers = fmt.Errorf("cleanup header task requests workers: %w", errWorkerRoute)
)

// storedTask returns refusal if data, the JSON object of a stored job or
// cleanup header, holds a task that requests workers, and an error if data
// is malformed. It examines only the root members that json.Unmarshal would
// decode into the "task" field, and within each such object only the members
// it would decode as "workers": keys are unescaped and matched as
// encoding/json matches field names, ignoring case. Every duplicate of either
// member is examined, so a later null or plain occurrence cannot hide an
// earlier request. An absent or null workers member is ordinary.
func storedTask(data []byte, refusal error) error {
	if !json.Valid(data) {
		return errors.New("malformed stored record")
	}
	workers := false
	err := eachMember(data, "task", func(task []byte) error {
		return eachMember(task, "workers", func(w []byte) error {
			if !bytes.Equal(w, []byte("null")) {
				workers = true
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	if workers {
		return refusal
	}
	return nil
}

// eachMember calls fn with the value of each member of the JSON object data
// whose unescaped key equals name under Unicode case folding, in order and
// including duplicates. Data that is not an object has no members. Data must
// be valid JSON: eachMember finds the extent of each value without checking
// its syntax, and does not copy the values of other members. Each value
// passed to fn is a copy that fn may retain or modify.
func eachMember(data []byte, name string, fn func(value []byte) error) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return nil
	}
	i := memberSpace(data, 1)
	if i < len(data) && data[i] == '}' {
		return nil
	}
	for i < len(data) && data[i] == '"' {
		end := memberString(data, i)
		if end < 0 {
			break
		}
		match, err := memberKeyIs(data[i:end], name)
		if err != nil {
			return err
		}
		i = memberSpace(data, end)
		if i >= len(data) || data[i] != ':' {
			break
		}
		start := memberSpace(data, i+1)
		end = memberValue(data, start)
		if end < 0 {
			break
		}
		if match {
			if err := fn(bytes.TrimSpace(bytes.Clone(data[start:end]))); err != nil {
				return err
			}
		}
		i = memberSpace(data, end)
		if i < len(data) && data[i] == '}' {
			return nil
		}
		if i >= len(data) || data[i] != ',' {
			break
		}
		i = memberSpace(data, i+1)
	}
	return errors.New("malformed stored record")
}

// memberKeyIs reports whether the JSON string key, unescaped as
// encoding/json unescapes it, equals name under Unicode case folding.
// A key of ASCII bytes without escapes unescapes to itself.
func memberKeyIs(key []byte, name string) (bool, error) {
	raw := key[1 : len(key)-1]
	for _, c := range raw {
		if c == '\\' || c >= utf8.RuneSelf {
			var s string
			if err := json.Unmarshal(key, &s); err != nil {
				return false, err
			}
			return strings.EqualFold(s, name), nil
		}
	}
	return strings.EqualFold(string(raw), name), nil
}

// memberSpace returns the index of the first byte of data at or after i
// that is not JSON white space.
func memberSpace(data []byte, i int) int {
	for i < len(data) {
		switch data[i] {
		case ' ', '\t', '\r', '\n':
			i++
		default:
			return i
		}
	}
	return i
}

// memberString returns the end of the JSON string that begins with the
// quote data[i], or -1 if it is unterminated.
func memberString(data []byte, i int) int {
	j := i + 1
	for {
		k := bytes.IndexByte(data[j:], '"')
		if k < 0 {
			return -1
		}
		j += k
		// A quote preceded by an odd run of backslashes is escaped.
		n := 0
		for j-1-n > i && data[j-1-n] == '\\' {
			n++
		}
		if n%2 == 0 {
			return j + 1
		}
		j++
	}
}

// memberValue returns the end of the JSON value that begins at data[i], a
// member value, or -1 if it is incomplete. It skips nested values
// iteratively, counting only their depth.
func memberValue(data []byte, i int) int {
	depth := 0
	for i < len(data) {
		switch data[i] {
		case '"':
			if i = memberString(data, i); i < 0 {
				return -1
			}
		case '{', '[':
			depth++
			i++
		case '}', ']':
			if depth == 0 {
				return -1
			}
			depth--
			i++
		default:
			if depth > 0 {
				i++
				continue
			}
			// A number, true, false or null ends at the delimiter that
			// follows every member value.
			j := i
			for j < len(data) && strings.IndexByte(",} \t\r\n", data[j]) < 0 {
				j++
			}
			if j == i || j == len(data) {
				return -1
			}
			return j
		}
		if depth == 0 {
			return i
		}
	}
	return -1
}

// decodeStoredJob decodes the persisted job data into j, after refusing a
// task that requests workers. Every decode of a stored or planned job row
// uses it; freshly marshaled typed jobs need not.
func decodeStoredJob(data []byte, j *backend.Job) error {
	if err := storedTask(data, errStoredJobWorkers); err != nil {
		return err
	}
	return json.Unmarshal(data, j)
}

// decodeCleanupHeader decodes the persisted cleanup header data into h, after
// refusing a header whose task requests workers. A header can outlive its
// job, so no job check covers it. loadJSON uses it for every *cleanupHeader.
func decodeCleanupHeader(data []byte, h *cleanupHeader) error {
	if err := storedTask(data, errStoredHeaderWorkers); err != nil {
		return err
	}
	return json.Unmarshal(data, h)
}
