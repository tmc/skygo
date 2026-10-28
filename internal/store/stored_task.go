package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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
// be valid JSON.
func eachMember(data []byte, name string, fn func(value []byte) error) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return nil
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if _, err := d.Token(); err != nil {
		return err
	}
	for d.More() {
		t, err := d.Token()
		if err != nil {
			return err
		}
		key, ok := t.(string)
		if !ok {
			return errors.New("malformed stored record")
		}
		var v json.RawMessage
		if err := d.Decode(&v); err != nil {
			return err
		}
		if strings.EqualFold(key, name) {
			if err := fn(bytes.TrimSpace(v)); err != nil {
				return err
			}
		}
	}
	return nil
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
