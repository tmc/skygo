package backend

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

// TaskBytes returns task's canonical JSON encoding. Zero extension fields
// preserve the original task encoding. It does not validate execution policy
// or include a server-selected target binding.
func TaskBytes(task Task) ([]byte, error) { return json.Marshal(task) }

// LocalBinding returns the logical built-in local target digest for a valid
// 32-character lowercase hexadecimal installation ID. It performs no I/O and
// proves neither physical host identity nor authority to migrate legacy jobs.
// The caller must first establish the actual built-in local backend.
func LocalBinding(installation string) (string, error) {
	if len(installation) != 32 {
		return "", errors.New("invalid installation id")
	}
	for _, c := range installation {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return "", errors.New("invalid installation id")
		}
	}
	data, err := json.Marshal(struct {
		Format       string `json:"format"`
		Installation string `json:"installation"`
		Kind         string `json:"kind"`
	}{"skygo-local-target-v1", installation, "local"})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}
