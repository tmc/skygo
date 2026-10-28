//go:build !linux && !darwin

package local

import "errors"

func hostMemory() (int64, error) { return 0, errors.New("unsupported operating system") }
