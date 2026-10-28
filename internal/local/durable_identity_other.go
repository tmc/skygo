//go:build !linux && !darwin

package local

import "errors"

func bootIdentity() (string, error)    { return "", errors.New("unsupported operating system") }
func processStart(int) (uint64, error) { return 0, errors.New("unsupported operating system") }
