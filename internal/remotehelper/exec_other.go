//go:build !linux && !darwin

package remotehelper

import "errors"

func execute(directory string, args, env []string) error { return errors.New("exec is unsupported") }
