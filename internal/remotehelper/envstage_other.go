//go:build !linux && !darwin

package remotehelper

import (
	"errors"
	"os"
)

var errEnvUnsupported = errors.New("artifact staging is unsupported")

func renameNoReplace(fd int, from, to string) error { return errEnvUnsupported }

func lockShared(f *os.File) error { return errEnvUnsupported }

func unlockFile(f *os.File) error { return errEnvUnsupported }

func tryLockExclusive(f *os.File) (bool, error) { return false, errEnvUnsupported }
