//go:build !linux && !darwin

package remotehelper

import "errors"

func publish(fd int) error { return errors.New("stage publication is unsupported") }
