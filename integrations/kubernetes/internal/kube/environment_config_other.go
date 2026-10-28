//go:build !linux && !darwin

package kube

import (
	"errors"
	"os"
)

func environmentConfigFileIdentity(*os.File) (qualificationIdentity, error) {
	return qualificationIdentity{}, errors.New("environment profiles unavailable on this platform")
}
