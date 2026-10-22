//go:build !linux && !darwin

package kube

import (
	"errors"
	"os"
)

func openQualificationFile(string) (*os.File, error) {
	return nil, errors.New("kubernetes qualification file unavailable on this platform")
}

func qualificationFileIdentity(*os.File) (qualificationIdentity, error) {
	return qualificationIdentity{}, errors.New("kubernetes qualification file unavailable on this platform")
}
