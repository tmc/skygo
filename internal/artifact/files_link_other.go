//go:build !linux && !darwin

package artifact

import (
	"errors"
	"os"
)

func linkFile(int, string, string) error {
	return errors.New("artifact publication requires Linux or Darwin")
}

func filesystemUnit(*os.File) (uint64, error) {
	return 0, errors.New("artifact filesystem facts require Linux or Darwin")
}
