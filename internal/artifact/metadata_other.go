//go:build !linux && !darwin

package artifact

import (
	"errors"
	"os"
)

func fileMetadata(os.FileInfo) (metadata, error) {
	return metadata{}, errors.New("artifact filesystem requires Linux or Darwin")
}
func nonblockingRead() (int, error) {
	return 0, errors.New("artifact filesystem requires Linux or Darwin")
}
