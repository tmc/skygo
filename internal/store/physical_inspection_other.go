//go:build !linux && !darwin

package store

import (
	"context"
	"errors"
	"os"
)

func openPhysicalFile(string) (*os.File, error) {
	return nil, errors.New("physical inspection unsupported on this platform")
}
func physicalDescriptor(context.Context, *os.File, *PhysicalInspection) (os.FileInfo, error) {
	return nil, errors.New("physical inspection unsupported on this platform")
}
