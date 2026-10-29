//go:build !linux && !darwin

package main

import (
	"errors"
	"os"
)

func openTaskFile(string) (*os.File, error) {
	return nil, errors.New("task file input requires linux or darwin")
}
