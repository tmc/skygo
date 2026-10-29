//go:build !unix

package gce

import (
	"errors"
	"io/fs"
	"os"
)

const openNoFollow = 0

var (
	errBusy        = errors.New("state directory is locked by another machine")
	errUnsupported = errors.New("private state files are supported only on unix systems")
)

func lockFile(*os.File) error                      { return errUnsupported }
func checkPrivate(string, fs.FileInfo, bool) error { return errUnsupported }
func syncDir(*os.File) error                       { return errUnsupported }
