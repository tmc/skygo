//go:build !(darwin || linux)

package ec2ctl

import (
	"errors"
	"io/fs"
	"os"
)

const (
	openNoFollow  = 0
	openDirectory = 0
	openNonblock  = 0
)

var errPlatform = errors.New("ec2ctl: private state needs Linux or macOS")

func lockFile(*os.File) error                      { return errPlatform }
func checkPrivate(string, fs.FileInfo, bool) error { return errPlatform }
func syncDir(*os.File) error                       { return errPlatform }
