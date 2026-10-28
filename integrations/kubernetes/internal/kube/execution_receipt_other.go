//go:build !(linux || darwin)

package kube

import (
	"errors"
	"io/fs"
	"os"
)

var errReceiptPlatform = errors.New("kubernetes execution receipts require linux or darwin")

func checkPrivate(os.FileInfo, bool) error                       { return errReceiptPlatform }
func checkPrivateDir(string) error                               { return errReceiptPlatform }
func dirID(string) (fileID, error)                               { return fileID{}, errReceiptPlatform }
func openScope(string, fileID, string, fileID) (*os.File, error) { return nil, errReceiptPlatform }
func openAt(*os.File, string, int) (*os.File, error)             { return nil, errReceiptPlatform }
func removeAt(*os.File, string) error                            { return errReceiptPlatform }
func renameAt(*os.File, string, string) error                    { return errReceiptPlatform }
func syncAt(*os.File) error                                      { return errReceiptPlatform }
func readDirAt(*os.File) ([]fs.DirEntry, error)                  { return nil, errReceiptPlatform }
func flock(*os.File, bool, bool) (bool, error)                   { return false, errReceiptPlatform }
func makeScope(string, fileID, string, func() error) (fileID, error) {
	return fileID{}, errReceiptPlatform
}
