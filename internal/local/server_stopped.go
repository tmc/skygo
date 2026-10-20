package local

import (
	"fmt"
	"os"
)

// ServerStoppedCloseError retains the original observation descriptor whose
// Close returned an error. Callers must retain this error in its resource frame,
// without retrying Close or reconstructing the descriptor. Retention does not
// imply that the descriptor remains open or usable.
type ServerStoppedCloseError struct {
	file     *os.File
	closeErr error
}

func (e *ServerStoppedCloseError) Error() string {
	return fmt.Sprintf("observe stopped server: close %s: %v", e.file.Name(), e.closeErr)
}

// Unwrap returns the original Close error.
func (e *ServerStoppedCloseError) Unwrap() error { return e.closeErr }

func stoppedLifetimeValid(value ServerLifetime) bool {
	if value.NamespaceDevice == 0 || value.NamespaceInode == 0 ||
		value.PID == 0 || value.PID > 2147483647 || value.StartTicks == 0 {
		return false
	}
	if host, err := lifetimeID([]byte(value.Host), false); err != nil || host != value.Host {
		return false
	}
	boot, err := lifetimeID([]byte(value.Boot), false)
	return err == nil && boot == value.Boot
}

func stoppedLifetimePair(current, original ServerLifetime) bool {
	return stoppedLifetimeValid(current) && stoppedLifetimeValid(original) &&
		current != original && current.Host == original.Host && current.Boot == original.Boot &&
		current.NamespaceDevice == original.NamespaceDevice && current.NamespaceInode == original.NamespaceInode
}

func closeStoppedQuery(file *os.File) (bool, error) {
	if err := file.Close(); err != nil {
		return false, &ServerStoppedCloseError{file: file, closeErr: err}
	}
	return false, nil
}
