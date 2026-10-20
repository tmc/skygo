//go:build !linux && !darwin

package artifact

import (
	"errors"
	"os"
)

func physicalUnsupported() error {
	return errors.New("artifact physical sampling requires Linux or Darwin")
}
func physicalDirectory(physicalObservation) bool               { return false }
func physicalEntryType(physicalObservation) PhysicalEntryType  { return PhysicalEntryOther }
func openPhysicalDirectory(*os.File, string) (*os.File, error) { return nil, physicalUnsupported() }
func openPhysicalMarker(*os.File, string) (*os.File, error)    { return nil, physicalUnsupported() }
func physicalAt(*os.File, string) (physicalObservation, error) {
	return physicalObservation{}, physicalUnsupported()
}
func physicalDescriptor(*os.File) (physicalObservation, error) {
	return physicalObservation{}, physicalUnsupported()
}
func physicalFree(*os.File) (uint64, error) { return 0, physicalUnsupported() }
