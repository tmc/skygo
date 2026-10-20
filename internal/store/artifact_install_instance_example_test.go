package store

import (
	"fmt"
	"os"
)

// The root facts here stand for a prior concrete Files initialization. This
// example demonstrates metadata installation, not filesystem ownership.
// The example remains unrun under the S06 execution hold.
func ExampleStore_InstallArtifacts() {
	dir, err := os.MkdirTemp("", "skygo-install-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	s, err := Open(dir)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	facts := ArtifactInstallation{Enabled: true, ID: "0123456789abcdef0123456789abcdef", Unit: 4096, Volume: 17}
	installed, err := s.InstallArtifacts(facts)
	if err != nil {
		panic(err)
	}
	readback, err := s.ArtifactInstallation()
	if err != nil {
		panic(err)
	}
	fmt.Println(installed.Enabled, readback == facts)
	// Output: true true
}
