//go:build linux || darwin

package store_test

import (
	"context"
	"fmt"
	"os"

	"github.com/tmc/skygo/internal/store"
)

func physicalExampleDirectory() (string, error) {
	dir, err := os.MkdirTemp("", "skygo-inspection-")
	if err != nil {
		return "", err
	}
	s, err := store.Open(dir)
	if err != nil {
		os.RemoveAll(dir)
		return "", err
	}
	if err = s.Close(); err != nil {
		os.RemoveAll(dir)
		return "", err
	}
	return dir, nil
}

func ExampleProbePhysicalState() {
	dir, err := physicalExampleDirectory()
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	observation, err := store.ProbePhysicalState(context.Background(), dir)
	if err != nil {
		panic(err)
	}
	fmt.Println(observation.ProbeComplete, observation.GeometryStatus, observation.ExaminedEntries)
	// Output: true not_requested 0
}

func ExampleInspectPhysicalState() {
	dir, err := physicalExampleDirectory()
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	observation, err := store.InspectPhysicalState(context.Background(), dir)
	if err != nil {
		panic(err)
	}
	fmt.Println(observation.GeometryStatus, *observation.Jobs, *observation.AddedLegacyIDPayloadLowerBound)
	// Output: complete 0 0
}

func ExamplePhysicalInspection() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	observation, err := store.ProbePhysicalState(ctx, "not-opened")
	// Unknown physical and geometry values are absent, not measured zero.
	fmt.Println(observation.FileBytes == nil, observation.Jobs == nil, err != nil)
	// Output: true true true
}
