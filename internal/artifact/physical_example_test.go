//go:build linux || darwin

package artifact

import (
	"context"
	"fmt"
)

// The example has an explicitly supplied admission fixture, not production
// capacity policy. Control supplies its actual fresh dedicated cached check.
func examplePublicationGrowth(admitted bool) func() error {
	return func() error {
		if !admitted {
			return ErrFilesBusy
		}
		return nil
	}
}

func ExampleFiles_BeginPhysicalSample() {
	files, _, _, cleanup := exampleFiles()
	defer cleanup()
	// The real service owns a common artifact I/O permit for each operation.
	scan, err := files.BeginPhysicalSample()
	if err != nil {
		panic(err)
	}
	if err := scan.Close(); err != nil {
		panic(err)
	}
	fmt.Println("closed original cursor")
	// Output: closed original cursor
}

func ExamplePhysicalScan_Step() {
	files, _, _, cleanup := exampleFiles()
	defer cleanup()
	scan, err := files.BeginPhysicalSample()
	if err != nil {
		panic(err)
	}
	sample, err := scan.Step(context.Background())
	if err != nil {
		panic(err)
	}
	if err := scan.Close(); err != nil {
		panic(err)
	}
	fmt.Println(sample.Status == PhysicalIntervalComplete, *sample.Inodes >= 2)
	// Output: true true
}

func ExamplePhysicalScan_Close() {
	files, _, _, cleanup := exampleFiles()
	defer cleanup()
	scan, err := files.BeginPhysicalSample()
	if err != nil {
		panic(err)
	}
	first, second := scan.Close(), scan.Close()
	fmt.Println(first == nil, second == nil)
	// Output: true true
}

func ExamplePhysicalSample() {
	var sample PhysicalSample
	fmt.Println(sample.Status == PhysicalUnavailable, sample.AllocatedBytes == nil)
	// Output: true true
}

func ExamplePhysicalStatus() {
	fmt.Println(PhysicalUnavailable < PhysicalPartial, PhysicalPartial < PhysicalIntervalComplete)
	// Output: true true
}

func ExamplePhysicalScan() {
	files, _, _, cleanup := exampleFiles()
	defer cleanup()
	scan, err := files.BeginPhysicalSample()
	if err != nil {
		panic(err)
	}
	for {
		sample, err := scan.Step(context.Background())
		if err != nil {
			panic(err)
		}
		if sample.Status == PhysicalIntervalComplete {
			break
		}
	}
	if err := scan.Close(); err != nil {
		panic(err)
	}
	fmt.Println("interval, not atomic usage")
	// Output: interval, not atomic usage
}

func ExampleFiles_SealPart() {
	files, ref, data, cleanup := exampleFiles()
	defer cleanup()
	id, err := files.CreatePart(ref)
	if err != nil {
		panic(err)
	}
	if _, err := files.WritePart(ref, id, 0, 0, data); err != nil {
		panic(err)
	}
	sealed, err := files.SealPart(ref, id, examplePublicationGrowth(true))
	if err != nil {
		panic(err)
	}
	fmt.Println(sealed.Manifest.Reference == ref)
	// Output: true
}

func ExamplePhysicalEntry() {
	entry := PhysicalEntry{Name: "observed", File: FileID{Device: 1, Inode: 2}, Type: PhysicalEntryRegular}
	fmt.Println(entry.Name, entry.Type == PhysicalEntryRegular)
	// Output: observed true
}

func ExamplePhysicalEntryType() {
	var kind PhysicalEntryType
	fmt.Println(kind == PhysicalEntryOther)
	// Output: true
}
