package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

func ExampleNewRetained() {
	directory, err := os.MkdirTemp("", "skygo-output-example-")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(directory)
	// Server assembly obtains these persisted facts from the stopped-state policy.
	unit, volume, err := outputFilesystem(directory)
	if err != nil {
		fmt.Println(err)
		return
	}
	b, err := NewRetained(directory, RetentionConfig{Owner: "0123456789abcdef0123456789abcdef", OutputUnit: unit, Volume: volume, OutputBytes: 256 << 20, OutputDomains: 256, OutputFiles: 512})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer b.Close()
	sample, err := b.OutputUsage(context.Background())
	fmt.Println(b.OutputReservation() > 0, b.Retention() != nil, sample.Files, err)
	// Output: true true 0 <nil>
}

func ExampleBackend_BindOutput() {
	directory, err := os.MkdirTemp("", "skygo-bound-output-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(directory)
	info, err := store.EnableRetention(directory, store.DefaultRetentionPolicy())
	if err != nil {
		panic(err)
	}
	s, err := store.Open(directory)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	p := info.Policy
	b, err := NewRetained(filepath.Join(directory, "logs"), RetentionConfig{Owner: s.Owner(), OutputUnit: p.OutputUnit, Volume: p.Volume, OutputBytes: p.OutputBytes, OutputDomains: p.OutputDomains, OutputFiles: p.OutputFiles})
	if err != nil {
		panic(err)
	}
	defer b.Close()
	binding, err := backend.LocalBinding(s.Owner())
	if err != nil {
		panic(err)
	}
	engine := auth.NewLegacy("") // Explicit single-operator mode in a private example.
	actor, err := engine.Authenticate("")
	if err != nil {
		panic(err)
	}
	job, err := s.SubmitRetained(actor, info.Domain, "training", backend.Task{Argv: []string{"trainer"}}, "local", binding, backend.LocalOutput, b.OutputReservation())
	if err != nil {
		panic(err)
	}
	// Commit start intent and output charge without calling Backend.Start.
	job, start, err := s.BeginAttempt(job.ID, job.ID, b.OutputReservation(), job.CreatedAt)
	if err != nil || !start {
		panic(fmt.Errorf("begin attempt: %v", err))
	}
	// Only the committed reservation supplies this constructor authority.
	ref, err := s.OutputReference(job.ID, job.ID)
	if err != nil {
		panic(err)
	}
	fmt.Println(b.BindOutput(ref), b.BindOutput(ref))
	usage, err := b.OutputUsage(context.Background())
	fmt.Println(usage.LogicalBytes, usage.Files, err) // No Start, so no log file.
	receipt, err := b.Retention().CheckOutputClosed(context.Background(), ref)
	fmt.Println(receipt.ClosureID == "", err != nil)
	// A committed binding does not grant writer closure or native stopped proof.

	// Output:
	// <nil> <nil>
	// 0 0 <nil>
	// true true
}
