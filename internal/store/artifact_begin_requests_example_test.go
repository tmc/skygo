//go:build linux || darwin

package store_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
)

func beginRequestExample() (*store.Store, auth.Actor, store.RequestDomain, func(error)) {
	base, err := os.MkdirTemp("", "artifact-request-example-")
	if err != nil {
		panic(err)
	}
	dir, companion := filepath.Join(base, "state"), filepath.Join(base, "artifacts")
	if err := os.Mkdir(companion, 0700); err != nil {
		panic(err)
	}
	if err := server.InitializeArtifacts(dir, companion); err != nil {
		panic(err)
	}
	p := store.DefaultRetentionPolicy()
	p.LocalOutputEnabled = false
	info, err := store.EnableRetention(dir, p)
	if err != nil {
		panic(err)
	}
	s, err := store.Open(dir)
	if err != nil {
		panic(err)
	}
	actor, err := auth.NewLegacy("").Authenticate("")
	if err != nil {
		panic(err)
	}
	return s, actor, info.Domain, func(operationErr error) {
		if err := errors.Join(operationErr, s.Close()); err != nil {
			panic(err)
		}
		if err := os.RemoveAll(base); err != nil {
			panic(err)
		}
	}
}

func ExampleStore_BeginArtifactUploadRequest() {
	s, actor, domain, finish := beginRequestExample()
	sha := strings.Repeat("a", 64)
	first, created, err := s.BeginArtifactUploadRequest(actor, domain, "input", sha, 1024)
	if err != nil {
		finish(err)
		return
	}
	second, again, err := s.BeginArtifactUploadRequest(actor, domain, "input", sha, 1024)
	finish(err)
	// Store only reserves metadata here; it attempts no part creation.
	fmt.Println(created, again, first.Reference == second.Reference)
	// Output: true false true
}

func ExampleStore_LookupArtifactUploadRequest() {
	s, actor, domain, finish := beginRequestExample()
	sha := strings.Repeat("a", 64)
	first, _, err := s.BeginArtifactUploadRequest(actor, domain, "input", sha, 1024)
	if err != nil {
		finish(err)
		return
	}
	got, found, err := s.LookupArtifactUploadRequest(actor, domain, "input", sha, 1024)
	finish(err)
	fmt.Println(found, got.Reference == first.Reference)
	// Output: true true
}

func ExampleStore_RetireArtifactUploadRequests() {
	s, _, _, finish := beginRequestExample()
	retired, err := s.RetireArtifactUploadRequests()
	finish(err)
	fmt.Println(retired)
	// Output: 0
}
