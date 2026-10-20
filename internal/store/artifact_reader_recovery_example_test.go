//go:build linux || darwin

package store

import (
	"errors"
	"fmt"
	"os"
)

// A dormant installation offers no recovery subject or stopped authority.
// This example performs real Store calls, but no native process observation.
func ExampleStore_NextArtifactReaderRecovery() {
	base, err := companionExampleBase()
	if err != nil {
		panic(err)
	}
	f := &companionExampleFrame{base: base}
	s, err := Open(base)
	if err != nil {
		f.fail(err)
	}
	f.store = s
	_, found, queryErr := s.NextArtifactReaderRecovery("")
	if err := f.closeStore(); err != nil {
		f.fail(errors.Join(queryErr, err))
	}
	if found || !errors.Is(queryErr, ErrArtifactsDisabled) {
		f.fail(errors.Join(queryErr, fmt.Errorf("unexpected reader selection: found %v", found)))
	}
	if err := os.RemoveAll(base); err != nil {
		f.fail(err)
	}
	fmt.Println(found, errors.Is(queryErr, ErrArtifactsDisabled))
	// Output: false true
}

// Settlement cannot initialize artifacts or invent stopped authority. A real
// positive call belongs to the qualified concrete coordinator after its native
// observation and deployment premise; there is no dummy positive receipt here.
func ExampleStore_SettleStoppedArtifactDownload() {
	base, err := companionExampleBase()
	if err != nil {
		panic(err)
	}
	f := &companionExampleFrame{base: base}
	s, err := Open(base)
	if err != nil {
		f.fail(err)
	}
	f.store = s
	queryErr := s.SettleStoppedArtifactDownload(ArtifactDownloadClosure{})
	if err := f.closeStore(); err != nil {
		f.fail(errors.Join(queryErr, err))
	}
	if !errors.Is(queryErr, ErrArtifactsDisabled) {
		f.fail(errors.Join(queryErr, errors.New("unexpected reader settlement")))
	}
	if err := os.RemoveAll(base); err != nil {
		f.fail(err)
	}
	fmt.Println(errors.Is(queryErr, ErrArtifactsDisabled))
	// Output: true
}
