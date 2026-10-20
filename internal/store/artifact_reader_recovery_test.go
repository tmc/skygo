//go:build linux || darwin

package store

import (
	"errors"
	"sort"
	"strings"
	"testing"
	"time"
)

// These are Store transition tests, not native cessation tests. The existing
// companion fixture installs and uploads through public APIs using actual Files
// receipts. No reader FD is opened here. Positive settlement calls explicitly
// model the trusted coordinator's stopped/nontransfer precondition; producer
// fields below are identity vectors, never fabricated native death receipts.
func readerRecoveryStore(t *testing.T) (*Store, ArtifactRef, time.Time) {
	t.Helper()
	f := newCompanionFixture(t, true)
	s, err := Open(f.data)
	if err != nil {
		t.Fatal(err)
	}
	f.restored = s // Existing exact-owner custody also retains a failed Close.
	t.Cleanup(func() {
		if err := f.closeRestored(); err != nil {
			t.Errorf("close recovery store: %v", err)
		}
	})
	now := time.Now().UTC()
	s.now = func() time.Time { return now }
	return s, f.ref, now
}

func readerRecoveryProducer() OriginalServerLifetime {
	return OriginalServerLifetime{Host: strings.Repeat("a", 32), Boot: strings.Repeat("b", 32), NamespaceDevice: 7, NamespaceInode: 8, PID: 9, StartTicks: 10}
}

func readerRecoveryLease(t *testing.T, s *Store, ref ArtifactRef, producer OriginalServerLifetime) ArtifactDownloadClosure {
	t.Helper()
	l, err := s.BeginArtifactDownload(testActor(), ref, producer)
	if err != nil {
		t.Fatal(err)
	}
	return ArtifactDownloadClosure{Owner: testActor().Principal(), LeaseID: l.ID, Revision: l.Revision, Ref: l.Reference, File: l.File, RootID: l.RootID, Producer: l.Producer}
}

func TestArtifactReaderRecoverySelection(t *testing.T) {
	s, ref, now := readerRecoveryStore(t)
	if _, found, err := s.NextArtifactReaderRecovery(""); err != nil || found {
		t.Fatalf("empty selection: %v, %v", found, err)
	}
	unknown := readerRecoveryLease(t, s, ref, OriginalServerLifetime{})
	known := readerRecoveryLease(t, s, ref, readerRecoveryProducer())
	want := []ArtifactDownloadClosure{unknown, known}
	sort.Slice(want, func(i, j int) bool { return want[i].LeaseID < want[j].LeaseID })
	// Expiration does not hide an unclosed reader or qualify stopped recovery.
	s.now = func() time.Time { return now.Add(10 * time.Minute) }
	after := ""
	for _, expected := range append(want, want[0]) {
		got, found, err := s.NextArtifactReaderRecovery(after)
		if err != nil || !found || got != expected {
			t.Fatalf("selection after %q: %+v, %v, %v; want %+v", after, got, found, err, expected)
		}
		after = got.LeaseID
	}
	if err := s.SettleStoppedArtifactDownload(unknown); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("unknown producer settled: %v", err)
	}
	for _, cursor := range []string{"x", strings.Repeat("A", 32), strings.Repeat("a", 31)} {
		if _, found, err := s.NextArtifactReaderRecovery(cursor); found || !errors.Is(err, ErrArtifactConflict) {
			t.Fatalf("invalid cursor %q: %v, %v", cursor, found, err)
		}
	}
	// MODELED trusted stopped/nontransfer precondition, not a native observation.
	if err := s.SettleStoppedArtifactDownload(known); err != nil {
		t.Fatal(err)
	}
	if got, found, err := s.NextArtifactReaderRecovery(unknown.LeaseID); err != nil || !found || got != unknown {
		t.Fatalf("closed reader selected: %+v, %v, %v", got, found, err)
	}
}

func TestArtifactReaderRecoveryExactTupleAndCapacity(t *testing.T) {
	s, ref, _ := readerRecoveryStore(t)
	var expected ArtifactDownloadClosure
	for i := 0; i < 32; i++ {
		expected = readerRecoveryLease(t, s, ref, readerRecoveryProducer())
	}
	full := func(t *testing.T) {
		t.Helper()
		if _, err := s.BeginArtifactDownload(testActor(), ref, readerRecoveryProducer()); err == nil {
			t.Fatal("reader charge released without settlement")
		} else {
			var capacity *ArtifactCapacityError
			if !errors.As(err, &capacity) || capacity.Domain != "downloads" {
				t.Fatalf("capacity refusal: %v", err)
			}
		}
	}
	full(t)
	tests := []struct {
		name string
		edit func(*ArtifactDownloadClosure)
	}{
		{"owner", func(c *ArtifactDownloadClosure) { c.Owner += "-other" }},
		{"lease", func(c *ArtifactDownloadClosure) { c.LeaseID = strings.Repeat("f", 32) }},
		{"root", func(c *ArtifactDownloadClosure) { c.RootID = strings.Repeat("f", 32) }},
		{"ref-id", func(c *ArtifactDownloadClosure) { c.Ref.ID = strings.Repeat("f", 32) }},
		{"ref-digest", func(c *ArtifactDownloadClosure) { c.Ref.SHA256 = strings.Repeat("f", 64) }},
		{"ref-size", func(c *ArtifactDownloadClosure) { c.Ref.Size++ }},
		{"file-device", func(c *ArtifactDownloadClosure) { c.File.Device++ }},
		{"file-inode", func(c *ArtifactDownloadClosure) { c.File.Inode++ }},
		{"producer", func(c *ArtifactDownloadClosure) { c.Producer.PID++ }},
		{"unknown", func(c *ArtifactDownloadClosure) { c.Producer = OriginalServerLifetime{} }},
		{"namespace-device", func(c *ArtifactDownloadClosure) { c.Producer.NamespaceDevice = 0 }},
		{"start-ticks", func(c *ArtifactDownloadClosure) { c.Producer.StartTicks = 0 }},
		{"revision-zero", func(c *ArtifactDownloadClosure) { c.Revision = 0 }},
		{"revision-ahead", func(c *ArtifactDownloadClosure) { c.Revision++ }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := expected
			tt.edit(&c)
			if err := s.SettleStoppedArtifactDownload(c); !errors.Is(err, ErrArtifactConflict) {
				t.Fatalf("altered tuple settled: %v", err)
			}
			full(t)
		})
	}
	// MODELED trusted stopped/nontransfer precondition. One exact settlement
	// releases one slot; replay cannot release a second.
	if err := s.SettleStoppedArtifactDownload(expected); err != nil {
		t.Fatal(err)
	}
	readerRecoveryLease(t, s, ref, readerRecoveryProducer())
	for _, revision := range []uint64{expected.Revision, expected.Revision + 1} {
		replay := expected
		replay.Revision = revision
		if err := s.SettleStoppedArtifactDownload(replay); err != nil {
			t.Fatal(err)
		}
		full(t)
	}
}

func TestArtifactReaderRecoveryClosingAndCompatibleReplay(t *testing.T) {
	s, ref, now := readerRecoveryStore(t)
	before := readerRecoveryLease(t, s, ref, readerRecoveryProducer())
	closing, err := s.CloseArtifactDownload(testActor(), ArtifactCursor{before.LeaseID, before.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if closing.Revision != before.Revision+1 || !closing.Closing {
		t.Fatalf("closing cursor: %+v", closing)
	}
	if err := s.SettleStoppedArtifactDownload(before); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("stale unclosed revision accepted: %v", err)
	}
	expected, found, err := s.NextArtifactReaderRecovery("")
	if err != nil || !found || expected.Revision != closing.Revision {
		t.Fatalf("closing selection: %+v, %v, %v", expected, found, err)
	}
	s.now = func() time.Time { return time.Time{} }
	if err := s.SettleStoppedArtifactDownload(expected); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("zero settlement time: %v", err)
	}
	got, found, err := s.NextArtifactReaderRecovery("")
	if err != nil || !found || got != expected {
		t.Fatalf("failed transition changed tuple: %+v, %v, %v", got, found, err)
	}
	s.now = func() time.Time { return now.Add(25 * time.Hour) }
	// MODELED trusted stopped/nontransfer precondition, not old Reader.Close.
	if err := s.SettleStoppedArtifactDownload(expected); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.NextArtifactReaderRecovery(""); err != nil || found {
		t.Fatalf("settled reader remains selectable: %v, %v", found, err)
	}
	s.now = func() time.Time { return now.Add(25*time.Hour + 10*time.Minute) }
	for _, c := range []ArtifactDownloadClosure{expected, before} {
		if err := s.SettleStoppedArtifactDownload(c); err != nil {
			t.Fatalf("compatible closed replay: %v", err)
		}
	}
	stale := expected
	stale.Revision += 2
	if err := s.SettleStoppedArtifactDownload(stale); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("incompatible closed replay: %v", err)
	}
	// Settlement after the original 24-hour floor starts a new hour of grace.
	// Replay ten minutes later must not extend that hour.
	s.now = func() time.Time { return now.Add(25*time.Hour + 61*time.Minute) }
	if _, err := s.RemoveArtifact(testActor(), ref); err != nil {
		t.Fatalf("replay extended grace or retained reader: %v", err)
	}
}

func TestArtifactReaderRecoveryStrictProducerRefuses(t *testing.T) {
	for _, field := range []string{"namespace-device", "start-ticks"} {
		t.Run(field, func(t *testing.T) {
			s, ref, _ := readerRecoveryStore(t)
			p := readerRecoveryProducer()
			if field == "namespace-device" {
				p.NamespaceDevice = 0
			} else {
				p.StartTicks = 0
			}
			expected := readerRecoveryLease(t, s, ref, p)
			if err := s.SettleStoppedArtifactDownload(expected); !errors.Is(err, ErrArtifactConflict) {
				t.Fatalf("non-strict persisted producer settled: %v", err)
			}
			if got, found, err := s.NextArtifactReaderRecovery(""); err != nil || !found || got != expected {
				t.Fatalf("refused reader changed: %+v, %v, %v", got, found, err)
			}
		})
	}
}
