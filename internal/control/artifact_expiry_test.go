package control

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/store"
)

// MODELED ownership/identity vectors only. No Files method or successful
// Store Finish is executed with these invented inode values. Actual original
// worker closure is covered by store_test's persistent Store/Files fixture.
func artifactExpiryVector() (*Control, *ArtifactDownload, *artifactDownloadClose) {
	c := artifactCallControl()
	c.artifacts.root = artifact.RootFacts{ID: strings.Repeat("a", 32), Volume: 7}
	ref := store.ArtifactRef{ID: strings.Repeat("b", 32), SHA256: strings.Repeat("c", 64), Size: 1024}
	d := &ArtifactDownload{control: c, owner: "legacy", ref: artifact.Ref{ID: ref.ID, SHA256: ref.SHA256, Size: int64(ref.Size)},
		lease: store.ArtifactLease{ID: strings.Repeat("d", 32), Reference: ref, Revision: 2, ExpiresAt: time.Now().Add(-time.Minute), Closing: true,
			File: store.ArtifactFile{Device: 7, Inode: 9}, RootID: c.artifacts.root.ID}, closing: true, settling: true}
	c.artifacts.downloads[d] = struct{}{}
	frame := &artifactDownloadClose{download: d, neverOpened: true,
		closure: store.ArtifactDownloadClosure{Owner: d.owner, LeaseID: d.lease.ID, Revision: d.lease.Revision, Ref: ref, File: d.lease.File, RootID: d.lease.RootID}}
	return c, d, frame
}

func TestArtifactExpirySelectorOriginalBoundaries(t *testing.T) {
	now := time.Now()
	for _, kind := range []string{"zero lease", "zero deadline", "future", "finished", "settling", "uncached"} {
		t.Run(kind, func(t *testing.T) {
			c, d, _ := artifactExpiryVector()
			d.settling = false
			switch kind {
			case "zero lease":
				d.lease.ID = ""
			case "zero deadline":
				d.lease.ExpiresAt = time.Time{}
			case "future":
				d.lease.ExpiresAt = now.Add(time.Second)
			case "finished":
				d.finished = true
			case "settling":
				d.settling = true
			case "uncached":
				delete(c.artifacts.downloads, d)
			}
			if got := c.nextExpiredDownload("", now); got != nil {
				t.Fatal("ineligible identity became expired original", got)
			}
		})
	}
	c, d, _ := artifactExpiryVector()
	d.settling = false
	d.lease.ExpiresAt = now
	second := *d
	second.lease.ID = strings.Repeat("e", 32)
	c.artifacts.downloads[&second] = struct{}{}
	if c.nextExpiredDownload("", now) != d || c.nextExpiredDownload(d.lease.ID, now) != &second || c.nextExpiredDownload(second.lease.ID, now) != d {
		t.Fatal("bounded original selection did not sort and wrap at exact expiry")
	}
	// LIVE zero Producer is deliberate here; it must not eliminate the SAME
	// original cached lease or create authority for any uncached durable row.
	if d.lease.Producer != (store.OriginalServerLifetime{}) {
		t.Fatal("fixture unexpectedly supplies a producer")
	}
}

func TestArtifactExpiryPrepareRefusesCompetingOriginalWithoutStore(t *testing.T) {
	for _, kind := range []string{"settling", "uncertain", "uncached", "finished"} {
		t.Run(kind, func(t *testing.T) {
			c, d, _ := artifactExpiryVector()
			cause := errors.New("modeled original close uncertainty")
			want := ErrArtifactBusy
			switch kind {
			case "uncertain":
				d.settling = false
				d.uncertain = true
				d.closeError = cause
			case "uncached":
				delete(c.artifacts.downloads, d)
				want = ErrInvalid
			case "finished":
				delete(c.artifacts.downloads, d)
				d.finished = true
				want = nil
			}
			frame, err := d.prepareOriginalClose() // nil Store: refusal must precede metadata.
			if frame != nil || !errors.Is(err, want) {
				t.Fatal("competing/unknown original reached Store", frame, err)
			}
			if kind == "uncertain" && !errors.Is(err, cause) {
				t.Fatal("original exact close cause lost", err)
			}
		})
	}
}

func TestArtifactExpiryCancellationRetainsUnintegratedOriginal(t *testing.T) {
	c, d, frame := artifactExpiryVector()
	owner, err := c.tryArtifactIO()
	if err != nil {
		t.Fatal(err)
	}
	request := &maintenanceRequest{sequence: 1, downloadClose: frame, closeOwner: owner}
	if !artifactDownloadCloseShape(*request) || physicalRequestShape(*request) || workspaceRetirementShape(*request) || artifactDeleteShape(*request) {
		t.Fatal("original close request escaped its exclusive union arm")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := runArtifactDownloadClose(ctx, *request)
	if !errors.Is(err, context.Canceled) || result == nil || result.invoked || !owner.finished || len(c.artifacts.workspaceIO) != 0 {
		t.Fatal("cancellation invoked close or lost unused permit", result, err)
	}
	if !d.settling || d.finished || frame.download != d {
		t.Fatal("unintegrated cancellation released original close custody")
	}
	r := maintenanceResult{request: *request, downloadClose: result, err: err}
	pair := &receivedMaintenance{request: request, result: r}
	m := &maintenanceState{pending: request, received: pair}
	if err := c.integrateArtifactDownloadClose(m, r); err != nil || m.pending != nil || d.settling || d.finished {
		t.Fatal("matching unused cancellation gained credit or retained short claim", err)
	}
	if _, present := c.artifacts.downloads[d]; !present {
		t.Fatal("no-call cancellation retired cached original")
	}
}

func TestArtifactExpiryRejectsForeignOrUninvokedResult(t *testing.T) {
	for _, kind := range []string{"serial", "frame", "permit", "missing result", "not invoked", "ordinary error", "close uncertainty", "mixed result"} {
		t.Run(kind, func(t *testing.T) {
			c, d, frame := artifactExpiryVector()
			owner, err := c.tryArtifactIO()
			if err != nil {
				t.Fatal(err)
			}
			request := &maintenanceRequest{sequence: 1, downloadClose: frame, closeOwner: owner}
			r := maintenanceResult{request: *request, downloadClose: &artifactDownloadCloseResult{}}
			switch kind {
			case "serial":
				r.request.sequence++
			case "frame":
				copied := *frame
				r.request.downloadClose = &copied
			case "permit":
				r.request.closeOwner = &artifactIO{service: c.artifacts}
			case "missing result":
				r.downloadClose = nil
			case "ordinary error":
				r.downloadClose.invoked = true
				r.err = errors.New("modeled original close failure")
			case "close uncertainty":
				r.downloadClose.invoked = true
				r.err = artifact.ErrFileCloseUncertain
				d.uncertain = true
				d.closeError = r.err
				frame.err = r.err
			case "mixed result":
				r.deletion = &artifactDeleteResult{}
			}
			owner.finish(r.err)
			pair := &receivedMaintenance{request: request, result: r}
			m := &maintenanceState{pending: request, received: pair}
			if err := c.integrateArtifactDownloadClose(m, r); err == nil || m.pending != request || m.received != pair || !d.settling || d.finished {
				t.Fatal("invalid/error result lost exact original pair or gained credit", err)
			}
			if kind == "close uncertainty" {
				if _, present := c.artifacts.workspaceIO[owner]; !present || owner.fileError != r.err || frame.err != r.err || d.closeError != r.err {
					t.Fatal("typed close uncertainty lost its original permit/frame/cause")
				}
			}
		})
	}
}

func TestArtifactExpiryTerminalStopAndCommonFour(t *testing.T) {
	c, d, _ := artifactExpiryVector()
	d.settling = false
	c.StopArtifactAdmission()
	m := &maintenanceState{input: make(chan maintenanceRequest, 1)}
	if admitted, err := c.dispatchArtifactExpiry(m, time.Now()); err != nil || admitted || m.pending != nil || len(c.artifacts.workspaceIO) != 0 {
		t.Fatal("terminal Stop admitted new expiry ownership", admitted, err)
	}
	c, _, _ = artifactExpiryVector()
	for i := 0; i < 4; i++ {
		if _, err := c.TryArtifactCall(); err != nil {
			t.Fatal(err)
		}
	}
	if owner, err := c.tryArtifactIO(); !errors.Is(err, ErrArtifactBusy) || owner != nil {
		t.Fatal("expiry's shared permit may exceed common four", owner, err)
	}
	if maintenanceClassCount != 5 || maintenanceArtifactExpiryClass >= maintenanceClassCount {
		t.Fatal("expiry changed the finite five-class scheduling bound")
	}
}

func TestArtifactExpiryPredispatchMarkErrorKeepsOriginal(t *testing.T) {
	c, d, _ := artifactExpiryVector()
	d.settling = false
	before := d.lease
	m := &maintenanceState{input: make(chan maintenanceRequest, 1)}
	artifactExpiryPolicyStore(t, c, m)
	// The real Store has no artifact installation. This is an unexpected
	// prepare refusal with MODELED identity, never a native close fixture.
	admitted, err := c.dispatchExpiredDownload(m, time.Now())
	if !errors.Is(err, store.ErrArtifactsDisabled) || admitted || m.pending != nil || len(m.input) != 0 || m.expiryHeld == nil {
		t.Fatal("predispatch Mark refusal escaped original Run custody", admitted, err)
	}
	if m.expiryHeld.download != d || m.expiryHeld.err != err || !d.settling || !d.closing || d.finished || d.lease != before {
		t.Fatal("predispatch failure changed original identity, claim or returned cause")
	}
	if _, present := c.artifacts.downloads[d]; !present || len(c.artifacts.workspaceIO) != 0 {
		t.Fatal("predispatch failure lost original or fabricated a worker permit")
	}
	if admitted, again := c.dispatchArtifactExpiry(m, time.Now()); admitted || again != err || m.expiryHeld.download != d {
		t.Fatal("held original Mark error was silently retried or replaced", admitted, again)
	}
}

// This concrete policy fixture owns only its original Store; there is no Files,
// reader or worker to join. Every acquisition/assertion exit runs one observed
// Close or retains the exact original case, including the acquisition cause.
func artifactExpiryPolicyStore(t *testing.T, c *Control, m *maintenanceState) *store.Store {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261021-expiry-policy")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(parent, "case-")
	if err != nil {
		t.Fatal(err)
	}
	var s *store.Store
	var acquisitionErr error
	// Register the concrete original frame before acquiring Store and before
	// any oracle may terminate the test. No reader/Files owner exists here.
	t.Cleanup(func() {
		cause := acquisitionErr
		if cause == nil && s != nil {
			cause = s.Close() // one observed original Close, including assertion failure
		}
		if cause != nil {
			heldDeletePolicyStores.Lock()
			heldDeletePolicyStores.frames = append(heldDeletePolicyStores.frames, struct {
				store       *store.Store
				control     *Control
				maintenance *maintenanceState
				path        string
				cause       error
			}{s, c, m, dir, cause})
			heldDeletePolicyStores.Unlock()
			t.Errorf("retaining original expiry policy case %s: %v", dir, cause)
			return
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	s, acquisitionErr = store.Open(dir)
	if acquisitionErr != nil {
		t.Fatal(acquisitionErr)
	}
	c.store = s
	return s
}

func TestArtifactExpiryFinishErrorKeepsOriginalReceivedPair(t *testing.T) {
	c, d, frame := artifactExpiryVector()
	m := &maintenanceState{}
	artifactExpiryPolicyStore(t, c, m)
	before := d.lease
	owner, err := c.tryArtifactIO()
	if err != nil {
		t.Fatal(err)
	}
	owner.finish(nil)
	request := &maintenanceRequest{sequence: 1, downloadClose: frame, closeOwner: owner}
	// MODELED positive invoked/nil Close result, NOT a native Reader.Close
	// receipt. A REAL uninstalled Store must refuse the subsequent Finish.
	// This targets the coordinator's Finish-error boundary, not r.err's
	// earlier return. No production failure seam or metadata seed is used.
	result := &artifactDownloadCloseResult{invoked: true}
	r := maintenanceResult{request: *request, downloadClose: result}
	pair := &receivedMaintenance{request: request, result: r}
	m.pending, m.received = request, pair
	err = c.integrateMaintenance(m, r)
	if err != store.ErrArtifactsDisabled {
		t.Fatal("actual Store Finish error changed", err)
	}
	if m.pending != request || m.received != pair || pair.request != request || pair.result.downloadClose != result || pair.result.err != nil ||
		request.downloadClose != frame || frame.download != d || request.closeOwner != owner || !d.settling || !d.closing || d.finished || d.lease != before {
		t.Fatal("Finish refusal lost original positive pair/claim or credited lease")
	}
	if _, present := c.artifacts.downloads[d]; !present || len(c.artifacts.downloads) != 1 || len(c.artifacts.workspaceIO) != 0 || !owner.finished {
		t.Fatal("Finish refusal retired original cache or changed settled permit")
	}
}
