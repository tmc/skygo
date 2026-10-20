package control

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// These are MODELED identity/ownership vectors, never native cessation or FD
// receipts. The real old-server SIGKILL/Wait/new-server test supplies that oracle.
func artifactRecoveryVector() (*Control, store.ArtifactDownloadClosure) {
	c := artifactCallControl()
	c.recoverStoppedReaders = true
	c.producer = store.OriginalServerLifetime{Host: strings.Repeat("1", 32), Boot: strings.Repeat("2", 32), NamespaceDevice: 7, NamespaceInode: 8, PID: 9, StartTicks: 10}
	c.artifacts.root = artifact.RootFacts{ID: strings.Repeat("a", 32), Volume: 7}
	p := c.producer
	p.PID++
	return c, store.ArtifactDownloadClosure{Owner: "legacy", LeaseID: strings.Repeat("b", 32), Revision: 1,
		Ref:  store.ArtifactRef{ID: strings.Repeat("c", 32), SHA256: strings.Repeat("d", 64), Size: 1024},
		File: store.ArtifactFile{Device: 7, Inode: 11}, RootID: c.artifacts.root.ID, Producer: p}
}

func artifactRecoveryRequest(t *testing.T, c *Control, w store.ArtifactDownloadClosure) *maintenanceRequest {
	t.Helper()
	owner, err := c.tryArtifactReaderRecovery(w)
	if err != nil {
		t.Fatal(err)
	}
	return &maintenanceRequest{sequence: 1, readerRecovery: &artifactReaderRecoveryRequest{control: c,
		current: workspaceLocalProducer(c.producer), closure: w, owner: owner}}
}

func TestArtifactReaderRecoveryAuthorityAndOriginalMirrors(t *testing.T) {
	for _, kind := range []string{"default off", "zero", "zero device", "zero ticks", "host", "boot", "namespace", "current", "file", "root"} {
		t.Run(kind, func(t *testing.T) {
			c, w := artifactRecoveryVector()
			switch kind {
			case "default off":
				c.recoverStoppedReaders = false
			case "zero":
				w.Producer = store.OriginalServerLifetime{}
			case "zero device":
				w.Producer.NamespaceDevice = 0
			case "zero ticks":
				w.Producer.StartTicks = 0
			case "host":
				w.Producer.Host = strings.Repeat("3", 32)
			case "boot":
				w.Producer.Boot = strings.Repeat("3", 32)
			case "namespace":
				w.Producer.NamespaceInode++
			case "current":
				w.Producer = c.producer
			case "file":
				w.File.Device++
			case "root":
				w.RootID = strings.Repeat("f", 32)
			}
			if owner, err := c.tryArtifactReaderRecovery(w); owner != nil || !errors.Is(err, ErrInvalid) || len(c.artifacts.workspaceIO) != 0 {
				t.Fatal("unqualified identity admitted an observation owner")
			}
		})
	}
	for _, kind := range []string{"same lease", "reserved", "uncertain"} {
		t.Run(kind, func(t *testing.T) {
			c, w := artifactRecoveryVector()
			d := &ArtifactDownload{control: c, ref: artifact.Ref{ID: w.Ref.ID}}
			if kind == "same lease" {
				d.lease.ID = w.LeaseID
			}
			if kind == "uncertain" {
				d.lease.ID = strings.Repeat("e", 32)
				d.uncertain = true
			}
			c.artifacts.downloads[d] = struct{}{}
			if owner, err := c.tryArtifactReaderRecovery(w); owner != nil || !errors.Is(err, ErrArtifactBusy) {
				t.Fatal("cached original was replaced by a new observation")
			}
		})
	}
}

func TestArtifactReaderRecoveryCommonFourStopAndUnion(t *testing.T) {
	c, w := artifactRecoveryVector()
	var calls []*ArtifactCall
	for i := 0; i < 4; i++ {
		call, err := c.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, call)
	}
	if owner, err := c.tryArtifactReaderRecovery(w); owner != nil || !errors.Is(err, ErrArtifactBusy) {
		t.Fatal("observation exceeded actual common-four ownership")
	}
	for _, call := range calls {
		call.Done()
	}
	r := artifactRecoveryRequest(t, c, w)
	if !artifactReaderRecoveryShape(*r) || artifactDownloadCloseShape(*r) || artifactDeleteShape(*r) || workspaceRetirementShape(*r) || physicalRequestShape(*r) {
		t.Fatal("reader recovery escaped its exclusive worker union arm")
	}
	r.readerRecovery.owner.finish(nil) // no native call was made in this policy vector.
	c.StopArtifactAdmission()
	if owner, err := c.tryArtifactReaderRecovery(w); owner != nil || !errors.Is(err, ErrArtifactBusy) {
		t.Fatal("terminal Stop admitted a new observation")
	}
	m := &maintenanceState{input: make(chan maintenanceRequest, 1)}
	if admitted, err := c.dispatchArtifactExpiry(m, time.Now()); admitted || err != nil || m.pending != nil {
		t.Fatal("stopped expiry family dispatched recovery")
	}
	if maintenanceClassCount != 5 {
		t.Fatal("recovery added an outer maintenance class")
	}
}

func TestArtifactReaderRecoveryCanceledBeforeQueryRetainsOriginalPair(t *testing.T) {
	c, w := artifactRecoveryVector()
	request := artifactRecoveryRequest(t, c, w)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out, err := runArtifactReaderRecovery(ctx, *request)
	if !errors.Is(err, context.Canceled) || out == nil || out.invoked || out.stopped || len(c.artifacts.workspaceIO) != 0 {
		t.Fatal("cancellation invoked a native query or retained an unused permit")
	}
	result := maintenanceResult{request: *request, readerRecovery: out, err: err}
	m := &maintenanceState{pending: request}
	if err := c.receiveMaintenance(ctx, m, result); !errors.Is(err, context.Canceled) || m.pending != request || m.received == nil || m.received.request != request || m.received.result.readerRecovery != out {
		t.Fatal("receive-before-cancel lost the original accepted pair")
	}
	// A separate matching no-call integration has no native or Store receipt.
	if err := c.integrateMaintenance(m, result); err != nil || m.pending != nil {
		t.Fatal("original no-call result did not settle only its unused claim")
	}
}

func TestArtifactReaderRecoveryRejectsForeignResult(t *testing.T) {
	for _, kind := range []string{"serial", "request", "missing", "mixed", "uninvoked"} {
		t.Run(kind, func(t *testing.T) {
			c, w := artifactRecoveryVector()
			request := artifactRecoveryRequest(t, c, w)
			result := maintenanceResult{request: *request, readerRecovery: &artifactReaderRecoveryResult{invoked: true, stopped: true}}
			switch kind {
			case "serial":
				result.request.sequence++
			case "request":
				copy := *request.readerRecovery
				result.request.readerRecovery = &copy
			case "missing":
				result.readerRecovery = nil
			case "mixed":
				result.deletion = &artifactDeleteResult{}
			case "uninvoked":
				result.readerRecovery.invoked = false
			}
			pair := &receivedMaintenance{request: request, result: result}
			m := &maintenanceState{pending: request, received: pair}
			if err := c.integrateMaintenance(m, result); err == nil || m.pending != request || m.received != pair || request.readerRecovery.owner.finished {
				t.Fatal("foreign/unproved result lost original pair or released permit")
			}
			request.readerRecovery.owner.finish(nil) // policy-only owner, no native query.
		})
	}
}

func TestArtifactReaderRecoveryCloseErrorCustody(t *testing.T) {
	// Zero typed errors are MODELED classification tokens only: they contain
	// no File and must not be formatted or claimed as actual Close failures.
	for _, typed := range []error{new(local.ServerStoppedCloseError), new(local.ServerLifetimeCloseError)} {
		c, w := artifactRecoveryVector()
		request := artifactRecoveryRequest(t, c, w)
		cause := errors.New("modeled original observation cause")
		err := errors.Join(typed, cause)
		request.readerRecovery.owner.finish(err)
		result := maintenanceResult{request: *request, readerRecovery: &artifactReaderRecoveryResult{invoked: true}, err: err}
		pair := &receivedMaintenance{request: request, result: result}
		m := &maintenanceState{pending: request, received: pair}
		if got := c.integrateMaintenance(m, result); got != err || m.pending != request || m.received != pair {
			t.Fatal("typed uncertainty lost original received request/result/cause")
		}
		owner := request.readerRecovery.owner
		if _, held := c.artifacts.workspaceIO[owner]; !held || !owner.finished || owner.fileError != err || !errors.Is(owner.fileError, cause) {
			t.Fatal("query/capture uncertainty released its original common-four slot")
		}
	}
}

func TestArtifactReaderRecoveryStoreErrorKeepsPositivePairAndPermit(t *testing.T) {
	c, w := artifactRecoveryVector()
	request := artifactRecoveryRequest(t, c, w)
	m := &maintenanceState{pending: request}
	artifactExpiryPolicyStore(t, c, m) // real disabled Store, no raw DB or Files operation.
	// MODELED positive native precondition ONLY to exercise the Store-error
	// boundary; this is not evidence of kernel cessation or descriptor closure.
	out := &artifactReaderRecoveryResult{invoked: true, stopped: true}
	result := maintenanceResult{request: *request, readerRecovery: out}
	pair := &receivedMaintenance{request: request, result: result}
	m.received = pair
	if err := c.integrateMaintenance(m, result); !errors.Is(err, store.ErrArtifactsDisabled) || m.pending != request || m.received != pair || request.readerRecovery.owner.finished {
		t.Fatal("Store error lost original positive pair or its common-four permit")
	}
	if _, present := c.artifacts.workspaceIO[request.readerRecovery.owner]; !present {
		t.Fatal("positive pair lost original ownership before durable settlement")
	}
}

func TestArtifactReaderRecoveryConfigRejectsBeforeTargetUse(t *testing.T) {
	c, _ := artifactRecoveryVector()
	m := &maintenanceState{}
	s := artifactExpiryPolicyStore(t, c, m)
	for _, config := range []Config{
		{RecoverStoppedArtifactReaders: true},
		{RecoverStoppedArtifactReaders: true, Artifacts: new(artifact.Files)},
	} {
		// No fake backend: this deliberately incomplete target cannot be used.
		// Recovery config must reject before target validation or any worker.
		got, err := NewWithConfig(s, []backend.Target{{Name: "unused"}}, auth.NewLegacy(""), config)
		if got != nil || err == nil || !strings.Contains(err.Error(), "stopped artifact reader recovery requires") {
			t.Fatal("unqualified recovery configuration reached targets or workers")
		}
	}
}

func TestArtifactReaderRecoveryPositiveReceiveBeforeCancel(t *testing.T) {
	c, w := artifactRecoveryVector()
	request := artifactRecoveryRequest(t, c, w)
	// MODELED returned observation isolates F1 ordering; no kernel/Store receipt.
	out := &artifactReaderRecoveryResult{invoked: true, stopped: true}
	result := maintenanceResult{request: *request, readerRecovery: out}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m := &maintenanceState{pending: request}
	// Store is intentionally nil: cancellation must retain before any CAS.
	if err := c.receiveMaintenance(ctx, m, result); !errors.Is(err, context.Canceled) || m.pending != request || m.received == nil || m.received.result.readerRecovery != out || request.readerRecovery.owner.finished {
		t.Fatal("canceled receive integrated or discarded original positive result")
	}
	if _, held := c.artifacts.workspaceIO[request.readerRecovery.owner]; !held {
		t.Fatal("canceled positive result released original common-four slot")
	}
	request.readerRecovery.owner.finish(nil) // policy vector had no actual query/FD.
}

func TestArtifactReaderRecoveryRejectsMixedOriginalCloseRequest(t *testing.T) {
	c, w := artifactRecoveryVector()
	request := artifactRecoveryRequest(t, c, w)
	closeControl, _, frame := artifactExpiryVector()
	closeOwner, err := closeControl.tryArtifactIO()
	if err != nil {
		t.Fatal(err)
	}
	originalClose := maintenanceRequest{sequence: request.sequence, downloadClose: frame, closeOwner: closeOwner}
	if !artifactDownloadCloseShape(originalClose) {
		t.Fatal("original close fixture is not independently valid")
	}
	mixed := originalClose
	mixed.readerRecovery = request.readerRecovery
	if artifactReaderRecoveryShape(mixed) || artifactDownloadCloseShape(mixed) {
		t.Fatal("two independently valid request arms were admitted together")
	}
	request.readerRecovery.owner.finish(nil)
	closeOwner.finish(nil) // neither modeled owner invoked a physical operation.
}
