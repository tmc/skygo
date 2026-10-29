package store

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/workeradmission"
	bolt "go.etcd.io/bbolt"
)

// These are MODELED native facts. Encoding uses the production validators and
// encoding/json, including its six-byte HTML escape for every opaque UID byte.
func maximalWorkerBarrierRecords() (workerGroupRecord, workerMemberRecord) {
	end := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	gen := strings.Repeat("f", 32)
	effect := func(seq uint64) workerEffectRecord {
		return workerEffectRecord{State: WorkerUnknown, Seq: seq, Gen: gen, At: end, Error: strings.Repeat("e", workerErrorLimit), Unsettled: true}
	}
	opaque := strings.Repeat("<", 128)
	dns := strings.Repeat("n", 63) + "." + strings.Repeat("n", 63) + "." + strings.Repeat("n", 63) + "." + strings.Repeat("n", 61)
	code := int32(255)
	m := workerMemberRecord{Job: strings.Repeat("f", 32), Rank: 0, Counter: math.MaxUint64, Create: effect(math.MaxUint64 - 5), Release: effect(math.MaxUint64 - 4), Cancel: effect(math.MaxUint64 - 3), Remove: effect(math.MaxUint64 - 2), Pod: workerPodRecord{Name: strings.Repeat("p", 63), UID: opaque}, Observed: &workerObservedRecord{State: backend.Failed, Container: opaque, Started: end, ExitCode: &code, Finished: end, Node: dns, At: end}}
	m.Create.State = WorkerRecorded
	m.Barrier = &workerBarrierMemberRecord{Version: 1, Service: workerPodRecord{Name: strings.Repeat("s", 63), UID: opaque}, Start: effect(math.MaxUint64 - 1), Recover: effect(math.MaxUint64), StartToken: strings.Repeat("f", 64), StartRevision: strings.Repeat("r", workerRevisionLimit), Observation: &workerBarrierObservationRecord{Revision: strings.Repeat("r", workerRevisionLimit), Node: dns, Started: true, Deleting: true, RendezvousHost: dns, RendezvousPort: 29500, Routed: true, At: end}, Stop: backend.WorkerTerminated, StopAt: end, RemoveHadPod: true, RecoverHadPod: true, PodRemoveSent: true, ServiceRemoveSent: true, Vanished: true, LocalUnsettled: true}
	// A revision is present only on a positive Start acknowledgement. The unknown
	// variant carries a maximal error instead, which has the larger encoding.
	m.Barrier.StartRevision = ""
	spec := backend.WorkerSpec{Count: backend.MaxWorkers, Mode: backend.WorkerIndependent}
	principal := "p" + strings.Repeat("9", 63)
	var digest [32]byte
	for i := range digest {
		digest[i] = 255
	}
	g := workerGroupRecord{Job: m.Job, Principal: principal, Domain: RequestDomain{Lineage: strings.Repeat("f", 32), Epoch: math.MaxUint64}, RequestID: strings.Repeat("R", 128), Authority: auth.Delegation{Principal: principal, TokenID: strings.Repeat("f", 32), Digest: digest}, TaskDigest: strings.Repeat("f", 64), Spec: spec, SpecDigest: workerSpecDigest(spec), Demand: workeradmission.Demand{Ranks: backend.MaxWorkers, CPUMillis: workerDemandLimit.CPUMillis / backend.MaxWorkers, MemoryBytes: workerDemandLimit.MemoryBytes / backend.MaxWorkers, Accelerators: math.MaxUint32}, Phase: WorkerGroupDone, Revoked: true, Revision: math.MaxUint64, CreatedAt: end, Barrier: &workerBarrierGroupRecord{Version: 1, Decision: WorkerBarrierAbort, DecidedAt: end}}
	return g, m
}

func TestWorkerBarrierRecordBounds(t *testing.T) {
	g, m := maximalWorkerBarrierRecords()
	gr, err := encodeWorkerGroupRecord(g)
	if err != nil {
		t.Fatal(err)
	}
	mr, err := encodeWorkerMemberRecord(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeWorkerGroupRecord(gr); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeWorkerMemberRecord(mr); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(mr, []byte(`\u003c`)) {
		t.Fatal("opaque UIDs did not exercise HTML escaping")
	}
	// Exercise the alternative positive acknowledgement image as well: wider
	// revisions, empty errors, and positive retirement flags.
	ack := m
	b := *m.Barrier
	ack.Barrier = &b
	b.Start.State, b.Start.Error, b.StartAck, b.StartRevision = WorkerRecorded, "", true, strings.Repeat("r", workerRevisionLimit)
	b.Recover.State, b.Recover.Error = WorkerRecorded, ""
	b.ServiceRemoved = true
	for _, e := range ack.effects() {
		e.Unsettled = false
	}
	ack.Create.Error = ""
	ack.Release.State, ack.Release.Error, ack.ReleaseAck, ack.Revision = WorkerRecorded, "", true, strings.Repeat("r", workerRevisionLimit)
	ack.Cancel.State, ack.Cancel.Error, ack.CancelAck = WorkerRecorded, "", true
	ack.Remove.State, ack.Remove.Error, ack.Removed = WorkerUnknown, strings.Repeat("e", workerErrorLimit), true
	ar, err := encodeWorkerMemberRecord(ack)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeWorkerMemberRecord(ar); err != nil {
		t.Fatal(err)
	}
	// Resolve is mutually exclusive with a completed Create. Cover its maximal
	// uncertain outcome separately, preserving that original unresolved state.
	resolve := m
	b2 := *m.Barrier
	resolve.Barrier = &b2
	resolve.Create.State = WorkerUnknown
	resolve.Release = workerEffectRecord{}
	resolve.ReleaseAck = false
	b2.Resolve = workerResolveRecord{Class: WorkerResolveMismatch, Refused: true, At: g.CreatedAt, Unsettled: true}
	rr, err := encodeWorkerMemberRecord(resolve)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeWorkerMemberRecord(rr); err != nil {
		t.Fatal(err)
	}
	t.Logf("actual JSON bytes: group=%d unknown=%d ack=%d resolve=%d; row caps=%d/%d", len(gr), len(mr), len(ar), len(rr), workerBarrierGroupLimit, workerBarrierMemberLimit)
	if workerRecordReserve(g) != 135778 {
		t.Fatalf("16-rank reserve=%d", workerRecordReserve(g))
	}
	// Even maximum worker row bounds (old+new) plus a maximum task-bearing job
	// on both sides leave room for actual bounded request/quota/retained writes.
	groupKeyBytes, memberKeyBytes := len(workerGroupKey(g.Job)), len(workerMemberKey(g.Job, 0))
	if groupKeyBytes != 34 || memberKeyBytes != 36 {
		t.Fatalf("serialized keys = %d/%d", groupKeyBytes, memberKeyBytes)
	}
	two := g
	two.Spec.Count = 2
	if workerRecordReserve(two) != 20586 {
		t.Fatalf("2-rank reserve = %d", workerRecordReserve(two))
	}
	rows := uint64(2*(workerBarrierGroupLimit+groupKeyBytes) + 2*backend.MaxWorkers*(workerBarrierMemberLimit+memberKeyBytes))
	if rows != 271556 {
		t.Fatalf("worker row mutation bytes=%d", rows)
	}
	if rows+2*(256<<10)+64<<10 > maxChangeBytes {
		t.Fatal("maximal rows leave no bounded metadata headroom")
	}
}

func TestWorkerBarrierLegacyEncoding(t *testing.T) {
	g, m := maximalWorkerBarrierRecords()
	g.Barrier = nil
	g.Spec.Mode = backend.WorkerIndependent
	g.SpecDigest = workerSpecDigest(g.Spec)
	for _, e := range m.effects() {
		e.Unsettled = false
	}
	m.Barrier = nil
	m.Counter = math.MaxUint64 - 2
	raw, err := encodeWorkerGroupRecord(g)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, direct) || bytes.Contains(raw, []byte(`"barrier"`)) {
		t.Fatal("legacy group encoding changed")
	}
	raw, err = encodeWorkerMemberRecord(m)
	if err != nil {
		t.Fatal(err)
	}
	direct, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, direct) || bytes.Contains(raw, []byte(`"barrier"`)) {
		t.Fatal("legacy member encoding changed")
	}
	if workerRecordReserve(g) != workerGroupReserve(g.Spec.Count) {
		t.Fatal("legacy charge changed")
	}
}

func TestWorkerBarrierCodecRefusals(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*workerMemberRecord)
	}{
		{"unknown version", func(m *workerMemberRecord) { m.Barrier.Version = 2 }},
		{"unbounded UID", func(m *workerMemberRecord) { m.Barrier.Service.UID = strings.Repeat("x", 129) }},
		{"dispatch without intent", func(m *workerMemberRecord) { m.Remove = workerEffectRecord{} }},
		{"recover without original dispatch", func(m *workerMemberRecord) { m.Barrier.PodRemoveSent = false }},
		{"lost original recover reference", func(m *workerMemberRecord) { m.Barrier.RecoverHadPod = false }},
		{"lost original pod reference", func(m *workerMemberRecord) { m.Barrier.RemoveHadPod = false }},
		{"bad observation host", func(m *workerMemberRecord) { m.Barrier.Observation.RendezvousHost = "bad/host" }},
		{"effect owner without latch", func(m *workerMemberRecord) { m.Barrier.LocalUnsettled = false }},
		{"effect owner without cause", func(m *workerMemberRecord) { m.Create.Error = "" }},
		{"unknown token", func(m *workerMemberRecord) { m.Barrier.StartToken = "not-a-token" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, m := maximalWorkerBarrierRecords()
			tt.change(&m)
			if _, err := encodeWorkerMemberRecord(m); err == nil {
				t.Fatal("accepted invalid member")
			}
		})
	}
}

// TestWorkerBarrierTransactionHeadroom encodes every non-worker, non-task cell
// acceptance or a later transition may change. The fieldwise maxima deliberately
// include the full uint64 representation of usage, even though current quotas
// allow smaller values. This is an encoding bound, not a populated installation.
func TestWorkerBarrierTransactionHeadroom(t *testing.T) {
	end := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	p := DefaultRetentionPolicy()
	p.Window = 90 * 24 * time.Hour
	p.OutputUnit = 65536
	p.Volume = math.MaxUint64
	if !policyValid(p) {
		t.Fatal("maximal policy is invalid")
	}
	st := retentionState{Enabled: true, Policy: p, Lineage: strings.Repeat("f", 32), Current: math.MaxUint64, RetiredThrough: math.MaxUint64 - 2, Epochs: []requestEpoch{{Number: math.MaxUint64 - 1, AcceptUntil: end.Add(-p.Window), RetryUntil: end}, {Number: math.MaxUint64, AcceptUntil: end.Add(-p.Window), RetryUntil: end}}, LegacyCutoff: end, RequestAfter: strings.Repeat("f", 64), LegacyJobsAfter: math.MaxUint64, LegacyJobsMax: math.MaxUint64, LineageSerial: math.MaxUint64}
	if !validRequestCursor(st.RequestAfter) || !validLegacyJobsCursor(st, math.MaxUint64) {
		t.Fatal("invalid maximal cursors")
	}
	var used retentionUsage
	for i := range used {
		used[i] = math.MaxUint64
	}
	principal := strings.Repeat("p", 64)
	domain := RequestDomain{Lineage: strings.Repeat("f", 32), Epoch: math.MaxUint64}
	request := strings.Repeat("R", 128)
	id := strings.Repeat("f", 32)
	meta := retainedJob{Principal: principal, Domain: domain, RequestID: request, Storage: backend.RemoteOutput, Overhead: jobOverheadReserve, Ordinal: math.MaxUint64}
	req := requestRecord{Principal: principal, Domain: domain, RequestID: request, JobID: id}
	for i := range req.TaskDigest {
		req.TaskDigest[i] = 255
	}
	encode := func(v any) []byte {
		t.Helper()
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	stateRaw, usageRaw := encode(st), encode(used)
	if len(retentionStateKey)+len(stateRaw)+len(retentionUsageKey)+len(usageRaw) > 2048 {
		t.Fatal("scalar fixtures exceed production scalar reservation")
	}
	var other uint64
	cell := func(name string, key, raw []byte) {
		t.Helper()
		n := uint64(len(key) + 2*len(raw))
		other += n
		t.Logf("%s key=%d value=%d old+new+key=%d", name, len(key), len(raw), n)
	}
	cell("retention state", retentionStateKey, stateRaw)
	cell("retention usage", retentionUsageKey, usageRaw)
	cell("retained job", []byte(id), encode(meta))
	cell("request witness", requestKey(principal, domain, request), encode(req))
	cell("scope counter", []byte(principal), encode(uint64(math.MaxUint64)))
	cell("job principal", []byte(id), []byte(principal))
	cell("task counter", taskBytesKey, sequenceKey(math.MaxUint64))
	cell("retry counter", retryBytesKey, sequenceKey(math.MaxUint64))
	cell("acceptance index", sequenceKey(math.MaxUint64), []byte(id))
	cell("principal index", sequenceKey(math.MaxUint64), []byte(id))
	// One newly created principal index name, and before/after sequences for
	// the job, worker and principal indexes. Counting all simultaneously is
	// conservative: a new principal index cannot also have an old sequence.
	other += uint64(len(principal)) + 3*16
	if other >= 32768 {
		t.Fatalf("other changed cells=%d exceed half of 64KiB headroom", other)
	}
	if 2*jobOverheadReserve+other > 64<<10 {
		t.Fatal("job overhead plus other cells exceed progress headroom")
	}
	t.Logf("actual fieldwise other-cell bound=%d; two enforced job-overhead reserves=%d; combined=%d of65536", other, 2*jobOverheadReserve, other+2*jobOverheadReserve)
}

// NeverReleased is an ephemeral recovery attestation requiring its own
// qualified durable inputs. It is never a persisted native Pod stop enum.
func TestWorkerBarrierRejectPersistedNeverReleased(t *testing.T) {
	now := workerT0
	m := workerMemberRecord{Job: strings.Repeat("f", 32), Rank: 0, Counter: 1, Create: workerEffectRecord{State: WorkerRecorded, Seq: 1, Gen: strings.Repeat("f", 32), At: now}, Pod: workerPodRecord{Name: "rank-0", UID: "original-pod"}, Barrier: &workerBarrierMemberRecord{Version: 1, Stop: backend.WorkerNeverReleased, StopAt: now}}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeWorkerMemberRecord(raw); err == nil {
		t.Fatal("canonical row invented native stop from never-released attestation")
	}
	m.Barrier.Stop = backend.WorkerStopUnproven
	m.Barrier.StopAt = time.Time{}
	m.Counter = 3
	m.Remove = workerEffectRecord{State: WorkerUnknown, Seq: 2, Gen: strings.Repeat("f", 32), At: now, Error: "unknown"}
	m.Barrier.RemoveHadPod = true
	m.Barrier.PodRemoveSent = true
	m.Barrier.RecoverHadPod = true
	m.Barrier.Recover = workerEffectRecord{State: WorkerIntended, Seq: 3, Gen: strings.Repeat("f", 32), At: now}
	raw, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeWorkerMemberRecord(raw); err == nil {
		t.Fatal("sent removal plus no Release invented recovery stop")
	}
}

// These negative canonical shapes are deliberately unreachable through the
// public Store API. They exercise historical codec proof independently without
// injecting any fake persistent Store or claiming native dispatch/absence.
func TestWorkerBarrierNoPodRecoveryCodecPrerequisites(t *testing.T) {
	f, g := noPodRecoveryFixture(t, true)
	_, g = barrierIntent(t, f, g, 0, WorkerRecover)
	var original workerMemberRecord
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		w, err := loadWorkerState(tx, g.Job.ID)
		if err == nil {
			original = w.m[0]
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeWorkerMemberRecord(raw); err != nil {
		t.Fatal("actual qualified intent was not readable", err)
	}
	for _, tt := range []struct {
		name   string
		change func(*workerMemberRecord)
	}{
		{"original refusal missing", func(m *workerMemberRecord) { m.Barrier.CreateRefused = false }},
		{"Service UID missing", func(m *workerMemberRecord) { m.Barrier.Service = workerPodRecord{} }},
		{"Service dispatch missing", func(m *workerMemberRecord) { m.Barrier.ServiceRemoveSent = false }},
		{"original Remove had Pod", func(m *workerMemberRecord) { m.Barrier.RemoveHadPod = true }},
		{"original Recover had Pod", func(m *workerMemberRecord) { m.Barrier.RecoverHadPod = true }},
		{"Release history", func(m *workerMemberRecord) {
			m.Counter++
			m.Release = workerEffectRecord{State: WorkerRecorded, Seq: m.Counter, Gen: workerGenString(workerGen()), At: workerNowUTC()}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var m workerMemberRecord
			if err := json.Unmarshal(raw, &m); err != nil {
				t.Fatal(err)
			}
			tt.change(&m)
			if workerBarrierNoPodRecovery(m) {
				t.Fatal("missing historical prerequisite still qualified")
			}
			changed, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeWorkerMemberRecord(changed); err == nil {
				t.Fatal("canonical unqualified recovery accepted")
			}
		})
	}
}
