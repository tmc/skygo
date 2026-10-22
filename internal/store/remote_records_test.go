package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

// These MODELED value fixtures retain the C2 profile and apply the ratified
// correction: c stores trusted ReleaseAt, never a worker-derived deadline.
// The earlier encoding oracle is
// 2f932b6ef03f8358128f8e5c9d0260768b2c037a27a685ddb339603d6e96a5e0.
// They exercise the real record codec, not native resource/termination proof.
// Purpose-separated hashes differ by one three-digit byte, preserving C2 width.
// No database rows or physical receipts are installed by these tests.
func remoteTestDigest() (h [32]byte) {
	for i := range h {
		h[i] = 255
	}
	return
}
func remoteTestIdentity(ch byte, prefix string, last byte) string {
	return prefix + strings.Repeat(string(ch), 128-len(prefix)-1) + string(last)
}

// remoteTestLexicalIdentity uses the FULL accepted128-byte printable-ASCII grammar.
// The three suffix bytes distinguish all7 identities without reducing escaping.
func remoteTestLexicalIdentity(ch byte, index int) string {
	alphabet := "<>&"
	if ch == '<' || ch == '>' || ch == '&' {
		alphabet = "<>&"
	} else {
		alphabet = "\"\\"
	}
	suffix := []byte{alphabet[0], alphabet[0], alphabet[0]}
	for i := len(suffix) - 1; i >= 0; i-- {
		suffix[i] = alphabet[index%len(alphabet)]
		index /= len(alphabet)
	}
	return strings.Repeat(string(ch), 125) + string(suffix)
}
func remoteTestRowIdentity(ch byte, index int, lexical bool) string {
	if lexical {
		return remoteTestLexicalIdentity(ch, index)
	}
	prefix := ""
	if index >= 3 {
		prefix = "containerd://"
	}
	return remoteTestIdentity(ch, prefix, byte('a'+index))
}
func remoteTestWhen(year int) time.Time {
	// Nonzero supported calendar endpoint neighborhoods, with9-digit nanos.
	if year == 1 {
		return time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC)
	}
	return time.Date(9999, 12, 20, 0, 0, 0, 1, time.UTC)
}
func remoteTestPointer(t time.Time) *time.Time { return &t }
func remoteTestMaximum(ch byte, year int, lexical bool) remoteRecord {
	t := remoteTestWhen(year)
	stage := &remoteExit{remoteTestRowIdentity(ch, 3, lexical), 0, t.Add(time.Nanosecond), t.Add(time.Second + time.Nanosecond)}
	setup := &remoteExit{remoteTestRowIdentity(ch, 4, lexical), 0, t.Add(2*time.Second + 2*time.Nanosecond), t.Add(3*time.Second + 3*time.Nanosecond)}
	main := remoteExit{remoteTestRowIdentity(ch, 5, lexical), 0, t.Add(4*time.Second + 4*time.Nanosecond), t.Add(60*time.Second + 5*time.Nanosecond)}
	collector := &remoteExit{remoteTestRowIdentity(ch, 6, lexical), 255, main.Started.Add(time.Nanosecond), main.Finished.Add(time.Second + time.Nanosecond)}
	renewed := collector.Finished.Add(time.Nanosecond)
	in := &remoteLease{remoteTestDigest(), math.MaxUint64, 3, t.Add(120 * time.Second), t}
	in.Hash[0] = 254
	out := &remoteLease{remoteTestDigest(), math.MaxUint64, 2, renewed.Add(120 * time.Second), renewed}
	return remoteRecord{
		Job: strings.Repeat("a", 32), Revision: math.MaxUint64, State: 2, Creation: 3,
		Authority: &remoteAuthority{strings.Repeat("a", 64), strings.Repeat("b", 32), remoteTestDigest(), [5]int64{600, 600, 86400, 604800, 120}},
		Resources: remoteResources{&remoteResource{"p" + strings.Repeat("a", 62), remoteTestRowIdentity(ch, 0, lexical)}, &remoteResource{"i" + strings.Repeat("a", 62), remoteTestRowIdentity(ch, 1, lexical)}, &remoteResource{"o" + strings.Repeat("a", 62), remoteTestRowIdentity(ch, 2, lexical)}},
		Active:    &remoteActive{Phase: 5, Budgets: [4]int64{600, 604800, 600, 120}, Began: t, StageDeadline: t.Add(600 * time.Second), ResourceDeadline: t.Add(606120 * time.Second), TaskDeadline: remoteTestPointer(main.Started.Add(604800 * time.Second)), ReleaseAt: remoteTestPointer(renewed), Input: in, Output: out, Stop: &remoteTaskStop{stage, setup, main}, Collector: collector, ReleaseRevision: math.MaxUint64, ResultID: strings.Repeat("c", 32)},
	}
}
func remoteTestVectors(ch byte, year int, lexical bool) map[string]remoteRecord {
	activeStop := remoteTestMaximum(ch, year, lexical)
	accepted := activeStop
	accepted.State, accepted.Resources, accepted.Active, accepted.Creation = 1, remoteResources{}, nil, 0
	running := remoteTestMaximum(ch, year, lexical)
	running.Active.Phase = 3
	running.Active.Running = &remoteStartReceipt{running.Active.Stop.Task.ID, running.Active.Stop.Task.Started}
	running.Active.Stop, running.Active.Collector, running.Active.ReleaseAt, running.Active.ReleaseRevision = nil, nil, nil, 0
	done := remoteTestMaximum(ch, year, lexical)
	code := int32(0)
	done.State = 3
	done.Done = &remoteDecision{Digest: remoteTestDigest(), Outcome: 4, OriginalExit: &code, Stop: done.Active.Stop, NoLaunch: nil, Collector: done.Active.Collector, ResultID: done.Active.ResultID}
	done.Active = nil // Complete requires joined server owners: no remoteLease/release copy.
	done.Ack = 6      // Known Secrets acked independently; original Pod still pending.
	stageFailure := remoteTestMaximum(ch, year, lexical)
	s := stageFailure.Active.Stop.Stage
	s.Code = 255
	stageFailure.Active.Phase, stageFailure.Active.ClosedReason = 5, 0
	stageFailure.Active.Input.State, stageFailure.Active.Output.State = 3, 3
	stageFailure.Active.Output.Renewed, stageFailure.Active.Output.Expires = stageFailure.Active.Began, stageFailure.Active.Began.Add(120*time.Second)
	stageFailure.Active.NoLaunch = &remoteNoLaunch{1, s, nil, &remoteBlocked{"setup", "PodInitializing"}, remoteBlocked{"task", "PodInitializing"}, &remoteBlocked{"collector", "PodInitializing"}}
	stageFailure.Active.Stop, stageFailure.Active.Collector, stageFailure.Active.TaskDeadline, stageFailure.Active.ReleaseAt, stageFailure.Active.ReleaseRevision = nil, nil, nil, nil, 0
	setupFailure := remoteTestMaximum(ch, year, lexical)
	u := setupFailure.Active.Stop.Setup
	u.Code = 255
	setupFailure.Active.Phase, setupFailure.Active.ClosedReason = 5, 0
	setupFailure.Active.Input.State, setupFailure.Active.Output.State = 3, 3
	setupFailure.Active.Output.Renewed, setupFailure.Active.Output.Expires = setupFailure.Active.Began, setupFailure.Active.Began.Add(120*time.Second)
	setupFailure.Active.NoLaunch = &remoteNoLaunch{2, setupFailure.Active.Stop.Stage, u, nil, remoteBlocked{"task", "PodInitializing"}, &remoteBlocked{"collector", "PodInitializing"}}
	setupFailure.Active.Stop, setupFailure.Active.Collector, setupFailure.Active.TaskDeadline, setupFailure.Active.ReleaseAt, setupFailure.Active.ReleaseRevision = nil, nil, nil, nil, 0
	noLaunchDone := setupFailure
	noLaunchDone.State = 3
	noLaunchDone.Done = &remoteDecision{Digest: remoteTestDigest(), Outcome: 3, NoLaunch: noLaunchDone.Active.NoLaunch}
	noLaunchDone.Active = nil
	noLaunchDone.Ack = 6
	retired := done
	retired.State, retired.Ack = 4, 7
	compact := *done.Done
	compact.Stop, compact.Collector = nil, nil
	retired.Done = &compact
	partial := remoteTestMaximum(ch, year, lexical)
	partial.Creation = 2
	partial.Resources.Pod = nil // Unknown Pod is NEVER acknowledged/known absent.
	partial.Active.Phase, partial.Active.ClosedReason = 6, 5
	partial.Active.Input.State, partial.Active.Output.State = 3, 3
	partial.Active.Output.Renewed, partial.Active.Output.Expires = partial.Active.Began, partial.Active.Began.Add(120*time.Second)
	partial.Active.Stop, partial.Active.Collector, partial.Active.TaskDeadline, partial.Active.ReleaseAt, partial.Active.ReleaseRevision = nil, nil, nil, nil, 0
	partial.Ack = 6 // Both original known Secrets acked, Pod obligation still unresolved.
	mountOnly := remoteTestMaximum(ch, year, lexical)
	mountOnly.Resources.Input, mountOnly.Resources.Output = nil, nil
	mountOnly.Active.Input, mountOnly.Active.Output, mountOnly.Active.Collector = nil, nil, nil
	mountOnly.Active.ResultID, mountOnly.Active.ReleaseRevision = "", 0
	mountOnly.Active.ReleaseAt = nil
	mountOnly.Active.Stop.Stage, mountOnly.Active.Stop.Setup = nil, nil
	held := remoteTestMaximum(ch, year, lexical)
	held.Active.Phase, held.Active.ClosedReason = 6, 6
	held.Active.Input.State, held.Active.Output.State = 3, 3
	// Original success/error returned only after admission had closed. Resources
	// are recorded for cleanup; no activation, native proof or release is invented.
	lateSuccess := remoteTestMaximum(ch, year, lexical)
	lateSuccess.Creation = 1
	lateSuccess.Active.Phase, lateSuccess.Active.ClosedReason = 6, 6
	lateSuccess.Active.Input.State, lateSuccess.Active.Output.State = 3, 3
	lateSuccess.Active.Output.Renewed, lateSuccess.Active.Output.Expires = lateSuccess.Active.Began, lateSuccess.Active.Began.Add(120*time.Second)
	lateSuccess.Active.Stop, lateSuccess.Active.Collector, lateSuccess.Active.TaskDeadline, lateSuccess.Active.ReleaseAt, lateSuccess.Active.ReleaseRevision = nil, nil, nil, nil, 0
	originalError := lateSuccess
	originalError.Creation = 2
	return map[string]remoteRecord{"held-revoked-full-proof": held, "late-success-never-activated": lateSuccess, "original-error-full-resources": originalError, "accepted": accepted, "active-running": running, "active-stop-collector-before-complete": activeStop, "completed-stop-pending-resources": done, "stage-nolaunch-active": stageFailure, "setup-nolaunch-active": setupFailure, "completed-nolaunch-pending-resources": noLaunchDone, "retired-proof-compacted": retired, "partial-secrets-acked-pod-unknown": partial, "mount-only": mountOnly}
}

func TestRemoteRecordCanonicalMaximum(t *testing.T) {
	max := 0
	overflow := 0
	for _, lexical := range []bool{false, true} {
		for _, year := range []int{1, 9999} {
			for _, ch := range []byte{'<', '>', '&', '"', '\\'} {
				for name, r := range remoteTestVectors(ch, year, lexical) {
					t.Run(fmt.Sprintf("%t/%d/%c/%s", lexical, year, ch, name), func(t *testing.T) {
						st := &artifactState{Remote: map[string]remoteRecord{r.Job: r}}
						if _, err := remoteRecordOwner(st, r.Job, r); err != nil {
							t.Fatalf("modeled fixture: %v", err)
						}
						raw, err := remoteEncode(r)
						if err != nil {
							t.Fatal(err)
						}
						if len(raw) > max {
							max = len(raw)
						}
						if len(raw) > 4096 || bytes.HasSuffix(raw, []byte("\n")) {
							t.Fatalf("stored record bytes=%d", len(raw))
						}
						got, err := remoteDecode(raw)
						if err != nil || !reflect.DeepEqual(got, r) {
							t.Fatalf("round trip: %v", err)
						}
						again, err := remoteEncode(got)
						if err != nil || !bytes.Equal(raw, again) {
							t.Fatalf("canonical reencode: %v", err)
						}
						old, err := json.Marshal(r)
						if err != nil {
							t.Fatal(err)
						}
						if len(old) > 4096 {
							overflow++
						}
					})
				}
			}
		}
	}
	// Parent remeasures the corrected full legal profile with the actual Go
	// encoder. No stale numeric maximum transfers from the old private meaning.
	full := remoteTestMaximum('"', 9999, true)
	full.Active.Phase, full.Active.ClosedReason = 6, 6
	full.Active.Output.State = 3
	fullRaw, err := remoteEncode(full)
	if err != nil {
		t.Fatal(err)
	}
	if max != len(fullRaw) {
		t.Fatalf("maximum=%d full legal profile=%d", max, len(fullRaw))
	}
	t.Logf("corrected full legal maximum=%d bytes; fixed cap=4096", max)
	if overflow == 0 {
		t.Fatal("default JSON overflow no longer reproduced")
	}
	// Preserve the original native-style counterexample independently of maxima.
	rawCounterexample := remoteTestMaximum('<', 9999, false)
	// Preserve the exact C2 literal for encoding only, not an admitted scope pair.
	rawCounterexample.Active.Input.Hash = rawCounterexample.Active.Output.Hash
	rawCounterexample.Active.Stop.Task.Code = 255
	// c preserves the old raw encoding counterexample only, not a release origin.
	rawCounterexample.Active.ReleaseAt = remoteTestPointer(rawCounterexample.Active.Stop.Task.Finished.Add(600 * time.Second))
	old, err := json.Marshal(rawCounterexample)
	if err != nil || len(old) <= 4096 {
		t.Fatalf("native-style default overflow bytes=%d err=%v", len(old), err)
	}
}

func TestRemoteRecordFullLexicalIdentities(t *testing.T) {
	for _, ch := range []byte{'<', '>', '&', '"', '\\'} {
		seen := map[string]bool{}
		for i := 0; i < 7; i++ {
			id := remoteTestLexicalIdentity(ch, i)
			if len(id) != 128 || seen[id] {
				t.Fatalf("%q identity %d length=%d or duplicate", ch, i, len(id))
			}
			seen[id] = true
			for _, b := range []byte(id) {
				if b < 32 || b > 126 {
					t.Fatalf("non-ASCII byte %x", b)
				}
			}
			if strings.HasPrefix(id, "containerd://") {
				t.Fatal("fixture accidentally restricted to native prefix")
			}
			raw, err := json.Marshal(id)
			if err != nil {
				t.Fatal(err)
			}
			want := 128*2 + 2
			if ch == '<' || ch == '>' || ch == '&' {
				want = 128*6 + 2
			}
			if len(raw) != want {
				t.Fatalf("%q encoded identity bytes=%d want=%d", ch, len(raw), want)
			}
		}
	}
}

func TestRemoteRecordRetainedHeldProof(t *testing.T) {
	r := remoteTestVectors('"', 9999, true)["held-revoked-full-proof"]
	if r.Creation != 3 || r.State != 2 || r.Active.Phase != 6 || r.Active.ClosedReason != 6 || r.Ack != 0 {
		t.Fatal("missing activated/revoked history")
	}
	if r.Active.Stop == nil || r.Active.Stop.Stage == nil || r.Active.Stop.Setup == nil || r.Active.Collector == nil || r.Active.Input == nil || r.Active.Output == nil || r.Active.Input.State != 3 || r.Active.Output.State != 3 || r.Active.TaskDeadline == nil || r.Active.ReleaseAt == nil || r.Active.ReleaseRevision == 0 || r.Active.ResultID == "" {
		t.Fatal("full retained proof fixture lost a coexistent field")
	}
	st := &artifactState{Remote: map[string]remoteRecord{r.Job: r}}
	if _, err := remoteRecordOwner(st, r.Job, r); err != nil {
		t.Fatal(err)
	}
	for _, creation := range []uint8{1, 2} {
		late := remoteTestVectors('"', 9999, true)["late-success-never-activated"]
		late.Creation = creation
		if late.Resources.Pod == nil || late.Resources.Input == nil || late.Resources.Output == nil || late.Active.Stop != nil || late.Active.Collector != nil || late.Active.ReleaseRevision != 0 {
			t.Fatal("late return must retain resources without inventing activation proof")
		}
		raw, err := remoteEncode(late)
		if err != nil {
			t.Fatal(err)
		}
		got, err := remoteDecode(raw)
		if err != nil || got.Creation != creation {
			t.Fatalf("original-return provenance: %v", err)
		}
	}
	r.Ack = 6
	st.Remote[r.Job] = r
	if _, err := remoteRecordOwner(st, r.Job, r); err == nil {
		t.Fatal("full-proof precompletion retirement accepted")
	}
}

func TestRemoteRecordRejectsNoncanonicalEncoding(t *testing.T) {
	r := remoteTestMaximum('<', 9999, true)
	raw, err := remoteEncode(r)
	if err != nil {
		t.Fatal(err)
	}
	html, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"newline":         append(append([]byte{}, raw...), '\n'),
		"leading-space":   append([]byte(" "), raw...),
		"second-value":    append(append([]byte{}, raw...), []byte("{}")...),
		"unknown-field":   append([]byte(`{"unknown":1,`), raw[1:]...),
		"duplicate-field": append([]byte(`{"r":1,`), raw[1:]...),
		"HTML-escaping":   html,
		"over-limit":      bytes.Repeat([]byte(" "), 4097),
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := remoteDecode(in); err == nil {
				t.Fatal("noncanonical record accepted")
			}
		})
	}
}

func TestRemoteRecordCloneDetached(t *testing.T) {
	for _, name := range []string{"held-revoked-full-proof", "active-running", "setup-nolaunch-active", "completed-stop-pending-resources"} {
		t.Run(name, func(t *testing.T) {
			r, ok := remoteTestVectors('"', 9999, true)[name]
			if !ok {
				t.Fatalf("missing C2 fixture %q", name)
			}
			before, err := remoteEncode(r)
			if err != nil {
				t.Fatal(err)
			}
			c := remoteClone(r)
			c.Authority.Principal = "changed"
			c.Resources.Pod.UID = "changed"
			c.Resources.Input.UID = "changed"
			c.Resources.Output.UID = "changed"
			if a := c.Active; a != nil {
				a.Input.Hash[0]++
				a.Output.Hash[0]++
				if a.TaskDeadline != nil {
					*a.TaskDeadline = a.TaskDeadline.Add(time.Second)
				}
				if a.ReleaseAt != nil {
					*a.ReleaseAt = a.ReleaseAt.Add(time.Second)
				}
				if a.Running != nil {
					a.Running.ID = "changed"
				}
				if a.Stop != nil {
					a.Stop.Stage.ID = "changed"
					a.Stop.Setup.ID = "changed"
					a.Stop.Task.ID = "changed"
				}
				if a.Collector != nil {
					a.Collector.ID = "changed"
				}
				if a.NoLaunch != nil {
					a.NoLaunch.Stage.ID = "changed"
					a.NoLaunch.Setup.ID = "changed"
					a.NoLaunch.CollectorBlocked.Reason = "changed"
				}
			}
			if d := c.Done; d != nil {
				*d.OriginalExit = 1
				d.Stop.Stage.ID = "changed"
				d.Stop.Setup.ID = "changed"
				d.Collector.ID = "changed"
			}
			after, err := remoteEncode(r)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("clone aliased original: %v", err)
			}
		})
	}
}

// remoteTestLinkedRows models all linkage and maximum-width fields. File and
// producer values exercise persisted representation only, not physical proof.
func remoteTestLinkedRows() *artifactState {
	r := remoteTestMaximum('"', 9999, true)
	// Collection is released only for a successful main task. A private result
	// may coexist with collector failure before the failure cleanup handoff.
	r.Active.Stop.Task.Code = 0
	owner := r.Authority.Principal
	now := r.Active.Began
	expires := now.Add(time.Hour)
	file := ArtifactFile{Device: math.MaxUint64, Inode: math.MaxUint64}
	input := ArtifactRef{ID: strings.Repeat("d", 32), SHA256: strings.Repeat("f", 64), Size: artifactMaxSize}
	output := ArtifactRef{ID: r.Active.ResultID, SHA256: strings.Repeat("e", 64), Size: artifactMaxSize}
	producer := &OriginalServerLifetime{Host: strings.Repeat("a", 32), Boot: strings.Repeat("b", 32), NamespaceDevice: math.MaxUint64, NamespaceInode: math.MaxUint64, PID: 1<<31 - 1, StartTicks: math.MaxUint64}
	st := &artifactState{
		Root:    artifactRoot{Layout: 1, SchemaFloor: 7, Policy: "default-v1", ID: strings.Repeat("f", 32), Unit: 65536, Volume: math.MaxUint64},
		Remote:  map[string]remoteRecord{r.Job: r},
		Uploads: map[string]artifactUploadRecord{}, Objects: map[string]artifactObject{},
		Grants: map[string]artifactGrant{}, Reads: map[string]artifactRead{}, JobInputs: map[string]artifactPin{},
	}
	for _, ref := range []ArtifactRef{input, output} {
		origin, source, job := "upload", ref.ID, ""
		attempt := ""
		if ref.ID == output.ID {
			origin, source, job, attempt = "remote_result", r.Job, r.Job, r.Job
		}
		created, uploadExpires := now, expires
		if attempt != "" {
			created = r.Active.Collector.Finished.Add(time.Nanosecond)
			uploadExpires = r.Active.ReleaseAt.Add(time.Duration(r.Active.Budgets[2]) * time.Second)
		}
		st.Uploads[ref.ID] = artifactUploadRecord{RemoteAttemptID: attempt, Owner: owner, Ref: ref, Revision: math.MaxUint64, State: "complete", Ack: ref.Size, Created: created, Expires: uploadExpires, Part: file, LastOffset: ref.Size - artifactChunkSize, LastSize: artifactChunkSize, LastSHA256: strings.Repeat("f", 64)}
		st.Objects[ref.ID] = artifactObject{Owner: owner, JobID: job, Ref: ref, Origin: origin, Source: source, State: "complete", File: file, Manifest: ArtifactManifest{Reference: ref, ExpandedBytes: artifactMaxSize, RegularFiles: 2048, Directories: 512}, Expires: expires, DeleteAfter: expires}
	}
	st.Grants[input.ID] = artifactGrant{Owner: owner, Ref: input, Source: "upload", SourceID: input.ID, Expires: expires}
	st.JobInputs[r.Job] = artifactPin{Owner: owner, JobID: r.Job, Ref: input, TaskDigest: remoteTestDigest()}
	st.Reads[strings.Repeat("e", 32)] = artifactRead{RemoteAttemptID: r.Job, Owner: owner, Ref: input, Revision: 2, Expires: expires, Closing: true, Closed: true, ClosedAt: expires, Producer: producer}
	return st
}

func TestRemoteLinkedRowsEncodingAndOldCodec(t *testing.T) {
	st := remoteTestLinkedRows()
	rows, _, _, err := st.encode()
	if err != nil {
		t.Fatalf("modeled linked encoding: %v", err)
	}
	for _, tt := range []struct {
		name  string
		limit int
	}{{"uploads", 2048}, {"reads", 1024}, {"objects", 1536}} {
		for id, raw := range rows[tt.name] {
			if len(raw) > tt.limit {
				t.Fatalf("%s/%s bytes=%d cap=%d", tt.name, id, len(raw), tt.limit)
			}
		}
	}
	for id, r := range st.Uploads {
		want, err := json.Marshal(r)
		if err != nil || !bytes.Equal(want, rows["uploads"][id]) {
			t.Fatalf("upload codec changed: %v", err)
		}
	}
	for id, r := range st.Reads {
		want, err := json.Marshal(r)
		if err != nil || !bytes.Equal(want, rows["reads"][id]) {
			t.Fatalf("read codec changed: %v", err)
		}
	}
	for id, r := range st.Objects {
		want, err := json.Marshal(r)
		if err != nil || !bytes.Equal(want, rows["objects"][id]) {
			t.Fatalf("object codec changed: %v", err)
		}
	}
	for id, r := range st.Remote {
		want, err := remoteEncode(r)
		if err != nil || !bytes.Equal(want, rows["remote_attempts"][id]) {
			t.Fatalf("remote codec not used: %v", err)
		}
	}
	// A linked row must carry the real maximum-width new field and producer.
	for _, r := range st.Reads {
		if r.RemoteAttemptID == "" || r.Producer == nil || !r.Producer.valid() || r.Producer.NamespaceDevice != math.MaxUint64 || r.Producer.StartTicks != math.MaxUint64 {
			t.Fatal("read maximum lost original server lifetime")
		}
	}
	result := st.Objects[strings.Repeat("c", 32)]
	if result.Origin != "remote_result" || result.JobID == "" || result.Source == "" {
		t.Fatal("missing remote result linkage")
	}
}

func TestRemoteLinkedRowsRejectWrongAttempt(t *testing.T) {
	for _, kind := range []string{"upload", "read", "object"} {
		t.Run(kind, func(t *testing.T) {
			st := remoteTestLinkedRows()
			switch kind {
			case "upload":
				id := strings.Repeat("c", 32)
				r := st.Uploads[id]
				r.RemoteAttemptID = strings.Repeat("9", 32)
				st.Uploads[id] = r
			case "read":
				id := strings.Repeat("e", 32)
				r := st.Reads[id]
				r.RemoteAttemptID = strings.Repeat("9", 32)
				st.Reads[id] = r
			case "object":
				id := strings.Repeat("c", 32)
				r := st.Objects[id]
				r.Source = strings.Repeat("9", 32)
				st.Objects[id] = r
			}
			if _, _, _, err := st.encode(); err == nil {
				t.Fatal("cross-attempt linkage accepted")
			}
		})
	}
}

func TestRemoteOptionalFieldsPreserveLegacyBytes(t *testing.T) {
	// Independent pre-S07 row declarations pin field order as well as omission.
	type legacyUpload struct {
		Owner      string
		Ref        ArtifactRef
		Revision   uint64
		State      string
		Ack        uint64
		Created    time.Time
		Expires    time.Time
		Part       ArtifactFile
		LastOffset uint64
		LastSize   uint64
		LastSHA256 string
	}
	type legacyRead struct {
		Owner    string
		Ref      ArtifactRef
		Revision uint64
		Expires  time.Time
		Closing  bool
		Closed   bool
		ClosedAt time.Time
		Producer *OriginalServerLifetime `json:",omitempty"`
	}
	st := remoteTestLinkedRows()
	u := st.Uploads[strings.Repeat("c", 32)]
	u.RemoteAttemptID = ""
	oldU := legacyUpload{u.Owner, u.Ref, u.Revision, u.State, u.Ack, u.Created, u.Expires, u.Part, u.LastOffset, u.LastSize, u.LastSHA256}
	for _, producer := range []*OriginalServerLifetime{nil, st.Reads[strings.Repeat("e", 32)].Producer} {
		r := st.Reads[strings.Repeat("e", 32)]
		r.RemoteAttemptID = ""
		r.Producer = producer
		oldR := legacyRead{r.Owner, r.Ref, r.Revision, r.Expires, r.Closing, r.Closed, r.ClosedAt, r.Producer}
		for _, pair := range []struct{ new, old any }{{u, oldU}, {r, oldR}} {
			got, err := json.Marshal(pair.new)
			if err != nil {
				t.Fatal(err)
			}
			want, err := json.Marshal(pair.old)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("legacy zero-field bytes changed:\ngot %s\nwant %s", got, want)
			}
			if bytes.Contains(got, []byte("RemoteAttemptID")) {
				t.Fatal("zero remote field persisted")
			}
		}
	}
}

func TestRemoteLinkedPublishingRowCaps(t *testing.T) {
	st := remoteTestLinkedRows()
	id := strings.Repeat("c", 32)
	upload := st.Uploads[id]
	upload.State = "finalizing" // The longest legal upload state spelling.
	st.Uploads[id] = upload
	object := st.Objects[id]
	object.State = "publishing"
	object.File = ArtifactFile{}
	object.Manifest = ArtifactManifest{}
	st.Objects[id] = object
	rows, _, _, err := st.encode()
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(upload)
	if err != nil || !bytes.Equal(rows["uploads"][id], want) || len(want) > 2048 {
		t.Fatalf("publishing upload bytes=%d err=%v", len(want), err)
	}
	for _, table := range artifactTables {
		switch table.name {
		case "uploads":
			if table.max != 2048 {
				t.Fatalf("upload cap widened to %d", table.max)
			}
		case "reads":
			if table.max != 1024 {
				t.Fatalf("read cap widened to %d", table.max)
			}
		case "objects":
			if table.max != 1536 {
				t.Fatalf("object cap widened to %d", table.max)
			}
		case "remote_attempts":
			if table.max != 4096 {
				t.Fatalf("remote cap changed to %d", table.max)
			}
		}
	}
}

func TestRemoteRecordC2VectorNames(t *testing.T) {
	want := []string{"held-revoked-full-proof", "late-success-never-activated", "original-error-full-resources", "accepted", "active-running", "active-stop-collector-before-complete", "completed-stop-pending-resources", "stage-nolaunch-active", "setup-nolaunch-active", "completed-nolaunch-pending-resources", "retired-proof-compacted", "partial-secrets-acked-pod-unknown", "mount-only"}
	values := remoteTestVectors('<', 9999, true)
	if len(values) != len(want) {
		t.Fatalf("C2 vector count=%d want=%d", len(values), len(want))
	}
	for _, name := range want {
		if _, ok := values[name]; !ok {
			t.Errorf("missing exact C2 vector name %q", name)
		}
	}
}

// Normal input completion is a separate modeled state, not a new native or
// transfer receipt. Only the trusted original transfer caller can commit it.
// Preserve all 13 original C2 closed3 vectors; exercise normal4 alongside them.
func TestRemoteRecordNormalInputCodec(t *testing.T) {
	for _, lexical := range []bool{false, true} {
		for _, year := range []int{1, 9999} {
			for _, ch := range []byte{'<', '>', '&', '"', '\\'} {
				t.Run(fmt.Sprintf("%t/%d/%c", lexical, year, ch), func(t *testing.T) {
					r := remoteTestMaximum(ch, year, lexical)
					closed, err := remoteEncode(r)
					if err != nil {
						t.Fatal(err)
					}
					r.Active.Input.State = 4
					st := &artifactState{Remote: map[string]remoteRecord{r.Job: r}}
					if _, err := remoteRecordOwner(st, r.Job, r); err != nil {
						t.Fatalf("normal input fixture: %v", err)
					}
					normal, err := remoteEncode(r)
					if err != nil {
						t.Fatal(err)
					}
					if len(normal) != len(closed) || len(normal) > 4096 {
						t.Fatalf("normal4 bytes=%d closed3=%d fixed cap=4096", len(normal), len(closed))
					}
					got, err := remoteDecode(normal)
					if err != nil || !reflect.DeepEqual(got, r) {
						t.Fatalf("normal4 canonical roundtrip: %v", err)
					}
					again, err := remoteEncode(got)
					if err != nil || !bytes.Equal(normal, again) {
						t.Fatalf("normal4 reencode: %v", err)
					}
				})
			}
		}
	}
}

func TestRemoteRecordNormalInputDetached(t *testing.T) {
	r := remoteTestMaximum('"', 9999, true)
	r.Active.Input.State = 4
	st := &artifactState{Remote: map[string]remoteRecord{r.Job: r}}
	before := *r.Active.Input
	clone := remoteClone(r)
	if *clone.Active.Input != before {
		t.Fatal("clone lost normal input provenance")
	}
	snapshot, err := remoteSnapshot(st, r.Job, r)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Active.Input.State != 4 || snapshot.Active.Input.Hash != before.Hash || snapshot.Active.Input.Epoch != before.Epoch || !snapshot.Active.Input.Expires.Equal(before.Expires) {
		t.Fatal("snapshot lost normal input provenance")
	}
	clone.Active.Input.State = 3
	clone.Active.Input.Hash[0]++
	snapshot.Active.Input.State = 3
	snapshot.Active.Input.Hash[0]++
	snapshot.Active.Input.Expires = snapshot.Active.Input.Expires.Add(time.Second)
	if *r.Active.Input != before || *st.Remote[r.Job].Active.Input != before {
		t.Fatal("normal input clone/snapshot aliased original")
	}
}

func TestRemoteRecordRevokePreservesNormalInput(t *testing.T) {
	r := remoteTestMaximum('"', 9999, true)
	r.Active.Input.State = 4
	before := *r.Active.Input
	remoteClose(&r, RemoteCanceled)
	if *r.Active.Input != before {
		t.Fatal("revocation changed normal input state, hash, epoch or lifetime")
	}
	if r.Active.Output.State != 3 || r.Active.ClosedReason != uint8(RemoteCanceled) {
		t.Fatal("revocation did not close output admission")
	}
	st := &artifactState{Remote: map[string]remoteRecord{r.Job: r}}
	if _, err := remoteRecordOwner(st, r.Job, r); err != nil {
		t.Fatalf("revoked normal input: %v", err)
	}
}

func TestRemoteRecordNormalInputValidation(t *testing.T) {
	for _, tt := range []struct {
		name          string
		creation      uint8
		input, output uint8
	}{
		{"activated-normal", 3, 4, 2},
		{"activated-normal-revoked-output", 3, 4, 3},
		{"unknown-cannot-complete", 0, 4, 3},
		{"unactivated-success-cannot-complete", 1, 4, 3},
		{"original-error-cannot-complete", 2, 4, 3},
		{"output-never-normal4", 3, 3, 4},
		{"both-normal4-invalid", 3, 4, 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := remoteTestMaximum('<', 9999, true)
			// Isolate scope provenance from native proof alternatives.
			r.Creation = tt.creation
			r.Active.Phase = 1
			r.Active.Stop = nil
			r.Active.Collector = nil
			r.Active.TaskDeadline = nil
			r.Active.ReleaseAt = nil
			r.Active.ReleaseRevision = 0
			r.Active.Input.State = tt.input
			r.Active.Output.State = tt.output
			st := &artifactState{Remote: map[string]remoteRecord{r.Job: r}}
			_, err := remoteRecordOwner(st, r.Job, r)
			wantOK := tt.creation == 3 && tt.input == 4 && tt.output != 4
			if (err == nil) != wantOK {
				t.Fatalf("creation=%d input=%d output=%d validation=%v wantOK=%t", tt.creation, tt.input, tt.output, err, wantOK)
			}
		})
	}
}

func TestRemoteRecordReleaseOriginValidation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		edit   func(*remoteRecord)
		wantOK bool
	}{
		{"trusted-release", func(r *remoteRecord) {}, true},
		{"native-finish-boundary", func(r *remoteRecord) { r.Active.ReleaseAt = remoteTestPointer(r.Active.Stop.Task.Finished) }, true},
		{"origin-missing", func(r *remoteRecord) { r.Active.ReleaseAt = nil }, false},
		{"revision-missing", func(r *remoteRecord) { r.Active.ReleaseRevision = 0 }, false},
		{"revision-above-row", func(r *remoteRecord) { r.Revision = math.MaxUint64 - 1 }, false},
		{"before-began", func(r *remoteRecord) { r.Active.ReleaseAt = remoteTestPointer(r.Active.Began.Add(-time.Nanosecond)) }, false},
		{"before-stop-finish", func(r *remoteRecord) {
			r.Active.ReleaseAt = remoteTestPointer(r.Active.Stop.Task.Finished.Add(-time.Nanosecond))
		}, false},
		{"resource-deadline", func(r *remoteRecord) { r.Active.ReleaseAt = remoteTestPointer(r.Active.ResourceDeadline) }, false},
		{"after-resource-deadline", func(r *remoteRecord) {
			r.Active.ReleaseAt = remoteTestPointer(r.Active.ResourceDeadline.Add(time.Nanosecond))
		}, false},
		{"main-failed", func(r *remoteRecord) { r.Active.Stop.Task.Code = 255 }, false},
		{"stop-missing", func(r *remoteRecord) { r.Active.Stop = nil }, false},
		{"output-missing", func(r *remoteRecord) { r.Active.Output = nil }, false},
		{"not-activated", func(r *remoteRecord) { r.Creation = 1; r.Active.Output.State = 3 }, false},
		{"unsupported-arithmetic", func(r *remoteRecord) {
			r.Active.ReleaseAt = remoteTestPointer(time.Date(9999, 12, 31, 23, 59, 59, 1, time.UTC))
			r.Active.ResourceDeadline = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
		}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := remoteTestMaximum('"', 9999, true)
			tt.edit(&r)
			st := &artifactState{Remote: map[string]remoteRecord{r.Job: r}}
			_, err := remoteRecordOwner(st, r.Job, r)
			if (err == nil) != tt.wantOK {
				t.Fatalf("release origin validation=%v wantOK=%t", err, tt.wantOK)
			}
		})
	}
}

func TestRemoteRecordCollectionDeadlineDerived(t *testing.T) {
	r := remoteTestMaximum('"', 9999, true)
	deadline, err := remoteCollectionDeadline(r.Active)
	if err != nil || deadline == nil {
		t.Fatalf("derive collection deadline: %v", err)
	}
	want := r.Active.ReleaseAt.Add(time.Duration(r.Active.Budgets[2]) * time.Second)
	workerDerived := r.Active.Stop.Task.Finished.Add(time.Duration(r.Active.Budgets[2]) * time.Second)
	if !deadline.Equal(want) || deadline.Equal(workerDerived) {
		t.Fatal("collection deadline does not use the trusted durable release origin")
	}
	st := &artifactState{Remote: map[string]remoteRecord{r.Job: r}}
	snapshot, err := remoteSnapshot(st, r.Job, r)
	if err != nil || snapshot.Active.CollectionDeadline == nil || !snapshot.Active.CollectionDeadline.Equal(want) {
		t.Fatalf("derived public snapshot: %v", err)
	}
	origin := *r.Active.ReleaseAt
	*deadline = deadline.Add(time.Second)
	*snapshot.Active.CollectionDeadline = snapshot.Active.CollectionDeadline.Add(time.Second)
	if !r.Active.ReleaseAt.Equal(origin) {
		t.Fatal("derived public deadline aliases private release origin")
	}
	clone := remoteClone(r)
	*clone.Active.ReleaseAt = clone.Active.ReleaseAt.Add(time.Second)
	if !r.Active.ReleaseAt.Equal(origin) {
		t.Fatal("cloned private release origin aliases original")
	}
	unreleased := remoteClone(r)
	unreleased.Active.ReleaseAt = nil
	unreleased.Active.ReleaseRevision = 0
	absent, err := remoteCollectionDeadline(unreleased.Active)
	if err != nil || absent != nil {
		t.Fatalf("unreleased scope deadline=%v err=%v", absent, err)
	}
	overflow := remoteClone(r)
	overflow.Active.ReleaseAt = remoteTestPointer(time.Date(9999, 12, 31, 23, 59, 59, 1, time.UTC))
	if _, err := remoteCollectionDeadline(overflow.Active); err == nil {
		t.Fatal("unsupported collection arithmetic accepted")
	}
}

func TestRemoteNativeReceiptTimeBounds(t *testing.T) {
	// These are MODELED chronology inputs only. The helper has no clock: a
	// transaction supplies trusted now, whereas reload can check only lower
	// bounds and the immutable strict main-start stage deadline.
	r := remoteTestMaximum('"', 9999, true)
	a := r.Active
	now := a.Collector.Finished.Add(time.Second)
	stop := func() backend.DataObservation {
		return backend.DataObservation{TaskStop: remoteStopDTO(a.Stop), Collector: remoteExitDTO(a.Collector)}
	}
	noLaunch := func() backend.DataObservation {
		return backend.DataObservation{NoLaunch: remoteNoLaunchDTO(remoteTestVectors('"', 9999, true)["setup-nolaunch-active"].Active.NoLaunch)}
	}
	for _, tt := range []struct {
		name  string
		make  func() backend.DataObservation
		field func(*backend.DataObservation) *time.Time
	}{
		{"task-start", func() backend.DataObservation {
			return backend.DataObservation{TaskStart: &backend.ContainerStart{ContainerID: a.Stop.Task.ID, StartedAt: a.Stop.Task.Started}}
		}, func(o *backend.DataObservation) *time.Time { return &o.TaskStart.StartedAt }},
		{"stage-start", stop, func(o *backend.DataObservation) *time.Time { return &o.TaskStop.Stage.StartedAt }},
		{"stage-finish", stop, func(o *backend.DataObservation) *time.Time { return &o.TaskStop.Stage.FinishedAt }},
		{"setup-start", stop, func(o *backend.DataObservation) *time.Time { return &o.TaskStop.Setup.StartedAt }},
		{"setup-finish", stop, func(o *backend.DataObservation) *time.Time { return &o.TaskStop.Setup.FinishedAt }},
		{"main-start", stop, func(o *backend.DataObservation) *time.Time { return &o.TaskStop.Task.StartedAt }},
		{"main-finish", stop, func(o *backend.DataObservation) *time.Time { return &o.TaskStop.Task.FinishedAt }},
		{"collector-start", stop, func(o *backend.DataObservation) *time.Time { return &o.Collector.StartedAt }},
		{"collector-finish", stop, func(o *backend.DataObservation) *time.Time { return &o.Collector.FinishedAt }},
		{"nolaunch-stage-start", noLaunch, func(o *backend.DataObservation) *time.Time { return &o.NoLaunch.Stage.StartedAt }},
		{"nolaunch-stage-finish", noLaunch, func(o *backend.DataObservation) *time.Time { return &o.NoLaunch.Stage.FinishedAt }},
		{"nolaunch-setup-start", noLaunch, func(o *backend.DataObservation) *time.Time { return &o.NoLaunch.Setup.StartedAt }},
		{"nolaunch-setup-finish", noLaunch, func(o *backend.DataObservation) *time.Time { return &o.NoLaunch.Setup.FinishedAt }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			valid := tt.make()
			if !remoteObservationTimes(a, valid, &now) || !remoteObservationTimes(a, valid, nil) {
				t.Fatal("valid modeled chronology rejected")
			}
			before := tt.make()
			*tt.field(&before) = a.Began.Add(-time.Nanosecond)
			if remoteObservationTimes(a, before, &now) || remoteObservationTimes(a, before, nil) {
				t.Fatal("pre-Began receipt accepted")
			}
			future := tt.make()
			*tt.field(&future) = now.Add(time.Nanosecond)
			if remoteObservationTimes(a, future, &now) {
				t.Fatal("future receipt accepted by trusted-now qualification")
			}
			if !remoteObservationTimes(a, future, nil) {
				t.Fatal("reload incorrectly invented a historical invocation clock")
			}
			for _, boundary := range []time.Time{a.Began, now} {
				o := tt.make()
				*tt.field(&o) = boundary
				if !remoteObservationTimes(a, o, &now) {
					t.Fatalf("inclusive trusted-time boundary %v rejected", boundary)
				}
			}
		})
	}
	for _, terminal := range []bool{false, true} {
		var o backend.DataObservation
		if terminal {
			o = stop()
			o.TaskStop.Task.StartedAt = a.StageDeadline
		} else {
			o = backend.DataObservation{TaskStart: &backend.ContainerStart{ContainerID: a.Stop.Task.ID, StartedAt: a.StageDeadline}}
		}
		later := a.StageDeadline.Add(time.Second)
		if remoteObservationTimes(a, o, &later) || remoteObservationTimes(a, o, nil) {
			t.Fatalf("strict main stage boundary accepted terminal=%t", terminal)
		}
	}
}
