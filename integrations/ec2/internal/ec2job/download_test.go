//go:build linux

package ec2job

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// These tests run the result download against the real canonical server
// of job_test.go. MODEL portions, each named where it is used: a reply
// lost after the server applied the request; a request the wire fails
// before the server receives it; a Begin reply rewritten after the server
// applied the Begin. The server's download descriptors, IDs, reads and
// closes are real. The test itself, as the job's principal, probes and
// releases a descriptor the job left held, so that the fixture's server
// returns settled; the job never does.

const (
	procBegin  = skyv1connect.ControlServiceBeginArtifactDownloadProcedure
	procRead   = skyv1connect.ControlServiceReadArtifactChunkProcedure
	procClose  = skyv1connect.ControlServiceCloseArtifactDownloadProcedure
	procSubmit = skyv1connect.ControlServiceSubmitWorkspaceJobProcedure
	procGetJob = skyv1connect.ControlServiceGetJobProcedure
)

var (
	errModelRead  = errors.New("model: read request failed before it was sent")
	errModelClose = errors.New("model: close request failed before it was sent")
	errModelCut   = errors.New("model: the process stopped before the request was sent")
)

// bob returns a control client with the job's principal, outside any job.
func (e *serverEnv) bob() skyv1connect.ControlServiceClient {
	return skyv1connect.NewControlServiceClient(e.client(readToken(e.t, e.in.bob), nil), "http://"+e.address)
}

// held reports whether the server still holds the download id for the
// job's principal, by reading one byte of it.
func (e *serverEnv) held(id string) bool {
	_, err := e.bob().ReadArtifactChunk(e.ctx, connect.NewRequest(&v1.ReadArtifactChunkRequest{DownloadId: id, Offset: 0, Limit: 1}))
	return err == nil
}

// release closes the download id the job left held, as the operator
// would, so that the fixture's server settles. It is the test's request,
// never the job's.
func (e *serverEnv) release(id string) {
	e.t.Helper()
	if _, err := e.bob().CloseArtifactDownload(e.ctx, connect.NewRequest(&v1.CloseArtifactDownloadRequest{DownloadId: id})); err != nil {
		e.t.Fatalf("operator release of held download %s: %v", id, err)
	}
}

// diskState decodes the job's committed journal.
func diskState(t *testing.T, dir string) wireState {
	t.Helper()
	var w wireState
	if err := json.Unmarshal(readJournal(t, dir), &w); err != nil {
		t.Fatal(err)
	}
	return w
}

// sum adds the counts of procedure over wires.
func sum(procedure string, wires ...*wire) int {
	n := 0
	for _, w := range wires {
		n += w.count(procedure)
	}
	return n
}

func noOutput(t *testing.T, st Status) {
	t.Helper()
	if st.Output != "" || st.OutputRef != nil || st.OutputFile != "" || st.Succeeded() {
		t.Fatalf("output recorded while the download is not settled: %+v", st)
	}
}

// TestDownloadOnce: one Begin, the bounded reads, one Close, recorded in
// the journal as a release bound to the result, then the saved output.
// A reopened job never downloads it again.
func TestDownloadOnce(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	st, err := j.Start(e.ctx, poll)
	if err != nil {
		t.Fatalf("Start = %v", err)
	}
	checkOutput(t, st)
	reads := int((st.OutputRef.Size + ChunkSize - 1) / ChunkSize)
	if w.count(procBegin) != 1 || w.count(procRead) != reads || w.count(procClose) != 1 {
		t.Fatalf("%d Begins, %d reads, %d Closes; want 1, %d, 1", w.count(procBegin), w.count(procRead), w.count(procClose), reads)
	}
	d := diskState(t, j.dir).Download
	if d == nil || d.Stage != downloadClosed || d.ID != st.DownloadID || d.Ref.ref() != *st.OutputRef || !hexID32.MatchString(d.Attempt) {
		t.Fatalf("journal download %+v, status %+v", d, st)
	}
	if e.held(st.DownloadID) {
		t.Fatal("the server still holds a download the job recorded released")
	}
	j = e.reopen(j)
	w2 := e.attach(j, e.in.bob, nil)
	if again, err := j.Resume(e.ctx, poll); err != nil || !again.Succeeded() {
		t.Fatalf("Resume = %+v, %v", again, err)
	}
	if sum(procBegin, w2)+sum(procSubmit, w2) != 0 {
		t.Fatal("a saved output was downloaded again, or the job submitted again")
	}
}

// TestDownloadBeginLost: the server allocated the download, and its Begin
// reply was lost (MODEL). The job is held with the Begin recorded and no
// ID. The server's descriptor is real and still held. A reopened job
// sends no Begin, read, Close or submit, and cancel reports the hold.
func TestDownloadBeginLost(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, map[string]int{procBegin: 1})
	st, err := j.Start(e.ctx, poll)
	if !errors.Is(err, ErrHeld) || st.Hold != HoldDownloadBegin || st.Download != downloadBegin || st.DownloadID != "" || !st.Durable {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	noOutput(t, st)
	var lost v1.BeginArtifactDownloadResponse
	if err := proto.Unmarshal(w.lostReply(procBegin), &lost); err != nil || !hexID32.MatchString(lost.Id) {
		t.Fatalf("the lost Begin reply holds no download: %v", err)
	}
	if !e.held(lost.Id) {
		t.Fatal("the server did not keep the allocated download")
	}
	if d := diskState(t, j.dir); d.Hold != HoldDownloadBegin || d.Download == nil || d.Download.Stage != downloadBegin || d.Download.ID != "" || d.Output != "" {
		t.Fatalf("journal %+v", d)
	}

	j = e.reopen(j)
	w2 := e.attach(j, e.in.bob, nil)
	if _, err := j.Resume(e.ctx, poll); !errors.Is(err, ErrHeld) {
		t.Fatalf("Resume = %v, want ErrHeld", err)
	}
	if err := j.RequestCancel(); !errors.Is(err, ErrHeld) {
		t.Fatalf("RequestCancel = %v, want ErrHeld", err)
	}
	if n := sum(procBegin, w2) + sum(procRead, w2) + sum(procClose, w2) + sum(procSubmit, w2); n != 0 {
		t.Fatalf("a held download sent %d requests", n)
	}
	if sum(procBegin, w, w2) != 1 || sum(procSubmit, w, w2) != 1 || len(e.jobs()) != 1 {
		t.Fatal("want one Begin, one submit and one job")
	}
	e.release(lost.Id)
}

// TestDownloadReplyNoID: the server allocated the download, and the MODEL
// reply the client reads names no valid ID. The job is held exactly as
// for a lost reply; no read or Close is sent.
func TestDownloadReplyNoID(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	var real string
	w.setReply(procBegin, editReply(func(m *v1.BeginArtifactDownloadResponse) { real, m.Id = m.Id, "download-1" }))
	st, err := j.Start(e.ctx, poll)
	if !errors.Is(err, ErrHeld) || st.Hold != HoldDownloadBegin || st.DownloadID != "" {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	noOutput(t, st)
	if w.count(procRead)+w.count(procClose) != 0 || w.count(procBegin) != 1 {
		t.Fatal("a download without an established ID was read or closed")
	}
	if !e.held(real) {
		t.Fatal("the server did not keep the allocated download")
	}
	e.release(real)
}

// TestDownloadMalformedReply: the server allocated the download, and the
// MODEL reply the client reads keeps its valid ID but has a changed or
// missing reference or an invalid expiry. The ID is stored, the download
// is refused unread and closed once, and the output is not recorded. The
// release recorded, a later Resume of the valid reply downloads the
// result once more: one extra Begin, no extra submit.
func TestDownloadMalformedReply(t *testing.T) {
	e := newServerEnv(t, false)
	for _, tt := range []struct {
		name string
		edit func(*v1.BeginArtifactDownloadResponse)
	}{
		{"other-reference", func(m *v1.BeginArtifactDownloadResponse) { m.Reference.Id = strings.Repeat("f", 32) }},
		{"other-digest", func(m *v1.BeginArtifactDownloadResponse) { m.Reference.Sha256 = strings.Repeat("e", 64) }},
		{"no-reference", func(m *v1.BeginArtifactDownloadResponse) { m.Reference = nil }},
		{"no-expiry", func(m *v1.BeginArtifactDownloadResponse) { m.ExpiresAt = nil }},
		{"invalid-expiry", func(m *v1.BeginArtifactDownloadResponse) { m.ExpiresAt = &timestamppb.Timestamp{Seconds: 1, Nanos: -1} }},
	} {
		j := e.create(tt.name, testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
		w := e.attach(j, e.in.bob, nil)
		var real string
		w.setReply(procBegin, editReply(func(m *v1.BeginArtifactDownloadResponse) { real = m.Id; tt.edit(m) }))
		st, err := j.Start(e.ctx, poll)
		if err == nil || errors.Is(err, ErrHeld) || st.Hold != "" {
			t.Fatalf("%s: Start = %+v, %v; want a refusal, not a hold", tt.name, st, err)
		}
		noOutput(t, st)
		if st.DownloadID != real || st.Download != downloadClosed {
			t.Fatalf("%s: status download %q %q, want the server's %q released", tt.name, st.Download, st.DownloadID, real)
		}
		if d := diskState(t, j.dir).Download; d == nil || d.ID != real || d.Stage != downloadClosed {
			t.Fatalf("%s: journal download %+v", tt.name, d)
		}
		if w.count(procRead) != 0 || w.count(procClose) != 1 {
			t.Fatalf("%s: %d reads, %d Closes; want 0, 1", tt.name, w.count(procRead), w.count(procClose))
		}
		if _, err := os.Lstat(filepath.Join(j.dir, outputName)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s: a refused download froze output.tar", tt.name)
		}
		if e.held(real) {
			t.Fatalf("%s: the server still holds the closed download", tt.name)
		}

		w.setReply(procBegin, nil)
		st, err = j.Resume(e.ctx, poll)
		if err != nil {
			t.Fatalf("%s: Resume = %v", tt.name, err)
		}
		checkOutput(t, st)
		if w.count(procBegin) != 2 || w.count(procClose) != 2 || w.count(procSubmit) != 1 || st.DownloadID == real {
			t.Fatalf("%s: %d Begins, %d Closes, %d submits", tt.name, w.count(procBegin), w.count(procClose), w.count(procSubmit))
		}
	}
}

// TestDownloadIDSaveFailure: the save of the returned download ID fails at
// the rename (MODEL fault between real writes). The ID stays pending in
// memory; no read, Close or GetJob is sent until it is stored. Once it is
// stored, the known download is closed, never read, before anything else,
// and only then is a new download begun.
func TestDownloadIDSaveFailure(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	failing := true
	j.s.fault = func(stage string) error {
		if stage == "rename" && failing && w.count(procBegin) == 1 {
			return errFault
		}
		return nil
	}
	st, err := j.Start(e.ctx, poll)
	if !errors.Is(err, ErrUnpersisted) || st.Durable || st.Download != downloadOpen || !hexID32.MatchString(st.DownloadID) {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	id := st.DownloadID
	noOutput(t, st)
	if d := diskState(t, j.dir).Download; d == nil || d.Stage != downloadBegin || d.ID != "" {
		t.Fatalf("the failed save reached the journal: %+v", d)
	}
	if w.count(procRead)+w.count(procClose) != 0 {
		t.Fatal("a download whose ID was not stored was read or closed")
	}
	gets := w.count(procGetJob)
	if _, err := j.Resume(e.ctx, poll); !errors.Is(err, ErrUnpersisted) || j.Status().DownloadID != id {
		t.Fatalf("Resume with the ID pending = %v", err)
	}
	if w.count(procGetJob) != gets || w.count(procRead)+w.count(procClose)+w.count(procBegin) != 1 {
		t.Fatal("a request was sent while the download ID was pending")
	}
	if !e.held(id) {
		t.Fatal("the server does not hold the pending download")
	}

	failing = false
	st, err = j.Resume(e.ctx, poll)
	if err != nil {
		t.Fatalf("Resume = %v", err)
	}
	checkOutput(t, st)
	if e.held(id) {
		t.Fatal("the original download was not closed")
	}
	if w.count(procBegin) != 2 || w.count(procClose) != 2 || w.count(procSubmit) != 1 {
		t.Fatalf("%d Begins, %d Closes, %d submits; want 2, 2, 1", w.count(procBegin), w.count(procClose), w.count(procSubmit))
	}
}

// TestDownloadReadAndCloseFail: the first read and the Close both fail
// before the server receives them (MODEL). Both causes are returned; the
// job is held with the Close recorded as sent; nothing is frozen or
// recorded as output. The server's descriptor is real and still held. A
// reopened job sends nothing for the download and never begins another.
func TestDownloadReadAndCloseFail(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	w.setBefore(func(p string) error {
		switch p {
		case procRead:
			return errModelRead
		case procClose:
			return errModelClose
		}
		return nil
	})
	st, err := j.Start(e.ctx, poll)
	if !errors.Is(err, errModelRead) || !errors.Is(err, errModelClose) || !errors.Is(err, ErrHeld) {
		t.Fatalf("Start = %v; want the read, the Close and the hold", err)
	}
	if st.Hold != HoldDownloadClose || st.Download != downloadClose || !hexID32.MatchString(st.DownloadID) {
		t.Fatalf("status %+v", st)
	}
	noOutput(t, st)
	if _, err := os.Lstat(filepath.Join(j.dir, outputName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("a failed read froze output.tar")
	}
	id := st.DownloadID
	if !e.held(id) {
		t.Fatal("the server does not hold the unclosed download")
	}

	j = e.reopen(j)
	w2 := e.attach(j, e.in.bob, nil)
	if err := j.CheckResume(); !errors.Is(err, ErrHeld) {
		t.Fatalf("CheckResume = %v, want ErrHeld", err)
	}
	if _, err := j.Resume(e.ctx, poll); !errors.Is(err, ErrHeld) {
		t.Fatalf("Resume = %v, want ErrHeld", err)
	}
	if n := sum(procBegin, w2) + sum(procRead, w2) + sum(procClose, w2) + sum(procSubmit, w2); n != 0 {
		t.Fatalf("a held download sent %d requests", n)
	}
	if j.Status().DownloadID != id || sum(procBegin, w, w2) != 1 {
		t.Fatal("the held download's ID changed, or another was begun")
	}
	e.release(id)
}

// TestDownloadCloseLost: the server applied the Close, and its reply was
// lost (MODEL). The verified output was frozen before the Close, but it
// is not recorded: the release is unknown, and the job is held. The
// server no longer holds the descriptor, which the job cannot know; its
// NotFound to the test's probe is not a receipt. A reopened job sends
// nothing and keeps the hold.
func TestDownloadCloseLost(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, map[string]int{procClose: 1})
	st, err := j.Start(e.ctx, poll)
	if !errors.Is(err, ErrHeld) || st.Hold != HoldDownloadClose || st.Download != downloadClose {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	noOutput(t, st)
	data, err := os.ReadFile(filepath.Join(j.dir, outputName))
	if err != nil {
		t.Fatalf("the verified output was not frozen before the Close: %v", err)
	}
	ref := diskState(t, j.dir).Download.Ref.ref()
	if _, err := artifact.Verify(ref, bytes.NewReader(data)); err != nil {
		t.Fatalf("frozen output does not verify: %v", err)
	}
	if _, err := e.bob().CloseArtifactDownload(e.ctx, connect.NewRequest(&v1.CloseArtifactDownloadRequest{DownloadId: st.DownloadID})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("probe Close = %v; want NotFound for the applied Close", err)
	}

	j = e.reopen(j)
	w2 := e.attach(j, e.in.bob, nil)
	if _, err := j.Resume(e.ctx, poll); !errors.Is(err, ErrHeld) {
		t.Fatalf("Resume = %v, want ErrHeld", err)
	}
	if n := sum(procBegin, w2) + sum(procClose, w2) + sum(procSubmit, w2); n != 0 {
		t.Fatalf("a held download sent %d requests", n)
	}
	noOutput(t, j.Status())
	if sum(procBegin, w, w2) != 1 || sum(procClose, w, w2) != 1 {
		t.Fatal("want one Begin and one Close")
	}
}

// TestDownloadCutBeforeBegin: the journal records the Begin before it is
// sent. The test reads the committed journal at the moment the Begin
// would be sent and then fails the request, which the server never
// receives (MODEL of a process that stopped there). Restoring exactly
// those committed bytes models the restart: the reopened job latches the
// hold and sends no Begin.
func TestDownloadCutBeforeBegin(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	var cut []byte
	w.setBefore(func(p string) error {
		if p != procBegin {
			return nil
		}
		cut = readJournal(t, j.dir)
		return errModelCut
	})
	if _, err := j.Start(e.ctx, poll); !errors.Is(err, errModelCut) {
		t.Fatalf("Start = %v", err)
	}
	var at wireState
	if err := json.Unmarshal(cut, &at); err != nil {
		t.Fatal(err)
	}
	if at.Download == nil || at.Download.Stage != downloadBegin || at.Download.ID != "" || at.Hold != "" || at.JobID == "" {
		t.Fatalf("journal when the Begin was to be sent: %+v", at.Download)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(j.dir, stateName), cut, 0o600); err != nil {
		t.Fatal(err)
	}
	j2, err := Open(j.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j2.Close() })
	if err := j2.Bind(e.cfg, e.verified); err != nil {
		t.Fatal(err)
	}
	w2 := e.attach(j2, e.in.bob, nil)
	if err := j2.CheckResume(); !errors.Is(err, ErrHeld) || j2.Status().Hold != HoldDownloadBegin {
		t.Fatalf("CheckResume = %v, status %+v", err, j2.Status())
	}
	if _, err := j2.Resume(e.ctx, poll); !errors.Is(err, ErrHeld) {
		t.Fatalf("Resume = %v", err)
	}
	if sum(procBegin, w, w2) != 0 || sum(procSubmit, w, w2) != 1 {
		t.Fatal("a Begin reached the server, or the job was submitted again")
	}
}
