//go:build linux

package ec2job

import (
	"errors"
	"testing"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/proto"
)

// These tests run against the real canonical server of job_test.go, as
// download_test.go's do. A failed intent save is the MODEL fault between
// real writes that store.fault injects at the rename; a lost reply and a
// request failed before the server receives it are MODEL wire faults.
// The server's download descriptors, IDs, reads and closes are real.

const procCancelJob = skyv1connect.ControlServiceCancelJobProcedure

// failIntent fails the rename of each save of a download intent at stage,
// while *failing is set and before any request of procedure was sent.
func failIntent(j *Job, w *wire, stage, procedure string, failing *bool) func(string) error {
	return func(s string) error {
		if s == "rename" && *failing && j.st.Download == stage && w.count(procedure) == 0 {
			return errFault
		}
		return nil
	}
}

// TestUnsentBeginSentOnce: the save of the Begin's intent fails, so this
// live Job sends no request at all. Once a later call stores the intent,
// the same Job does not hold it: it sends that Begin, exactly once, and
// completes the download.
func TestUnsentBeginSentOnce(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	failing := true
	j.s.fault = failIntent(j, w, downloadBegin, procBegin, &failing)
	st, err := j.Start(e.ctx, poll)
	if !errors.Is(err, ErrUnpersisted) || st.Durable || st.Download != downloadBegin || st.Hold != "" {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	if n := w.count(procBegin) + w.count(procRead) + w.count(procClose); n != 0 {
		t.Fatalf("%d download requests sent before the intent was stored", n)
	}
	if err := j.CheckResume(); !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("CheckResume with the save failing = %v", err)
	}

	failing = false
	if err := j.CheckResume(); err != nil {
		t.Fatalf("CheckResume = %v; the live Job knows its Begin was never sent", err)
	}
	if d := diskState(t, j.dir); d.Hold != "" || d.Download == nil || d.Download.Stage != downloadBegin {
		t.Fatalf("journal %+v; want the stored intent, not a hold", d)
	}
	if w.count(procBegin) != 0 {
		t.Fatal("CheckResume sent a Begin")
	}
	st, err = j.Resume(e.ctx, poll)
	if err != nil {
		t.Fatalf("Resume = %v", err)
	}
	checkOutput(t, st)
	if w.count(procBegin) != 1 || w.count(procClose) != 1 || w.count(procSubmit) != 1 {
		t.Fatalf("%d Begins, %d Closes, %d submits; want 1, 1, 1", w.count(procBegin), w.count(procClose), w.count(procSubmit))
	}
	if e.held(st.DownloadID) {
		t.Fatal("the server still holds the released download")
	}
}

// TestUnsentBeginReopenedHolds: the same failed Begin-intent save, then a
// stored intent; but the Job is closed and the journal reopened, which
// has no proof that the Begin was never sent. It holds, and sends nothing.
func TestUnsentBeginReopenedHolds(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	failing := true
	j.s.fault = failIntent(j, w, downloadBegin, procBegin, &failing)
	if _, err := j.Start(e.ctx, poll); !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("Start = %v", err)
	}
	failing = false
	if err := j.CheckResume(); err != nil {
		t.Fatalf("CheckResume = %v", err)
	}

	j = e.reopen(j)
	w2 := e.attach(j, e.in.bob, nil)
	if err := j.CheckResume(); !errors.Is(err, ErrHeld) || j.Status().Hold != HoldDownloadBegin {
		t.Fatalf("CheckResume = %v, hold %q; want %s", err, j.Status().Hold, HoldDownloadBegin)
	}
	if _, err := j.Resume(e.ctx, poll); !errors.Is(err, ErrHeld) {
		t.Fatalf("Resume = %v, want ErrHeld", err)
	}
	if n := sum(procBegin, w, w2) + sum(procRead, w, w2) + sum(procClose, w, w2); n != 0 {
		t.Fatalf("%d download requests", n)
	}
}

// TestUnsentCloseSentOnce: the result is read and frozen, and the save of
// the Close's intent fails, so this live Job sends no Close. Once a later
// call stores the intent, the same Job sends that Close exactly once and
// then records the frozen output. No second Begin is sent.
func TestUnsentCloseSentOnce(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	failing := true
	j.s.fault = failIntent(j, w, downloadClose, procClose, &failing)
	st, err := j.Start(e.ctx, poll)
	if !errors.Is(err, ErrUnpersisted) || st.Download != downloadClose || st.Hold != "" {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	noOutput(t, st)
	id := st.DownloadID
	if w.count(procBegin) != 1 || w.count(procClose) != 0 || !e.held(id) {
		t.Fatal("want one Begin, no Close and the download still held")
	}

	failing = false
	if err := j.CheckResume(); err != nil {
		t.Fatalf("CheckResume = %v; the live Job knows its Close was never sent", err)
	}
	st, err = j.Resume(e.ctx, poll)
	if err != nil {
		t.Fatalf("Resume = %v", err)
	}
	checkOutput(t, st)
	if st.DownloadID != id || w.count(procBegin) != 1 || w.count(procClose) != 1 || w.count(procSubmit) != 1 {
		t.Fatalf("download %s, %d Begins, %d Closes, %d submits; want %s, 1, 1, 1", st.DownloadID, w.count(procBegin), w.count(procClose), w.count(procSubmit), id)
	}
	if e.held(id) {
		t.Fatal("the server still holds the released download")
	}
}

// TestUnsentCloseReopenedHolds: the failed Close-intent save, then a
// stored intent, then a reopened journal. It holds and sends nothing; the
// test releases the server's real descriptor.
func TestUnsentCloseReopenedHolds(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	failing := true
	j.s.fault = failIntent(j, w, downloadClose, procClose, &failing)
	st, err := j.Start(e.ctx, poll)
	if !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("Start = %v", err)
	}
	id := st.DownloadID
	failing = false
	if err := j.CheckResume(); err != nil {
		t.Fatalf("CheckResume = %v", err)
	}

	j = e.reopen(j)
	w2 := e.attach(j, e.in.bob, nil)
	if err := j.CheckResume(); !errors.Is(err, ErrHeld) || j.Status().Hold != HoldDownloadClose {
		t.Fatalf("CheckResume = %v; want %s", err, HoldDownloadClose)
	}
	if err := j.RequestCancel(); !errors.Is(err, ErrHeld) {
		t.Fatalf("RequestCancel = %v, want ErrHeld", err)
	}
	if n := sum(procBegin, w2) + sum(procClose, w, w2); n != 0 {
		t.Fatalf("%d download requests after the reopen", n)
	}
	noOutput(t, j.Status())
	e.release(id)
}

// TestUnsentCloseLostReplyHolds: the Close the live Job sends after its
// failed intent save is applied, and its reply lost (MODEL). That is a
// sent request whose outcome is unknown: the Job holds, and never sends
// it again.
func TestUnsentCloseLostReplyHolds(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, map[string]int{procClose: 1})
	failing := true
	j.s.fault = failIntent(j, w, downloadClose, procClose, &failing)
	if _, err := j.Start(e.ctx, poll); !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("Start = %v", err)
	}
	failing = false
	st, err := j.Resume(e.ctx, poll)
	if !errors.Is(err, ErrHeld) || st.Hold != HoldDownloadClose || w.count(procClose) != 1 {
		t.Fatalf("Resume = %+v, %v; want the hold after one Close", st, err)
	}
	if _, err := j.Resume(e.ctx, poll); !errors.Is(err, ErrHeld) || w.count(procClose) != 1 {
		t.Fatalf("Resume again = %v, %d Closes; want the hold and no second Close", err, w.count(procClose))
	}
	noOutput(t, j.Status())
}

// knownOpen starts a job whose returned download ID's save fails (MODEL
// fault), as TestDownloadIDSaveFailure does, then clears the fault and
// latches the cancel, which stores the ID. It returns the reopened Job
// and the server's real download ID.
func knownOpen(t *testing.T, e *serverEnv) (*Job, string) {
	t.Helper()
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
	if !errors.Is(err, ErrUnpersisted) || st.Download != downloadOpen {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	id := st.DownloadID
	failing = false
	if err := j.RequestCancel(); err != nil {
		t.Fatalf("RequestCancel = %v", err)
	}
	if d := diskState(t, j.dir); d.Download == nil || d.Download.Stage != downloadOpen || d.Download.ID != id || d.Desired != desiredCancel {
		t.Fatalf("journal %+v; want the known download and the cancel", d)
	}
	if !j.NeedsRemote() {
		t.Fatal("NeedsRemote = false for a known open download")
	}
	if !e.held(id) || w.count(procClose) != 0 {
		t.Fatal("want the download held and not closed")
	}
	j = e.reopen(j)
	if err := j.RequestCancel(); err != nil || !j.NeedsRemote() {
		t.Fatalf("reopened RequestCancel = %v, NeedsRemote %v; want no hold and a download to close", err, j.NeedsRemote())
	}
	return j, id
}

// TestCancelClosesKnownDownload: a canceled job whose download ID is known
// and was never closed. Cancel closes exactly that ID, once, before it
// reports the ended job; it begins no download, submits and cancels
// nothing else. A second Cancel sends nothing.
func TestCancelClosesKnownDownload(t *testing.T) {
	e := newServerEnv(t, false)
	j, id := knownOpen(t, e)
	w := e.attach(j, e.in.bob, nil)
	st, err := j.Cancel(e.ctx, poll)
	if err != nil || st.Download != downloadClosed || st.DownloadID != id || st.Hold != "" {
		t.Fatalf("Cancel = %+v, %v", st, err)
	}
	noOutput(t, st)
	if w.count(procClose) != 1 || w.count(procBegin)+w.count(procRead)+w.count(procSubmit)+w.count(procCancelJob) != 0 {
		t.Fatalf("%d Closes, %d Begins, %d reads, %d submits, %d CancelJobs; want only the one Close",
			w.count(procClose), w.count(procBegin), w.count(procRead), w.count(procSubmit), w.count(procCancelJob))
	}
	if d := diskState(t, j.dir).Download; d == nil || d.Stage != downloadClosed || d.ID != id {
		t.Fatalf("journal download %+v", d)
	}
	if e.held(id) {
		t.Fatal("the server still holds the closed download")
	}
	if j.NeedsRemote() {
		t.Fatal("NeedsRemote after the release")
	}
	if _, err := j.Cancel(e.ctx, poll); err != nil || w.count(procClose) != 1 {
		t.Fatalf("Cancel again = %v, %d Closes", err, w.count(procClose))
	}
}

// TestCancelClosedKnownDownloadReopened: the healthy twin of the held
// cases below on a reopened journal. Once Cancel recorded the release, a
// reopened job's Cancel reports the ended job with a nil error and sends
// nothing.
func TestCancelClosedKnownDownloadReopened(t *testing.T) {
	e := newServerEnv(t, false)
	j, id := knownOpen(t, e)
	w := e.attach(j, e.in.bob, nil)
	if _, err := j.Cancel(e.ctx, poll); err != nil || w.count(procClose) != 1 {
		t.Fatalf("Cancel = %v, %d Closes", err, w.count(procClose))
	}
	j = e.reopen(j)
	w2 := e.attach(j, e.in.bob, nil)
	for i := range 2 {
		st, err := j.Cancel(e.ctx, poll)
		if err != nil || st.Hold != "" || st.Download != downloadClosed || st.DownloadID != id {
			t.Fatalf("reopened Cancel %d = %+v, %v; want the release and no error", i, st, err)
		}
	}
	if n := allRequests(w2); n != 0 {
		t.Fatalf("%d requests for a released download of an ended job", n)
	}
}

// allRequests counts every download, submit and job request w sent.
func allRequests(w *wire) int {
	return w.count(procBegin) + w.count(procRead) + w.count(procClose) + w.count(procSubmit) + w.count(procCancelJob) + w.count(procGetJob)
}

// TestCancelKnownDownloadCloseFails: the Close fails before the server
// receives it (MODEL). Cancel holds the job. Every later Cancel, on the
// same Job or a reopened journal, reports the hold although the job has
// ended, and sends nothing; the server still holds the real descriptor,
// which the test releases. The wire does not count a request it fails
// before sending, so the attempts are counted here.
func TestCancelKnownDownloadCloseFails(t *testing.T) {
	e := newServerEnv(t, false)
	j, id := knownOpen(t, e)
	w := e.attach(j, e.in.bob, nil)
	closes := 0
	w.setBefore(func(p string) error {
		if p == procClose {
			closes++
			return errModelClose
		}
		return nil
	})
	st, err := j.Cancel(e.ctx, poll)
	if !errors.Is(err, errModelClose) || !errors.Is(err, ErrHeld) || st.Hold != HoldDownloadClose || closes != 1 {
		t.Fatalf("Cancel = %+v, %v, %d Close attempts", st, err, closes)
	}
	if !st.Ended() {
		t.Fatalf("status %+v; want the ended job, so that only the hold refuses", st)
	}
	if err := j.RequestCancel(); !errors.Is(err, ErrHeld) {
		t.Fatalf("RequestCancel = %v, want ErrHeld", err)
	}
	if j.NeedsRemote() {
		t.Fatal("NeedsRemote for a held Close")
	}
	for i := range 2 {
		st, err := j.Cancel(e.ctx, poll)
		if !errors.Is(err, ErrHeld) || st.Hold != HoldDownloadClose {
			t.Fatalf("Cancel again %d = %+v, %v; want the hold", i, st, err)
		}
	}
	if closes != 1 || allRequests(w) != 0 {
		t.Fatalf("%d Close attempts, %d requests; want the one refused Close and nothing else", closes, allRequests(w))
	}

	j = e.reopen(j)
	w2 := e.attach(j, e.in.bob, nil)
	if _, err := j.Cancel(e.ctx, poll); !errors.Is(err, ErrHeld) {
		t.Fatalf("reopened Cancel = %v, want ErrHeld", err)
	}
	if n := allRequests(w2); n != 0 || closes != 1 {
		t.Fatalf("%d requests after the reopen, %d Close attempts", n, closes)
	}
	if !e.held(id) {
		t.Fatal("the server does not hold the download whose Close never arrived")
	}
	e.release(id)
}

// TestCancelKnownDownloadCloseLost: the server applies the Close and its
// reply is lost (MODEL). Cancel holds: a lost reply is no release. A
// repeated Cancel and a reopened journal's Cancel report the hold and
// send nothing.
func TestCancelKnownDownloadCloseLost(t *testing.T) {
	e := newServerEnv(t, false)
	j, _ := knownOpen(t, e)
	w := e.attach(j, e.in.bob, map[string]int{procClose: 1})
	st, err := j.Cancel(e.ctx, poll)
	if !errors.Is(err, ErrHeld) || st.Hold != HoldDownloadClose || st.Download != downloadClose {
		t.Fatalf("Cancel = %+v, %v", st, err)
	}
	if _, err := j.Cancel(e.ctx, poll); !errors.Is(err, ErrHeld) {
		t.Fatalf("Cancel again = %v, want ErrHeld", err)
	}
	j = e.reopen(j)
	w2 := e.attach(j, e.in.bob, nil)
	if _, err := j.Cancel(e.ctx, poll); !errors.Is(err, ErrHeld) {
		t.Fatalf("reopened Cancel = %v, want ErrHeld", err)
	}
	if err := j.RequestCancel(); !errors.Is(err, ErrHeld) {
		t.Fatalf("reopened RequestCancel = %v, want ErrHeld", err)
	}
	if n := allRequests(w2) + sum(procBegin, w); n != 0 || w.count(procClose) != 1 || w.count(procCancelJob) != 0 {
		t.Fatalf("%d further requests, %d Closes, %d CancelJobs; want none, one, none", n, w.count(procClose), w.count(procCancelJob))
	}
}

// TestCancelCloseLostHoldUnsaved: the Close's reply is lost (MODEL) and
// the save of the hold fails too (MODEL fault), so the journal records the
// Close as sent without a hold. While the save fails, Cancel stores it
// first and sends nothing. A reopened journal, which has no hold and no
// proof that the Close was unsent, is held by Cancel itself, before any
// RequestCancel, though the job has ended; it sends nothing.
func TestCancelCloseLostHoldUnsaved(t *testing.T) {
	e := newServerEnv(t, false)
	j, _ := knownOpen(t, e)
	w := e.attach(j, e.in.bob, map[string]int{procClose: 1})
	j.s.fault = func(stage string) error {
		if stage == "rename" && j.st.Hold == HoldDownloadClose {
			return errFault
		}
		return nil
	}
	st, err := j.Cancel(e.ctx, poll)
	if !errors.Is(err, ErrHeld) || !errors.Is(err, ErrUnpersisted) || w.count(procClose) != 1 {
		t.Fatalf("Cancel = %+v, %v, %d Closes", st, err, w.count(procClose))
	}
	if d := diskState(t, j.dir); d.Hold != "" || d.Download == nil || d.Download.Stage != downloadClose {
		t.Fatalf("journal %+v; want the sent Close without its hold", d)
	}
	if _, err := j.Cancel(e.ctx, poll); !errors.Is(err, ErrUnpersisted) || w.count(procClose) != 1 {
		t.Fatalf("Cancel with the save failing = %v, %d Closes", err, w.count(procClose))
	}

	j = e.reopen(j)
	w2 := e.attach(j, e.in.bob, nil)
	if j.NeedsRemote() {
		t.Fatal("NeedsRemote for a Close in doubt")
	}
	for i := range 2 {
		st, err := j.Cancel(e.ctx, poll)
		if !errors.Is(err, ErrHeld) || st.Hold != HoldDownloadClose || !st.Ended() {
			t.Fatalf("reopened Cancel %d = %+v, %v; want the hold of the ended job", i, st, err)
		}
	}
	if d := diskState(t, j.dir); d.Hold != HoldDownloadClose {
		t.Fatalf("journal hold %q; want it stored", d.Hold)
	}
	if n := allRequests(w2) + sum(procBegin, w); n != 0 || w.count(procClose) != 1 {
		t.Fatalf("%d further requests, %d Closes; want none and one", n, w.count(procClose))
	}
}

// TestCancelBeginLostReopened: the server allocated the download and the
// Begin's reply was lost (MODEL), and the hold's save failed (MODEL
// fault), so the reopened journal records the Begin without a hold. The
// reopened RequestCancel latches it, and every Cancel then reports it,
// though the job has ended: no Begin, Close or submit is sent. The test
// releases the server's real descriptor.
func TestCancelBeginLostReopened(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, map[string]int{procBegin: 1})
	j.s.fault = func(stage string) error {
		if stage == "rename" && j.st.Hold == HoldDownloadBegin {
			return errFault
		}
		return nil
	}
	st, err := j.Start(e.ctx, poll)
	if !errors.Is(err, ErrHeld) || !errors.Is(err, ErrUnpersisted) || st.Download != downloadBegin {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	var lost v1.BeginArtifactDownloadResponse
	if err := proto.Unmarshal(w.lostReply(procBegin), &lost); err != nil || !hexID32.MatchString(lost.Id) {
		t.Fatalf("the lost Begin reply holds no download: %v", err)
	}
	if d := diskState(t, j.dir); d.Hold != "" || d.Download == nil || d.Download.Stage != downloadBegin {
		t.Fatalf("journal %+v; want the sent Begin without its hold", d)
	}

	j = e.reopen(j)
	w2 := e.attach(j, e.in.bob, nil)
	if err := j.RequestCancel(); !errors.Is(err, ErrHeld) {
		t.Fatalf("reopened RequestCancel = %v, want ErrHeld", err)
	}
	for i := range 2 {
		st, err := j.Cancel(e.ctx, poll)
		if !errors.Is(err, ErrHeld) || st.Hold != HoldDownloadBegin || !st.Ended() {
			t.Fatalf("Cancel %d = %+v, %v; want the hold of the ended job", i, st, err)
		}
	}
	if n := allRequests(w2); n != 0 || w.count(procBegin) != 1 || w.count(procSubmit) != 1 {
		t.Fatalf("%d requests after the reopen, %d Begins, %d submits; want none, one, one", n, w.count(procBegin), w.count(procSubmit))
	}
	if !e.held(lost.Id) {
		t.Fatal("the server did not keep the allocated download")
	}
	e.release(lost.Id)
}

// TestCancelClosedDownloadSendsNothing: the twin of the cases above. A job
// whose download was closed and whose output is saved needs no remote
// call to cancel, and Cancel sends none.
func TestCancelClosedDownloadSendsNothing(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	e.attach(j, e.in.bob, nil)
	if _, err := j.Start(e.ctx, poll); err != nil {
		t.Fatalf("Start = %v", err)
	}
	j = e.reopen(j)
	if err := j.RequestCancel(); err != nil || j.NeedsRemote() {
		t.Fatalf("RequestCancel = %v, NeedsRemote %v", err, j.NeedsRemote())
	}
	w := e.attach(j, e.in.bob, nil)
	st, err := j.Cancel(e.ctx, poll)
	if err != nil || st.Download != downloadClosed || !st.Succeeded() {
		t.Fatalf("Cancel = %+v, %v", st, err)
	}
	if n := w.count(procClose) + w.count(procBegin) + w.count(procSubmit) + w.count(procCancelJob) + w.count(procGetJob); n != 0 {
		t.Fatalf("%d requests for an ended job with a closed download", n)
	}
}

// TestCancelWithdrawsUnsentBegin: the Begin's intent save failed and was
// then stored. A cancel requested by the same live Job withdraws that
// never-sent Begin: no download exists, nothing is held and nothing is
// sent. A reopened job needs no remote call.
func TestCancelWithdrawsUnsentBegin(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	failing := true
	j.s.fault = failIntent(j, w, downloadBegin, procBegin, &failing)
	if _, err := j.Start(e.ctx, poll); !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("Start = %v", err)
	}
	failing = false
	if err := j.RequestCancel(); err != nil {
		t.Fatalf("RequestCancel = %v", err)
	}
	if d := diskState(t, j.dir); d.Download != nil || d.Hold != "" || d.Desired != desiredCancel {
		t.Fatalf("journal %+v; want no download and no hold", d)
	}
	j = e.reopen(j)
	if err := j.RequestCancel(); err != nil || j.NeedsRemote() {
		t.Fatalf("reopened RequestCancel = %v, NeedsRemote %v", err, j.NeedsRemote())
	}
	if n := w.count(procBegin) + w.count(procClose); n != 0 {
		t.Fatalf("%d download requests", n)
	}
}
