//go:build darwin || linux

package ec2job

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// These tests build each journal through the real commit path and then
// cancel it from a reopened journal, as a later command does, with a
// MODEL peer that answers nothing and counts every request.

// cancelTwice calls RequestCancel on j, then again, then once on the
// reopened journal, and returns each error, the journal after the first
// call and the reopened Job.
func cancelTwice(t *testing.T, j *Job, dir string) ([3]error, []byte, *Job) {
	t.Helper()
	var errs [3]error
	errs[0] = j.RequestCancel()
	first := readJournal(t, dir)
	errs[1] = j.RequestCancel()
	if !bytes.Equal(first, readJournal(t, dir)) {
		t.Fatal("a repeated cancel changed the journal")
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	errs[2] = r.RequestCancel()
	if !bytes.Equal(first, readJournal(t, dir)) {
		t.Fatal("a cancel of the reopened journal changed it")
	}
	return errs, first, r
}

// TestRepeatCancelKeepsHold: each journal whose upload, submit or result
// download is in doubt is held by the first cancel, and every later
// cancel, of the same Job or of the reopened journal, reports the same
// hold, with the same request ID and no request.
func TestRepeatCancelKeepsHold(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(st state) state
		hold   string
		state  string
	}{
		{"upload begin in doubt", func(st state) state {
			st.Retention, st.Upload = RetentionDisabled, uploadBeginSent
			return st
		}, HoldUploadBegin, "NEVER_SUBMITTED"},
		{"upload begin held by run", func(st state) state {
			st.Retention, st.Upload, st.Hold = RetentionDisabled, uploadBeginSent, HoldUploadBegin
			return st
		}, HoldUploadBegin, "NEVER_SUBMITTED"},
		{"submit in doubt", func(st state) state {
			return doubtful(st, RetentionDisabled, time.Time{})
		}, HoldCanceledSubmit, ""},
		{"download begin in doubt", func(st state) state {
			st = downloading(st)
			st.Download, st.DownloadID = downloadBegin, ""
			return st
		}, HoldDownloadBegin, "SUCCEEDED"},
		{"download close in doubt", func(st state) state {
			st = downloading(st)
			st.Download = downloadClose
			return st
		}, HoldDownloadClose, "SUCCEEDED"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			j, c, dir := newJob(t)
			peer, n := countingPeer(t)
			// The first Attach records agent facts. Establish that binding
			// before measuring whether repeated cancellation changes state.
			if err := j.Attach(peerSession{peer, sessionFor(c, testVM(c))}); err != nil {
				t.Fatal(err)
			}
			if err := j.commit(tt.mutate(j.st)); err != nil {
				t.Fatal(err)
			}
			id := j.Status().RequestID
			errs, first, r := cancelTwice(t, j, dir)
			for i, err := range errs {
				if !errors.Is(err, ErrHeld) || !strings.Contains(err.Error(), tt.hold) {
					t.Fatalf("cancel %d = %v, want ErrHeld: %s", i+1, err, tt.hold)
				}
			}
			if !bytes.Contains(first, []byte(`"hold": "`+tt.hold+`"`)) || !bytes.Contains(first, []byte(`"desired": "cancel"`)) {
				t.Fatalf("journal after the first cancel:\n%s", first)
			}
			st := r.Status()
			if st.Hold != tt.hold || st.RequestID != id || st.State != tt.state || r.NeedsRemote() || !st.Durable {
				t.Fatalf("reopened status %+v", st)
			}
			if err := r.Bind(c, testVM(c)); err != nil {
				t.Fatal(err)
			}
			if err := r.Attach(peerSession{peer, sessionFor(c, testVM(c))}); err != nil {
				t.Fatal(err)
			}
			r.Cancel(context.Background(), Wait{})
			r.Resume(context.Background(), Wait{})
			if n.Load() != 0 {
				t.Fatalf("a held cancel sent %d requests", n.Load())
			}
			if !bytes.Equal(first, readJournal(t, dir)) {
				t.Fatal("the held journal changed")
			}
		})
	}
}

// TestRepeatCancelEnds is the twin without a hold: a job never submitted
// and with no upload ends at the first cancel, and every later cancel
// succeeds again without a hold or a remote need.
func TestRepeatCancelEnds(t *testing.T) {
	j, _, dir := newJob(t)
	errs, _, r := cancelTwice(t, j, dir)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("cancel %d = %v", i+1, err)
		}
	}
	if st := r.Status(); st.State != "NEVER_SUBMITTED" || st.Hold != "" || st.Upload != uploadNone || r.NeedsRemote() {
		t.Fatalf("status %+v", st)
	}
}

// TestRepeatCancelKnownMismatch: a known accepted job held because its
// reply did not match is canceled by its ID like any known job: every
// cancel succeeds, the hold stays, and a remote cancel is still needed.
func TestRepeatCancelKnownMismatch(t *testing.T) {
	for _, hold := range []string{HoldAcceptedTask, HoldAcceptedReply} {
		t.Run(hold, func(t *testing.T) {
			j, _, dir := newJob(t)
			st := doubtful(j.st, RetentionDisabled, time.Time{})
			st.Submit, st.JobID, st.Hold = submitAccepted, strings.Repeat("1", 32), hold
			st.Observed, st.ObservedAt = "RUNNING", time.Now().UTC()
			if err := j.commit(st); err != nil {
				t.Fatal(err)
			}
			errs, _, r := cancelTwice(t, j, dir)
			for i, err := range errs {
				if err != nil {
					t.Fatalf("cancel %d = %v", i+1, err)
				}
			}
			if s := r.Status(); s.Hold != hold || s.JobID != st.JobID || s.Desired != desiredCancel || !r.NeedsRemote() || !r.Known() {
				t.Fatalf("status %+v", s)
			}
		})
	}
}
