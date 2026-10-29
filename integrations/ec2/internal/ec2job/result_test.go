//go:build linux

package ec2job

import (
	"errors"
	"strings"
	"testing"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
)

// The pinned server sets every workspace job's result, ABSENT unless the
// attempt stored, expired or holds one, and names the last attempt, or
// the job itself while it has none (internal/rpc/workspace.go wireJob and
// wireArtifactResult; internal/store/artifact_reads.go). The cases below
// rewrite the GetJob replies the client reads (MODEL) after the real
// server answered: they are malformed or changed observations, not
// anything the server sends.

// endedResult rewrites the result of an ended job in a GetJob reply.
func endedResult(edit func(*v1.Job)) func(*v1.GetJobResponse) {
	return func(m *v1.GetJobResponse) {
		switch m.GetJob().GetState() {
		case v1.JobState_JOB_STATE_SUCCEEDED, v1.JobState_JOB_STATE_FAILED, v1.JobState_JOB_STATE_CANCELED, v1.JobState_JOB_STATE_UNKNOWN:
			edit(m.Job)
		}
	}
}

// TestInvalidResultRefused: a missing result, one naming another attempt,
// and one whose state is UNSPECIFIED or unknown are refused. Nothing is
// recorded, no download is begun and nothing is submitted again; the
// next valid observation of the same job is collected.
func TestInvalidResultRefused(t *testing.T) {
	e := newServerEnv(t, false)
	for _, tt := range []struct {
		name string
		edit func(*v1.Job)
	}{
		{"missing", func(j *v1.Job) { j.Result = nil }},
		{"unspecified", func(j *v1.Job) {
			j.Result.State, j.Result.Reference = v1.ArtifactResultState_ARTIFACT_RESULT_STATE_UNSPECIFIED, nil
		}},
		{"unknown", func(j *v1.Job) { j.Result.State = v1.ArtifactResultState(99) }},
		{"other-attempt", func(j *v1.Job) { j.Result.AttemptId = strings.Repeat("9", 32) }},
		{"no-attempt-id", func(j *v1.Job) { j.Result.AttemptId = "" }},
		{"absent-with-reference", func(j *v1.Job) { j.Result.State = v1.ArtifactResultState_ARTIFACT_RESULT_STATE_ABSENT }},
		{"expired-with-reference", func(j *v1.Job) { j.Result.State = v1.ArtifactResultState_ARTIFACT_RESULT_STATE_EXPIRED }},
		{"available-without-reference", func(j *v1.Job) { j.Result.Reference = nil }},
	} {
		j := e.create(tt.name, testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
		w := e.attach(j, e.in.bob, nil)
		w.setReply(procGetJob, editReply(endedResult(tt.edit)))
		st, err := j.Start(e.ctx, poll)
		if !errors.Is(err, errResult) || st.State != "SUCCEEDED" {
			t.Fatalf("%s: Start = %+v, %v; want the result refused", tt.name, st, err)
		}
		if st.Output != "" || st.Hold != "" || st.Download != "" || !st.Durable || st.Succeeded() {
			t.Fatalf("%s: a refused result was recorded: %+v", tt.name, st)
		}
		if d := diskState(t, j.dir); d.Output != "" || d.Download != nil {
			t.Fatalf("%s: journal output %q, download %+v", tt.name, d.Output, d.Download)
		}
		if w.count(procBegin) != 0 || w.count(procSubmit) != 1 {
			t.Fatalf("%s: %d Begins, %d submits", tt.name, w.count(procBegin), w.count(procSubmit))
		}

		w.setReply(procGetJob, nil)
		st, err = j.Resume(e.ctx, poll)
		if err != nil {
			t.Fatalf("%s: Resume = %v", tt.name, err)
		}
		checkOutput(t, st)
		if w.count(procBegin) != 1 || w.count(procSubmit) != 1 {
			t.Fatalf("%s: %d Begins, %d submits after the valid observation", tt.name, w.count(procBegin), w.count(procSubmit))
		}
	}
}

// TestExplicitAbsentResult: an explicit ABSENT, without a reference, of
// the job's attempt is recorded as absent, with no download; so is an
// explicit EXPIRED as expired. Neither is a success.
func TestExplicitAbsentResult(t *testing.T) {
	e := newServerEnv(t, false)
	for _, tt := range []struct {
		name  string
		state v1.ArtifactResultState
		want  string
	}{
		{"absent", v1.ArtifactResultState_ARTIFACT_RESULT_STATE_ABSENT, outputAbsent},
		{"expired", v1.ArtifactResultState_ARTIFACT_RESULT_STATE_EXPIRED, outputExpired},
	} {
		j := e.create(tt.name, testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
		w := e.attach(j, e.in.bob, nil)
		w.setReply(procGetJob, editReply(endedResult(func(j *v1.Job) { j.Result.State, j.Result.Reference = tt.state, nil })))
		st, err := j.Start(e.ctx, poll)
		if err != nil || st.State != "SUCCEEDED" || st.Output != tt.want || st.OutputRef != nil || st.Succeeded() {
			t.Fatalf("%s: Start = %+v, %v", tt.name, st, err)
		}
		if w.count(procBegin) != 0 {
			t.Fatalf("%s: an explicitly %s result was downloaded", tt.name, tt.want)
		}
	}
}
