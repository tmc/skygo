package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestJobDetails(t *testing.T) {
	for _, state := range []skyv1.JobState{skyv1.JobState_JOB_STATE_SUCCEEDED, skyv1.JobState_JOB_STATE_FAILED, skyv1.JobState_JOB_STATE_UNKNOWN, skyv1.JobState_JOB_STATE_RETRYING} {
		for _, exit := range []*int32{nil, proto.Int32(0), proto.Int32(7)} {
			j := &skyv1.Job{Id: "j1", Target: "local", State: state, ExitCode: exit, Reason: "line\n\r\x1b[31m\tbad", CreatedAt: timestamppb.New(time.Date(2026, 10, 29, 1, 2, 3, 0, time.UTC)), UpdatedAt: &timestamppb.Timestamp{Seconds: 1 << 62}}
			var out bytes.Buffer
			if err := new(options).jobDetails(&out, j); err != nil {
				t.Fatal(err)
			}
			text := out.String()
			for _, want := range []string{`ID: "j1"`, `Target: "local"`, "Created (recorded): 2026-10-29T01:02:03Z", `Reason: "line\n\r\x1b[31m\tbad"`} {
				if !strings.Contains(text, want) {
					t.Fatalf("missing %q in %q", want, text)
				}
			}
			if strings.ContainsAny(text, "\r\x1b\t") || strings.Contains(text, "Updated (recorded)") {
				t.Fatalf("unsafe or invalid timestamp: %q", text)
			}
			if strings.Contains(text, "Exit code:") != (exit != nil) {
				t.Fatalf("optional exit lost: %q", text)
			}
			var gotJSON, wantJSON bytes.Buffer
			gotErr := (&options{json: true}).jobDetails(&gotJSON, j)
			wantErr := writeJSON(&wantJSON, j)
			if (gotErr == nil) != (wantErr == nil) || gotErr != nil && gotErr.Error() != wantErr.Error() {
				t.Fatalf("JSON error changed: %v, want %v", gotErr, wantErr)
			}
			if gotJSON.String() != wantJSON.String() {
				t.Fatal("JSON changed")
			}
		}
	}
}

func TestJobDetailsBounded(t *testing.T) {
	j := &skyv1.Job{Id: strings.Repeat("x", 600), Reason: strings.Repeat("界", 400)}
	for i := 1; i <= 12; i++ {
		j.Attempts = append(j.Attempts, &skyv1.Attempt{Id: "attempt", Ordinal: uint32(i), Reason: strings.Repeat("r", 1000), State: skyv1.JobState_JOB_STATE_FAILED})
	}
	var out bytes.Buffer
	if err := new(options).jobDetails(&out, j); err != nil {
		t.Fatal(err)
	}
	s := out.String()
	if !strings.Contains(s, "(truncated)") || !strings.Contains(s, "Attempts: 4 earlier omitted") || strings.Contains(s, "Attempt 4:") || !strings.Contains(s, "Attempt 12:") || strings.Count(s, "Attempt ") != 8 || len(s) > 12000 {
		t.Fatalf("unbounded or lost recent attempts: %q", s)
	}
	if strings.Contains(s, "\\ufffd") {
		t.Fatal("truncation split UTF-8")
	}
	if jobDetailText("\xff") != `"\xff"` {
		t.Fatal("invalid UTF-8 not escaped")
	}
}

type jobDetailErrorWriter struct{ err error }

func (w jobDetailErrorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestJobDetailsErrors(t *testing.T) {
	want := errors.New("write refused")
	if err := new(options).jobDetails(jobDetailErrorWriter{want}, &skyv1.Job{}); !errors.Is(err, want) {
		t.Fatalf("writer error=%v", err)
	}
	if err := new(options).jobDetails(jobDetailErrorWriter{}, &skyv1.Job{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write error=%v", err)
	}
	if err := new(options).jobDetails(new(bytes.Buffer), nil); err == nil {
		t.Fatal("nil job accepted")
	}
	var row bytes.Buffer
	if err := new(options).job(&row, &skyv1.Job{Id: "j1", Target: "local", State: skyv1.JobState_JOB_STATE_FAILED, Reason: "reason"}); err != nil {
		t.Fatal(err)
	}
	if row.String() != "j1\tlocal\tfailed\n" {
		t.Fatalf("existing row changed: %q", row.String())
	}
}

func TestJobDetailsCommand(t *testing.T) {
	s := new(ergonomicsService)
	srv := serveErgonomics(t, s)
	out, _, err := execute(context.Background(), "--endpoint", srv.URL, "jobs", "get", "j1")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`ID: "j1"`, `Reason: "bad\n\x1b[31m"`, "Exit code: 0", "Attempt 1:"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in %q", want, out)
		}
	}
	if n, _, _ := s.snapshot(); n != 1 {
		t.Fatalf("%d requests, want one get", n)
	}
}
