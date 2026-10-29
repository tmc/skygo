//go:build darwin || linux

package ec2job

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/artifact"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSnapshotDeterministic(t *testing.T) {
	dir := newWorkdir(t, map[string]string{"a.txt": "a\n", "b.txt": "b\n"})
	a1, d1, err := Snapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	a2, d2, err := Snapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a1, a2) || d1 != d2 || !hex64.MatchString(d1) {
		t.Fatal("two snapshots of an unchanged workdir differ")
	}
	got, err := artifact.Validate(bytes.NewReader(a1), int64(len(a1)))
	if err != nil || got != d1 {
		t.Fatalf("Validate = %q, %v; want %q", got, err, d1)
	}
	if _, _, err := Snapshot(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("Snapshot of a missing directory succeeded")
	}
}

// TestSnapshotFrozen: the job uploads the snapshot frozen at Create, not
// the workdir as it is later; a frozen snapshot that changed is refused.
func TestSnapshotFrozen(t *testing.T) {
	j, _, dir := newJob(t)
	want := j.st.Snapshot
	data, err := j.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(data)) != want.Size {
		t.Fatalf("frozen snapshot is %d bytes, journal says %d", len(data), want.Size)
	}
	other, _, err := Snapshot(newWorkdir(t, map[string]string{"input.txt": "changed\n"}))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, workdirName), other, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := j.snapshot(); !errors.Is(err, ErrMismatch) {
		t.Fatalf("snapshot after tampering = %v, want ErrMismatch", err)
	}
	if err := os.Remove(filepath.Join(dir, workdirName)); err != nil {
		t.Fatal(err)
	}
	if _, err := j.snapshot(); !errors.Is(err, ErrNoState) {
		t.Fatalf("snapshot after removal = %v, want ErrNoState", err)
	}
}

func TestCheckUpload(t *testing.T) {
	const size = 3 * ChunkSize
	sha := strings.Repeat("a", 64)
	id := strings.Repeat("b", 32)
	refID := strings.Repeat("c", 32)
	expires := timestamppb.New(time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC))
	upload := func(f func(u *v1.ArtifactUpload)) *v1.ArtifactUpload {
		u := &v1.ArtifactUpload{
			Id:           id,
			Reference:    &v1.ArtifactRef{Id: refID, Sha256: sha, Size: size},
			Acknowledged: ChunkSize,
			State:        v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE,
			ExpiresAt:    expires,
		}
		if f != nil {
			f(u)
		}
		return u
	}
	fresh := state{Snapshot: artifact.Ref{SHA256: sha, Size: size}}
	open := fresh
	open.UploadID, open.Workdir = id, artifact.Ref{ID: refID, SHA256: sha, Size: size}
	for _, tt := range []struct {
		name     string
		st       state
		u        *v1.ArtifactUpload
		ok       bool
		mismatch bool
	}{
		{name: "begin reply", st: fresh, u: upload(nil), ok: true},
		{name: "known upload", st: open, u: upload(nil), ok: true},
		{name: "complete", st: open, u: upload(func(u *v1.ArtifactUpload) {
			u.State, u.Acknowledged = v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_COMPLETE, size
		}), ok: true},
		{name: "nil", st: fresh},
		{name: "invalid ID", st: fresh, u: upload(func(u *v1.ArtifactUpload) { u.Id = "upload-1" })},
		{name: "other ID", st: open, u: upload(func(u *v1.ArtifactUpload) { u.Id = strings.Repeat("d", 32) })},
		{name: "no reference", st: fresh, u: upload(func(u *v1.ArtifactUpload) { u.Reference = nil })},
		{name: "invalid reference", st: fresh, u: upload(func(u *v1.ArtifactUpload) { u.Reference.Id = "x" })},
		{name: "other digest", st: fresh, u: upload(func(u *v1.ArtifactUpload) { u.Reference.Sha256 = strings.Repeat("e", 64) }), mismatch: true},
		{name: "other size", st: fresh, u: upload(func(u *v1.ArtifactUpload) { u.Reference.Size = size + 1 }), mismatch: true},
		{name: "reference changed", st: open, u: upload(func(u *v1.ArtifactUpload) { u.Reference.Id = strings.Repeat("f", 32) }), mismatch: true},
		{name: "negative ack", st: fresh, u: upload(func(u *v1.ArtifactUpload) { u.Acknowledged = -1 })},
		{name: "ack past end", st: fresh, u: upload(func(u *v1.ArtifactUpload) { u.Acknowledged = size + 1 })},
		{name: "no expiry", st: fresh, u: upload(func(u *v1.ArtifactUpload) { u.ExpiresAt = nil })},
		{name: "complete partial", st: open, u: upload(func(u *v1.ArtifactUpload) {
			u.State = v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_COMPLETE
		})},
		{name: "finalizing partial", st: open, u: upload(func(u *v1.ArtifactUpload) {
			u.State = v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_FINALIZING
		})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{st: tt.st}
			ref, err := j.checkUpload(tt.u)
			if tt.ok {
				if err != nil || ref != (artifact.Ref{ID: refID, SHA256: sha, Size: size}) {
					t.Fatalf("checkUpload = %v, %v", ref, err)
				}
				return
			}
			if err == nil {
				t.Fatal("checkUpload accepted an invalid upload")
			}
			if errors.Is(err, ErrMismatch) != tt.mismatch {
				t.Errorf("err = %v, mismatch want %v", err, tt.mismatch)
			}
		})
	}
}

// TestBeginNotStarted: only the server's marked ResourceExhausted refusal,
// received over the wire, proves a Begin admitted no upload.
func TestBeginNotStarted(t *testing.T) {
	marked := func(code connect.Code, wire bool, values ...string) error {
		var e *connect.Error
		if wire {
			e = connect.NewWireError(code, errors.New("refused"))
		} else {
			e = connect.NewError(code, errors.New("refused"))
		}
		for _, v := range values {
			e.Meta().Add("Skygo-Artifact-Begin-Not-Started", v)
		}
		return e
	}
	for _, tt := range []struct {
		name string
		err  error
		want bool
	}{
		{"marked", marked(connect.CodeResourceExhausted, true, "v1"), true},
		{"wrapped", errors.Join(errors.New("context"), marked(connect.CodeResourceExhausted, true, "v1")), true},
		{"unmarked", marked(connect.CodeResourceExhausted, true), false},
		{"other version", marked(connect.CodeResourceExhausted, true, "v2"), false},
		{"repeated", marked(connect.CodeResourceExhausted, true, "v1", "v1"), false},
		{"other code", marked(connect.CodeUnavailable, true, "v1"), false},
		{"local", marked(connect.CodeResourceExhausted, false, "v1"), false},
		{"plain", errors.New("Skygo-Artifact-Begin-Not-Started: v1"), false},
	} {
		if got := beginNotStarted(tt.err); got != tt.want {
			t.Errorf("%s: beginNotStarted = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestLogPageAdd(t *testing.T) {
	type msg struct {
		data      string
		next      int64
		complete  bool
		truncated bool
	}
	for _, tt := range []struct {
		name      string
		offset    int64
		msgs      []msg
		want      string
		next      int64
		truncated bool
		bad       bool
	}{
		{name: "exact", msgs: []msg{{"ab", 2, false, false}, {"c", 3, true, false}}, want: "abc", next: 3},
		{name: "from offset", offset: 5, msgs: []msg{{"xy", 7, true, false}}, want: "xy", next: 7},
		{name: "empty complete", msgs: []msg{{"", 0, true, false}}, next: 0},
		{name: "truncated gap", msgs: []msg{{"ab", 10, false, true}, {"c", 11, true, false}}, want: "abc", next: 11, truncated: true},
		{name: "gap not truncated", msgs: []msg{{"ab", 10, false, false}}, bad: true},
		{name: "offset backward", msgs: []msg{{"ab", 1, false, false}}, bad: true},
		{name: "past limit", offset: MaxLogBytes - 1, msgs: []msg{{"ab", MaxLogBytes + 1, false, false}}, bad: true},
		{name: "after complete", msgs: []msg{{"a", 1, true, false}, {"b", 2, false, false}}, bad: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := LogPage{Offset: tt.offset, NextOffset: tt.offset}
			var buf bytes.Buffer
			var err error
			for _, m := range tt.msgs {
				if err = p.add(&buf, []byte(m.data), m.next, m.complete, m.truncated); err != nil {
					break
				}
			}
			if tt.bad {
				if err == nil {
					t.Fatal("add accepted an invalid stream")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if buf.String() != tt.want || p.NextOffset != tt.next || p.Bytes != int64(len(tt.want)) || p.Truncated != tt.truncated || !p.Complete {
				t.Fatalf("page %+v, data %q", p, buf.String())
			}
		})
	}
}

// shortWriter writes at most n bytes of each Write and, unlike a
// conforming io.Writer, may report no error for a short write.
type shortWriter struct {
	buf bytes.Buffer
	n   int
	err error
}

func (w *shortWriter) Write(p []byte) (int, error) {
	n := min(len(p), w.n)
	w.buf.Write(p[:n])
	return n, w.err
}

// TestLogPageShortWrite: a page advances only past messages written in
// full. A short write without an error, a zero write and a failed write
// each stop it at the last full message; the full write is the twin.
func TestLogPageShortWrite(t *testing.T) {
	errWrite := errors.New("write failed")
	for _, tt := range []struct {
		name string
		w    *shortWriter
		want error
	}{
		{"full", &shortWriter{n: 1 << 10}, nil},
		{"short", &shortWriter{n: 1}, io.ErrShortWrite},
		{"zero", &shortWriter{n: 0}, io.ErrShortWrite},
		{"error", &shortWriter{n: 1, err: errWrite}, errWrite},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := LogPage{}
			if err := p.add(&shortWriter{n: 1 << 10}, []byte("ab"), 2, false, false); err != nil {
				t.Fatal(err)
			}
			err := p.add(tt.w, []byte("cd"), 4, true, false)
			if tt.want == nil {
				if err != nil || p.NextOffset != 4 || p.Bytes != 4 || !p.Complete {
					t.Fatalf("add = %v, page %+v", err, p)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("add = %v, want %v", err, tt.want)
			}
			if p.NextOffset != 2 || p.Bytes != 2 || p.Complete {
				t.Fatalf("page %+v advanced past a message not written in full", p)
			}
		})
	}
}
