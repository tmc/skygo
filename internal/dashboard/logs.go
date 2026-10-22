package dashboard

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/backend"
)

const (
	maxLogPage = 64 << 10 // log bytes shown on one HTML page
	maxLogText = 1 << 20  // body bytes of one logs.txt response
	maxCursor  = 683
)

var (
	errInvalidDomain = errors.New("server returned an invalid fixed log domain")
	errNoAttempt     = errors.New("server returned no such attempt")
	errPageFull      = errors.New("log page full")
	errResponseFull  = errors.New("log response full")
)

// logQuery is one log selection, bounded as "skygo jobs logs" bounds it.
type logQuery struct {
	id        string
	offset    int64
	attempt   uint32
	cursor    string
	cursorSet bool
	follow    bool
}

// parseLogQuery reads attempt (1..10), offset (0..1 MiB) and cursor (at most
// 683 bytes, exclusive with a nonzero offset) from r's query.
func parseLogQuery(r *http.Request) (logQuery, bool) {
	q := r.URL.Query()
	l := logQuery{id: r.PathValue("id"), cursor: q.Get("cursor"), cursorSet: q.Has("cursor")}
	if q.Has("offset") {
		n, err := strconv.ParseInt(q.Get("offset"), 10, 64)
		if err != nil || n < 0 || n > backend.MaxLogBytes {
			return l, false
		}
		l.offset = n
	}
	if q.Has("attempt") {
		n, err := strconv.ParseUint(q.Get("attempt"), 10, 32)
		if err != nil || n < 1 || n > 10 {
			return l, false
		}
		l.attempt = uint32(n)
	}
	if len(l.cursor) > maxCursor || !utf8.ValidString(l.cursor) || (l.cursor != "" && l.offset != 0) {
		return l, false
	}
	return l, true
}

// values encodes q's selection, without follow, as link query values.
func (q logQuery) values() url.Values {
	v := url.Values{}
	if q.attempt != 0 {
		v.Set("attempt", strconv.FormatUint(uint64(q.attempt), 10))
	}
	if q.offset != 0 {
		v.Set("offset", strconv.FormatInt(q.offset, 10))
	}
	if q.cursorSet {
		v.Set("cursor", q.cursor)
	}
	return v
}

type logChunk struct {
	data       []byte
	nextOffset int64
	complete   bool
	truncated  bool
	attemptID  string
	cursor     string
}

// readLogs selects retained or legacy logs as "skygo jobs logs" does in
// internal/cli/retention_logs.go. A cursor parameter, even empty, or
// GetRetention reporting retention enabled selects StreamRetainedJobLogs;
// Unimplemented or disabled retention selects StreamJobLogs after checking
// that a requested attempt beyond the first exists. It passes each chunk to
// emit in order and returns emit's error or the stream's.
//
// This copies the CLI's selection branch. TestLogsMatchCLI pins both to the
// same server responses with retention disabled and enabled; a change to
// either copy must keep that test passing and extend it for new behavior.
func (s *server) readLogs(ctx context.Context, q logQuery, emit func(logChunk) error) error {
	retained := q.cursorSet
	if !q.cursorSet {
		rctx, cancel := context.WithTimeout(ctx, s.timeout)
		info, err := s.client.GetRetention(rctx, connect.NewRequest(&v1.GetRetentionRequest{}))
		cancel()
		if err != nil && connect.CodeOf(err) != connect.CodeUnimplemented {
			return err
		}
		if err == nil {
			if info == nil || info.Msg == nil {
				return errors.New("server returned no retention information")
			}
			retained = info.Msg.Enabled
		}
	}
	if retained {
		stream, err := s.client.StreamRetainedJobLogs(ctx, connect.NewRequest(&v1.StreamRetainedJobLogsRequest{Id: q.id, Offset: q.offset, Attempt: q.attempt, Follow: q.follow, Cursor: q.cursor}))
		if err != nil {
			return err
		}
		defer stream.Close()
		selected := ""
		for stream.Receive() {
			m := stream.Msg()
			if !hexID(m.AttemptId) || (selected != "" && selected != m.AttemptId) || m.Cursor == "" || len(m.Cursor) > maxCursor || !utf8.ValidString(m.Cursor) {
				return errInvalidDomain
			}
			selected = m.AttemptId
			if err := emit(logChunk{m.Data, m.NextOffset, m.Complete, m.Truncated, m.AttemptId, m.Cursor}); err != nil {
				return err
			}
		}
		return stream.Err()
	}
	if q.attempt > 1 {
		rctx, cancel := context.WithTimeout(ctx, s.timeout)
		resp, err := s.client.GetJob(rctx, connect.NewRequest(&v1.GetJobRequest{Id: q.id}))
		cancel()
		if err != nil {
			return err
		}
		if resp == nil || resp.Msg == nil || resp.Msg.Job == nil {
			return errors.New("server returned no job")
		}
		if q.attempt > uint32(len(resp.Msg.Job.Attempts)) {
			return errNoAttempt
		}
	}
	stream, err := s.client.StreamJobLogs(ctx, connect.NewRequest(&v1.StreamJobLogsRequest{Id: q.id, Offset: q.offset, Follow: q.follow, Attempt: q.attempt}))
	if err != nil {
		return err
	}
	defer stream.Close()
	for stream.Receive() {
		m := stream.Msg()
		if err := emit(logChunk{m.Data, m.NextOffset, m.Complete, m.Truncated, m.AttemptId, ""}); err != nil {
			return err
		}
	}
	return stream.Err()
}

func hexID(s string) bool {
	if len(s) != 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

type logsView struct {
	ID, Job, Text, Next, Attempt string
	Output                       string
	Truncated, Complete          bool
	Ended                        string
}

// logs renders one bounded page of log output, never following.
func (s *server) logs(w http.ResponseWriter, r *http.Request) {
	q, ok := parseLogQuery(r)
	if !validJobID(q.id) {
		s.notFound(w)
		return
	}
	if !ok {
		s.message(w, http.StatusBadRequest, "invalid log selection", "")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	var data []byte
	var last logChunk
	chunks := 0
	v := logsView{ID: q.id, Job: jobPath(q.id), Text: jobPath(q.id) + "/logs.txt"}
	if text := q.values().Encode(); text != "" {
		v.Text += "?" + text
	}
	err := s.readLogs(ctx, q, func(c logChunk) error {
		if chunks > 0 && len(data)+len(c.data) > maxLogPage {
			return errPageFull
		}
		chunks++
		data = append(data, c.data...)
		last = c
		v.Truncated = v.Truncated || c.truncated
		v.Complete = c.complete
		return nil
	})
	if err != nil && !errors.Is(err, errPageFull) {
		if chunks == 0 {
			s.failure(w, err)
			return
		}
		v.Ended = endReason(err)
	}
	v.Attempt = last.attemptID
	if chunks > 0 && (!v.Complete || errors.Is(err, errPageFull)) {
		next := logQuery{attempt: q.attempt, offset: last.nextOffset}
		if last.cursor != "" {
			next.offset, next.cursor, next.cursorSet = 0, last.cursor, true
		}
		v.Next = jobPath(q.id) + "/logs?" + next.values().Encode()
	}
	v.Output = visible(data)
	s.render(w, http.StatusOK, "logs", "logs "+q.id, v)
}

// logsText writes raw log bytes as inert text/plain. With follow=1 it streams
// until the server completes or ends the stream, the client leaves, the
// dashboard stops, ten minutes pass or 1 MiB is written. Output that is not
// known to be whole ends with "-- ... --" marker lines: one if the server
// truncated it, and one if it is incomplete, capped or ended early. Complete
// output has no marker.
func (s *server) logsText(w http.ResponseWriter, r *http.Request) {
	q, ok := parseLogQuery(r)
	if !validJobID(q.id) {
		s.notFound(w)
		return
	}
	if !ok {
		http.Error(w, "invalid log selection", http.StatusBadRequest)
		return
	}
	limit := s.timeout
	if q.follow = r.URL.Query().Get("follow") == "1"; q.follow {
		select {
		case s.follows <- struct{}{}:
			defer func() { <-s.follows }()
		default:
			http.Error(w, "too many log streams", http.StatusServiceUnavailable)
			return
		}
		limit = s.followLimit
	}
	ctx, cancel := context.WithTimeout(r.Context(), limit)
	defer cancel()
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(limit + 10*time.Second))
	started, written := false, 0
	complete, truncated := false, false
	start := func() {
		if !started {
			started = true
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
		}
	}
	err := s.readLogs(ctx, q, func(c logChunk) error {
		start()
		complete, truncated = c.complete, truncated || c.truncated
		data, full := c.data, false
		if written+len(data) > maxLogText {
			data, full = data[:maxLogText-written], true
		}
		n, err := w.Write(data)
		written += n
		if err != nil {
			return err
		}
		if full {
			return errResponseFull
		}
		return rc.Flush()
	})
	if err != nil && !started && !errors.Is(err, errResponseFull) {
		s.failure(w, err)
		return
	}
	start()
	var marks []string
	if truncated {
		marks = append(marks, "output truncated: retained output limit reached")
	}
	switch {
	case errors.Is(err, errResponseFull):
		marks = append(marks, "response limit reached")
	case err != nil:
		marks = append(marks, "stream ended: "+endReason(err))
	case !complete:
		marks = append(marks, "output incomplete: the job may write more")
	}
	if len(marks) > 0 {
		fmt.Fprint(w, "\n")
		for _, m := range marks {
			fmt.Fprintf(w, "-- %s --\n", m)
		}
	}
}

// endReason names why a stream stopped early, without server message text.
func endReason(err error) string {
	switch {
	case errors.Is(err, errInvalidDomain):
		return "invalid log domain"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	if reason := expiry(err); reason != "" {
		return reason
	}
	var rpc *connect.Error
	if errors.As(err, &rpc) {
		return rpc.Code().String()
	}
	return "error"
}

// visible returns log bytes as text: invalid UTF-8 becomes U+FFFD, C0
// controls other than newline and tab become Control Pictures (ESC is ␛),
// DEL becomes ␡ and C1 controls become \u0080-style escapes.
func visible(b []byte) string {
	var sb strings.Builder
	for len(b) > 0 {
		r, n := utf8.DecodeRune(b)
		b = b[n:]
		switch {
		case r == utf8.RuneError && n == 1:
			sb.WriteRune(utf8.RuneError)
		case r == '\n' || r == '\t':
			sb.WriteRune(r)
		case r < 0x20:
			sb.WriteRune(0x2400 + r)
		case r == 0x7f:
			sb.WriteRune('␡')
		case r >= 0x80 && r < 0xa0:
			fmt.Fprintf(&sb, "\\u%04x", r)
		default:
			sb.WriteRune(r)
		}
	}
	return sb.String()
}
