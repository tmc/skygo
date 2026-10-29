package ec2job

import (
	"context"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
)

// MaxLogBytes bounds the log bytes one Logs call reads, and the offset it
// starts at: the server's retained log limit.
const MaxLogBytes = 1 << 20

// A LogPage describes one bounded copy of a job's log.
type LogPage struct {
	JobID      string `json:"job_id"`
	Offset     int64  `json:"offset"`
	NextOffset int64  `json:"next_offset"`
	Bytes      int64  `json:"bytes"`
	Complete   bool   `json:"complete"`
	Truncated  bool   `json:"truncated"`
	AttemptID  string `json:"attempt_id,omitempty"`
}

// Logs copies the known job's log from offset to w, exactly as the server
// sends it, without following: it returns once the server ends the
// stream. Offset is an explicit seek; the attempt read is whatever the
// server selects. A job with retention recorded as enabled is read with
// the retained log stream, any other with the plain one; the choice is
// the journal's, never refreshed. Logs only reads: it never submits, and
// a job without a known ID has no log it can read.
//
// The stream's Close is called once and its error returned with any
// other; truncation is reported, never hidden. Every Write to w must
// write all its bytes; a short write fails the copy at the last offset
// fully written.
//
// Logs does not move the job's log checkpoint: the caller does, with
// [Job.CommitLogs], once it has delivered what w received.
func (j *Job) Logs(ctx context.Context, offset int64, w io.Writer) (LogPage, error) {
	return j.copyLogs(ctx, offset, false, w)
}

// NextLogs copies the job's log as Logs does, from the log checkpoint:
// the offset where the last committed copy ended, or 0. The copy must be
// of the checkpoint's attempt, if it names one; a stream of another
// attempt is refused before any of its bytes is written to w.
func (j *Job) NextLogs(ctx context.Context, w io.Writer) (LogPage, error) {
	if err := j.usable(); err != nil {
		return LogPage{}, err
	}
	return j.copyLogs(ctx, j.st.LogOffset, true, w)
}

// CommitLogs durably moves the log checkpoint to where page, the result
// of the job's last Logs or NextLogs call, ended, keyed to the page's
// attempt. That call must have returned no error, and the caller must
// have delivered every byte it wrote: the checkpoint records delivery,
// not reading.
//
// Writing a log copy and committing it are two operations. A crash
// between them, or a failed commit, leaves the checkpoint where it was,
// and the next NextLogs copies those bytes again: delivery is at least
// once, never exactly once.
func (j *Job) CommitLogs(page LogPage) error {
	if err := j.usable(); err != nil {
		return err
	}
	if err := j.flush(); err != nil {
		return err
	}
	if j.read == nil || *j.read != page {
		return errors.New("ec2job: the page is not the last clean log copy")
	}
	j.read = nil
	// A copy that received no message names no attempt; one continuing
	// the checkpoint keeps the checkpoint's. A continuing copy also keeps
	// the record of skipped bytes; any other copy, an explicit seek,
	// starts a new record.
	attempt := page.AttemptID
	if attempt == "" && page.Offset == j.st.LogOffset {
		attempt = j.st.LogAttempt
	}
	continues := page.Offset == j.st.LogOffset && attempt == j.st.LogAttempt
	next := j.st
	next.LogOffset, next.LogAttempt, next.LogComplete = page.NextOffset, attempt, page.Complete
	next.LogTruncated = page.Truncated || continues && j.st.LogTruncated
	if err := next.check(); err != nil {
		return fmt.Errorf("ec2job: log checkpoint: %w", err)
	}
	return j.commit(next)
}

func (j *Job) copyLogs(ctx context.Context, offset int64, resume bool, w io.Writer) (page LogPage, err error) {
	if err := j.ready(true); err != nil {
		return LogPage{}, err
	}
	j.read = nil
	if j.st.JobID == "" {
		return LogPage{}, errors.New("ec2job: no job ID is known; there is no log to read")
	}
	if offset < 0 || offset > MaxLogBytes {
		return LogPage{}, fmt.Errorf("ec2job: log offset must be in 0..%d", MaxLogBytes)
	}
	page = LogPage{JobID: j.st.JobID, Offset: offset, NextOffset: offset}
	// A copy from the checkpoint must be of its attempt. A plain stream
	// of the default attempt names none.
	want := func(id string) error {
		if resume && id != j.st.LogAttempt && (j.st.LogAttempt != "" || j.st.LogOffset != 0) {
			return fmt.Errorf("%w: the log checkpoint is of another attempt; copy with an explicit offset", ErrMismatch)
		}
		return nil
	}
	if j.st.Retention == RetentionEnabled {
		err = j.retainedLogs(ctx, &page, want, w)
	} else {
		err = j.plainLogs(ctx, &page, want, w)
	}
	if err == nil {
		p := page
		j.read = &p
	}
	return page, err
}

// retainedLogs copies the retained log stream into p. The stream's Close
// error is joined to the result.
func (j *Job) retainedLogs(ctx context.Context, p *LogPage, want func(string) error, w io.Writer) (err error) {
	c := j.session.Control()
	stream, err := c.StreamRetainedJobLogs(ctx, connect.NewRequest(&v1.StreamRetainedJobLogsRequest{Id: p.JobID, Offset: p.Offset}))
	if err != nil {
		return remote("StreamRetainedJobLogs", err)
	}
	defer func() {
		if cerr := stream.Close(); cerr != nil {
			err = errors.Join(err, remote("StreamRetainedJobLogs close", cerr))
		}
	}()
	for stream.Receive() {
		m := stream.Msg()
		if !hexID32.MatchString(m.AttemptId) || p.AttemptID != "" && p.AttemptID != m.AttemptId || len(m.Cursor) > 683 || !utf8.ValidString(m.Cursor) {
			return errors.New("ec2job: server returned an invalid fixed log domain")
		}
		if err := want(m.AttemptId); err != nil {
			return err
		}
		p.AttemptID = m.AttemptId
		if err := p.add(w, m.Data, m.NextOffset, m.Complete, m.Truncated); err != nil {
			return err
		}
	}
	if err := stream.Err(); err != nil {
		return remote("StreamRetainedJobLogs", err)
	}
	return nil
}

// plainLogs copies the plain log stream into p. The stream's Close error
// is joined to the result.
func (j *Job) plainLogs(ctx context.Context, p *LogPage, want func(string) error, w io.Writer) (err error) {
	c := j.session.Control()
	stream, err := c.StreamJobLogs(ctx, connect.NewRequest(&v1.StreamJobLogsRequest{Id: p.JobID, Offset: p.Offset}))
	if err != nil {
		return remote("StreamJobLogs", err)
	}
	defer func() {
		if cerr := stream.Close(); cerr != nil {
			err = errors.Join(err, remote("StreamJobLogs close", cerr))
		}
	}()
	for stream.Receive() {
		m := stream.Msg()
		if m.AttemptId != "" && (!hexID32.MatchString(m.AttemptId) || p.AttemptID != "" && p.AttemptID != m.AttemptId) {
			return errors.New("ec2job: server returned an invalid log attempt")
		}
		if err := want(m.AttemptId); err != nil {
			return err
		}
		p.AttemptID = m.AttemptId
		if err := p.add(w, m.Data, m.NextOffset, m.Complete, m.Truncated); err != nil {
			return err
		}
	}
	if err := stream.Err(); err != nil {
		return remote("StreamJobLogs", err)
	}
	return nil
}

// add writes one message's data and advances p. Offsets must advance by
// exactly the data's length, or further only in a message marked
// truncated, and stay within MaxLogBytes; a message after a complete one
// is refused. p advances only by a write of all of data: a short write
// leaves it at the last message fully written.
func (p *LogPage) add(w io.Writer, data []byte, next int64, complete, truncated bool) error {
	end := p.NextOffset + int64(len(data))
	switch {
	case p.Complete:
		return errors.New("ec2job: server sent log data after completing the stream")
	case next < end || next != end && !truncated || next > MaxLogBytes:
		return errors.New("ec2job: server returned an invalid log offset")
	}
	n, err := w.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return fmt.Errorf("ec2job: write log: %w", err)
	}
	p.NextOffset = next
	p.Bytes += int64(len(data))
	p.Complete = complete
	p.Truncated = p.Truncated || truncated
	return nil
}
