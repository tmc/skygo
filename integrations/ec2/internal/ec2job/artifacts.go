package ec2job

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
)

// ChunkSize is the size of every upload chunk and download read but the
// last: the protocol's 64 KiB bound.
const ChunkSize = 64 << 10

// closeTimeout bounds the one CloseArtifactDownload request a download
// sends after its own context ends.
const closeTimeout = 10 * time.Second

// archiveBuffer refuses to grow beyond artifact.MaxSize.
type archiveBuffer struct{ bytes.Buffer }

func (b *archiveBuffer) Write(p []byte) (int, error) {
	if int64(len(p)) > artifact.MaxSize-int64(b.Len()) {
		return 0, errors.New("artifact archive exceeds 32 MiB")
	}
	return b.Buffer.Write(p)
}

// Snapshot returns the archive of directory dir that skygo's artifact
// package writes, and its hex SHA-256. The archive is at most 32 MiB and
// validated before it is returned. A job freezes it: the job's uploads,
// and any replay, use exactly these bytes, never a new snapshot of dir.
//
// If the archive's Close of one of dir's files is uncertain
// ([artifact.ErrFileCloseUncertain]), Snapshot does not close dir's root:
// the archive's operation on it has not settled. If the root's own first
// Close fails, it is never closed again. Either error wraps
// [ErrUnsettled] and keeps the root, with every cause; the caller must
// keep the error. A failure whose Close succeeded is an ordinary error.
func Snapshot(dir string) (archive []byte, digest string, err error) {
	return snapshotDir(dir, artifact.Snapshot, (*os.Root).Close)
}

// A rootError is a workdir snapshot that did not settle. It keeps the
// workdir's original root reachable while the error is, and the archive's
// own error, which keeps its file. Nothing closes the root again.
type rootError struct {
	root *os.Root
	err  error
}

func (e *rootError) Error() string   { return "snapshot workdir: " + e.err.Error() }
func (e *rootError) Unwrap() []error { return []error{ErrUnsettled, e.err} }

// snapshotDir is Snapshot, archiving with archive and closing the root
// with closeRoot.
func snapshotDir(dir string, archive func(*os.Root, io.Writer) error, closeRoot func(*os.Root) error) ([]byte, string, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, "", fmt.Errorf("open workdir: %w", err)
	}
	var b archiveBuffer
	err = archive(root, &b)
	if errors.Is(err, artifact.ErrFileCloseUncertain) {
		return nil, "", &rootError{root: root, err: err}
	}
	if cerr := closeRoot(root); cerr != nil {
		return nil, "", &rootError{root: root, err: errors.Join(err, fmt.Errorf("close workdir: %w", cerr))}
	}
	if err != nil {
		return nil, "", fmt.Errorf("snapshot workdir: %w", err)
	}
	digest, err := artifact.Validate(bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil {
		return nil, "", fmt.Errorf("validate workdir snapshot: %w", err)
	}
	return b.Bytes(), digest, nil
}

// snapshot reads the frozen snapshot and checks it is the journal's.
func (j *Job) snapshot() ([]byte, error) {
	data, err := j.s.read(workdirName, artifact.MaxSize)
	if err != nil {
		return nil, err
	}
	digest, err := artifact.Validate(bytes.NewReader(data), int64(len(data)))
	if err != nil || digest != j.st.Snapshot.SHA256 || int64(len(data)) != j.st.Snapshot.Size {
		return nil, fmt.Errorf("%w: the frozen workdir snapshot changed", ErrMismatch)
	}
	return data, nil
}

// upload advances the workdir upload until its reference is finalized.
// A Begin is sent at most once per journal unless the server marks it as
// not started. A Begin whose outcome is unknown holds the job: its
// upload ID is lost, and a second Begin would allocate another upload. A
// known upload is reconciled with GetArtifactUpload. Ordinary Busy refusals
// share one bounded retry budget; unknown outcomes still require Resume.
func (j *Job) upload(ctx context.Context, c skyv1connect.ControlServiceClient) error {
	var retries uploadRetries
	if err := ctx.Err(); err != nil {
		return err
	}
	switch j.st.Upload {
	case uploadDone:
		return nil
	case uploadBeginSent:
		return j.hold(HoldUploadBegin)
	}
	archive, err := j.snapshot()
	if err != nil {
		return err
	}
	if j.st.Upload == uploadNone {
		err = j.begin(ctx, c, &retries)
	} else {
		err = j.reconcile(ctx, c)
	}
	if err != nil {
		return err
	}
	size := j.st.Snapshot.Size
	for j.st.Upload == uploadOpen && j.st.Acknowledged < size {
		off := j.st.Acknowledged
		end := min(size, off+ChunkSize)
		r, err := writeUploadChunk(ctx, c, j.st.UploadID, off, archive[off:end], &retries)
		if err != nil {
			return fmt.Errorf("%w (resume reconciles offset %d)", err, off)
		}
		if r == nil || r.Msg == nil {
			return errors.New("ec2job: server returned no chunk acknowledgment")
		}
		u := r.Msg.Upload
		if _, err := j.checkUpload(u); err != nil {
			return err
		}
		if u.State != v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE || u.Acknowledged != end {
			return errors.New("ec2job: server returned a partial or conflicting chunk acknowledgment")
		}
		next := j.st
		next.Acknowledged = end
		if err := j.commit(next); err != nil {
			return err
		}
	}
	if j.st.Upload == uploadOpen {
		next := j.st
		next.Upload = uploadFinalize
		if err := j.commit(next); err != nil {
			return err
		}
	}
	r, err := finalizeUpload(ctx, c, j.st.UploadID, &retries)
	if err != nil {
		return fmt.Errorf("%w (resume reconciles the upload)", err)
	}
	if r == nil || r.Msg == nil || r.Msg.Reference == nil {
		return errors.New("ec2job: server returned no finalized reference")
	}
	got := artifact.Ref{ID: r.Msg.Reference.Id, SHA256: r.Msg.Reference.Sha256, Size: r.Msg.Reference.Size}
	if got != j.st.Workdir {
		return fmt.Errorf("%w: the finalized reference is not the upload's", ErrMismatch)
	}
	next := j.st
	next.Upload = uploadDone
	return j.commit(next)
}

// uploadRetries is shared by every upload stage under the caller's context.
// Waiting grants no server ownership or allocation and changes no journal.
type uploadRetries struct{ used int }

func (r *uploadRetries) wait(ctx context.Context, cause error) error {
	if err := ctx.Err(); err != nil {
		return errors.Join(cause, err)
	}
	if r.used >= 20 {
		return fmt.Errorf("ec2job: upload retry limit reached: %w", cause)
	}
	r.used++
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return errors.Join(cause, ctx.Err())
	case <-timer.C:
		if err := ctx.Err(); err != nil {
			return errors.Join(cause, err)
		}
		return nil
	}
}

func writeUploadChunk(ctx context.Context, c skyv1connect.ControlServiceClient, id string, offset int64, data []byte, retries *uploadRetries) (*connect.Response[v1.WriteArtifactChunkResponse], error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r, err := c.WriteArtifactChunk(ctx, connect.NewRequest(&v1.WriteArtifactChunkRequest{Id: id, Offset: offset, Data: data}))
		if err == nil {
			return r, nil
		}
		if connect.CodeOf(err) != connect.CodeResourceExhausted {
			return nil, remote("WriteArtifactChunk", err)
		}
		if err := retries.wait(ctx, remote("WriteArtifactChunk", err)); err != nil {
			return nil, err
		}
	}
}

func finalizeUpload(ctx context.Context, c skyv1connect.ControlServiceClient, id string, retries *uploadRetries) (*connect.Response[v1.FinalizeArtifactUploadResponse], error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r, err := c.FinalizeArtifactUpload(ctx, connect.NewRequest(&v1.FinalizeArtifactUploadRequest{Id: id}))
		if err == nil {
			return r, nil
		}
		if connect.CodeOf(err) != connect.CodeResourceExhausted {
			return nil, remote("FinalizeArtifactUpload", err)
		}
		if err := retries.wait(ctx, remote("FinalizeArtifactUpload", err)); err != nil {
			return nil, err
		}
	}
}

// begin stores that a Begin may be sent, sends it once and stores the
// returned upload ID and reference before anything else is sent.
func (j *Job) begin(ctx context.Context, c skyv1connect.ControlServiceClient, retries *uploadRetries) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		next := j.st
		next.Upload = uploadBeginSent
		if err := j.commit(next); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := c.BeginArtifactUpload(ctx, connect.NewRequest(&v1.BeginArtifactUploadRequest{Sha256: j.st.Snapshot.SHA256, Size: j.st.Snapshot.Size}))
		if err != nil {
			if beginNotStarted(err) {
				next := j.st
				next.Upload = uploadNone
				if saveErr := j.commit(next); saveErr != nil {
					return errors.Join(remote("BeginArtifactUpload", err), saveErr)
				}
				if waitErr := retries.wait(ctx, remote("BeginArtifactUpload", err)); waitErr != nil {
					return waitErr
				}
				continue
			}
			return errors.Join(remote("BeginArtifactUpload", err), j.hold(HoldUploadBegin))
		}
		var u *v1.ArtifactUpload
		if r != nil && r.Msg != nil {
			u = r.Msg.Upload
		}
		ref, err := j.checkUpload(u)
		if err != nil || (u.State != v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE && u.State != v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_COMPLETE) {
			// An upload may exist under an ID this reply does not establish.
			return errors.Join(errors.New("ec2job: server returned an invalid upload"), j.hold(HoldUploadBegin))
		}
		next = j.st
		next.Upload, next.UploadID, next.Workdir, next.Acknowledged = uploadOpen, u.Id, ref, u.Acknowledged
		return j.commit(next)
	}
}

// reconcile reads the known upload and accepts its acknowledged offset
// only if it is the stored offset or the end of the one chunk that may
// have been in flight.
func (j *Job) reconcile(ctx context.Context, c skyv1connect.ControlServiceClient) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r, err := c.GetArtifactUpload(ctx, connect.NewRequest(&v1.GetArtifactUploadRequest{Id: j.st.UploadID}))
	if err != nil {
		return remote("GetArtifactUpload", err)
	}
	var u *v1.ArtifactUpload
	if r != nil && r.Msg != nil {
		u = r.Msg.Upload
	}
	if _, err := j.checkUpload(u); err != nil {
		return err
	}
	size, stored := j.st.Snapshot.Size, j.st.Acknowledged
	inFlight := min(size, stored+ChunkSize)
	switch {
	case u.Acknowledged != stored && u.Acknowledged != inFlight:
		return fmt.Errorf("%w: the upload's acknowledged offset conflicts with the journal", ErrMismatch)
	case u.State == v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE:
	case u.State == v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_FINALIZING, u.State == v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_COMPLETE:
		// checkUpload required the full acknowledgment.
	default:
		return errors.New("ec2job: the upload is not writable")
	}
	if u.Acknowledged != stored {
		next := j.st
		next.Acknowledged = u.Acknowledged
		return j.commit(next)
	}
	return nil
}

// checkUpload checks an upload the server returned against the journal:
// its ID, if one is stored, its reference against the snapshot and, if
// stored, the upload's reference, its acknowledgment and its state.
func (j *Job) checkUpload(u *v1.ArtifactUpload) (artifact.Ref, error) {
	if u == nil || !hexID32.MatchString(u.Id) || (j.st.UploadID != "" && u.Id != j.st.UploadID) {
		return artifact.Ref{}, errors.New("ec2job: server returned an invalid upload ID")
	}
	if u.Reference == nil {
		return artifact.Ref{}, errors.New("ec2job: server returned no upload reference")
	}
	ref := artifact.Ref{ID: u.Reference.Id, SHA256: u.Reference.Sha256, Size: u.Reference.Size}
	switch {
	case ref.Validate() != nil:
		return artifact.Ref{}, errors.New("ec2job: server returned an invalid upload reference")
	case ref.SHA256 != j.st.Snapshot.SHA256 || ref.Size != j.st.Snapshot.Size:
		return artifact.Ref{}, fmt.Errorf("%w: the upload is not of the frozen snapshot", ErrMismatch)
	case j.st.Workdir != (artifact.Ref{}) && ref != j.st.Workdir:
		return artifact.Ref{}, fmt.Errorf("%w: the upload's reference changed", ErrMismatch)
	case u.Acknowledged < 0 || u.Acknowledged > ref.Size:
		return artifact.Ref{}, errors.New("ec2job: server returned an invalid acknowledged offset")
	case u.ExpiresAt == nil || u.ExpiresAt.CheckValid() != nil:
		return artifact.Ref{}, errors.New("ec2job: server returned an invalid upload expiry")
	case (u.State == v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_COMPLETE || u.State == v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_FINALIZING) && u.Acknowledged != ref.Size:
		return artifact.Ref{}, errors.New("ec2job: server returned a completed upload with an incomplete acknowledgment")
	}
	return ref, nil
}

// needsCleanup reports whether the job is canceled, was never submitted,
// and has a known upload the server has not been seen to mark for
// deletion. No submit can own that upload: none was sent.
func (j *Job) needsCleanup() bool {
	return j.st.Desired == desiredCancel && j.st.Submit == submitNone &&
		(j.st.Upload == uploadOpen || j.st.Upload == uploadFinalize) && j.st.Cleanup != cleanupDelivered
}

// cancelUpload asks the server to delete the known upload with
// CancelArtifactUpload. It first stores that the request may be sent; a
// later explicit call sends the same upload ID again, which the server
// accepts for an upload it already marked. A nil reply means the server
// marked the upload for deletion, which its own maintenance completes
// later; it is not a receipt that the bytes are gone. Any failure, an
// ended context included, leaves the outcome unknown and is never taken
// as cleanup. A finalized reference is not an upload and is kept.
func (j *Job) cancelUpload(ctx context.Context, c skyv1connect.ControlServiceClient) error {
	if j.st.Cleanup == cleanupNone {
		next := j.st
		next.Cleanup = cleanupSent
		if err := j.commit(next); err != nil {
			return err
		}
	}
	if _, err := c.CancelArtifactUpload(ctx, connect.NewRequest(&v1.CancelArtifactUploadRequest{Id: j.st.UploadID})); err != nil {
		return fmt.Errorf("%w (cleanup outcome unknown; cancel again sends the same request)", remote("CancelArtifactUpload", err))
	}
	next := j.st
	next.Cleanup = cleanupDelivered
	return j.commit(next)
}

// beginNotStarted reports whether err is the server's marked refusal of a
// Begin that admitted no upload, the one Begin failure that is not in
// doubt.
func beginNotStarted(err error) bool {
	var wire *connect.Error
	if !connect.IsWireError(err) || !errors.As(err, &wire) || wire.Code() != connect.CodeResourceExhausted {
		return false
	}
	values := wire.Meta().Values("Skygo-Artifact-Begin-Not-Started")
	return len(values) == 1 && values[0] == "v1"
}

// errResult reports a job observation whose result this package cannot
// read. Nothing is recorded for it; a later observation is collected.
var errResult = errors.New("ec2job: server returned no valid result; nothing was recorded and resume collects it later")

// collect records the ended job's result. A task without outputs, or a
// job never submitted, has none. Otherwise the result is the one the
// server reports for a workspace job: always present, of the job's last
// attempt, or of the job itself when it has no attempt, and in one of
// the explicit states AVAILABLE, ABSENT, EXPIRED or HELD. An available
// result is downloaded (see [Job.download]); an explicitly absent or
// expired one is recorded as such; a held one is not recorded, and a
// later Resume collects it. A missing result, one of another attempt, or
// one whose state is UNSPECIFIED or unknown is refused without recording
// anything or sending any request.
//
// A download an earlier call left open, or whose Begin or Close is in
// doubt, is settled first (see [Job.settleDownload]).
func (j *Job) collect(ctx context.Context, c skyv1connect.ControlServiceClient, job *v1.Job) error {
	t, err := decodeTask(j.st.Task)
	if err != nil {
		return err
	}
	if err := j.settleDownload(ctx, c); err != nil {
		return err
	}
	if j.st.Output != outputNone {
		// The settled download was this Job's unsent Begin: it saved the output.
		return nil
	}
	next := j.st
	if len(t.Outputs) == 0 || j.st.Observed == "NEVER_SUBMITTED" {
		next.Output = outputNotUsed
		return j.commit(next)
	}
	res := job.GetResult()
	attempt := job.GetId()
	if n := len(job.GetAttempts()); n > 0 {
		attempt = job.Attempts[n-1].GetId()
	}
	if res == nil || !hexID32.MatchString(res.AttemptId) || res.AttemptId != attempt {
		return errResult
	}
	switch res.State {
	case v1.ArtifactResultState_ARTIFACT_RESULT_STATE_AVAILABLE:
		w := res.GetReference()
		ref := artifact.Ref{ID: w.GetId(), SHA256: w.GetSha256(), Size: w.GetSize()}
		if w == nil || ref.Validate() != nil {
			return errResult
		}
		return j.saveOutput(ctx, c, ref, res.AttemptId)
	case v1.ArtifactResultState_ARTIFACT_RESULT_STATE_HELD:
		return errors.New("ec2job: the server holds the result; collect it later with resume")
	case v1.ArtifactResultState_ARTIFACT_RESULT_STATE_EXPIRED:
		if res.Reference != nil {
			return errResult
		}
		next.Output = outputExpired
	case v1.ArtifactResultState_ARTIFACT_RESULT_STATE_ABSENT:
		if res.Reference != nil {
			return errResult
		}
		next.Output = outputAbsent
	default:
		return errResult
	}
	return j.commit(next)
}

// saveOutput records the result ref of attempt as saved once output.tar
// holds it. A file an earlier download froze, whose download was then
// closed, is accepted only if it verifies as exactly ref; otherwise the
// result is downloaded.
func (j *Job) saveOutput(ctx context.Context, c skyv1connect.ControlServiceClient, ref artifact.Ref, attempt string) error {
	if _, err := os.Lstat(j.s.path(outputName)); errors.Is(err, fs.ErrNotExist) {
		return j.download(ctx, c, ref, attempt)
	}
	if j.st.Download != downloadNone && (j.st.DownloadRef != ref || j.st.DownloadAttempt != attempt) {
		return fmt.Errorf("%w: a saved output is of another download than the job's result", ErrMismatch)
	}
	data, err := j.s.read(outputName, artifact.MaxSize)
	if err != nil {
		return err
	}
	if _, err := artifact.Verify(ref, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("%w: a saved output is not the job's result", ErrMismatch)
	}
	next := j.st
	next.Output, next.OutputRef, next.OutputAttempt = outputSaved, ref, attempt
	return j.commit(next)
}

// download downloads the result ref of attempt with the server's one
// download protocol, keeping custody of the server's download in the
// journal. BeginArtifactDownload reserves one of the server's 32
// download descriptors before it opens the result, and may keep it even
// when it fails; it has no request ID and no read-only lookup, and
// CloseArtifactDownload is the one release of a known ID.
//
//   - Before the Begin, the journal records that it may be sent, bound to
//     ref and attempt. A Begin that fails, whose reply is lost, or whose
//     reply holds no valid ID holds the job ([HoldDownloadBegin]): no
//     other Begin is ever sent for it.
//   - A valid returned ID is stored before anything else, even when the
//     reply's reference or expiry is invalid. If that save fails, the ID
//     stays pending and no request is sent until it is stored.
//   - A reply whose reference is not ref or whose expiry is invalid is
//     refused; its download is not read, only closed.
//   - Otherwise the result is read in bounded chunks, verified as ref, and
//     frozen durably as output.tar.
//   - The download is then closed, whatever happened: the journal records
//     that a Close may be sent, then sends it once, after ctx ends too,
//     bounded by its own timeout. Only a nil reply is recorded as the
//     server's release. Any failed Close holds the job
//     ([HoldDownloadClose]); it is never sent again, and no expiry,
//     NotFound, refusal or cancellation is taken as a release.
//   - The output is recorded saved, with ref and attempt, only after the
//     release is recorded: output.tar may exist while the job is held,
//     and is then not reported as the job's output.
//
// A read or freeze error and a Close error are both returned. After a
// recorded release that did not save the output, a later call may begin
// a new download.
//
// If the save of the Begin's or the Close's intent fails, that request is
// not sent, and this Job remembers that it was not (see [Job.unsent]):
// once a later call stores the intent, this Job sends it, exactly once. A
// reopened journal has no such proof and holds the intent.
func (j *Job) download(ctx context.Context, c skyv1connect.ControlServiceClient, ref artifact.Ref, attempt string) error {
	next := j.st
	next.Download, next.DownloadID, next.DownloadRef, next.DownloadAttempt = downloadBegin, "", ref, attempt
	if err := j.commit(next); err != nil {
		j.unsent = frameOf(j.st)
		return err
	}
	return j.fetch(ctx, c)
}

// fetch sends the stored download Begin once, for the stored reference
// and attempt, and completes the download.
func (j *Job) fetch(ctx context.Context, c skyv1connect.ControlServiceClient) error {
	j.unsent = frame{}
	ref, attempt := j.st.DownloadRef, j.st.DownloadAttempt
	r, err := c.BeginArtifactDownload(ctx, connect.NewRequest(&v1.BeginArtifactDownloadRequest{Reference: &v1.ArtifactRef{Id: ref.ID, Sha256: ref.SHA256, Size: ref.Size}}))
	if err != nil {
		return errors.Join(remote("BeginArtifactDownload", err), j.hold(HoldDownloadBegin))
	}
	var m *v1.BeginArtifactDownloadResponse
	if r != nil {
		m = r.Msg
	}
	if m == nil || !hexID32.MatchString(m.Id) {
		// A download may exist under an ID this reply does not establish.
		return errors.Join(errors.New("ec2job: server returned no valid download ID"), j.hold(HoldDownloadBegin))
	}
	next := j.st
	next.Download, next.DownloadID = downloadOpen, m.Id
	if err := j.commit(next); err != nil {
		return err
	}
	err = checkDownload(m, ref)
	var data []byte
	if err == nil {
		data, err = j.readDownload(ctx, c, ref)
	}
	if err == nil {
		err = j.s.freeze(outputName, data)
	}
	if cerr := j.closeDownload(ctx, c); cerr != nil {
		return errors.Join(err, cerr)
	}
	if err != nil {
		return err
	}
	next = j.st
	next.Output, next.OutputRef, next.OutputAttempt = outputSaved, ref, attempt
	return j.commit(next)
}

// checkDownload checks a Begin reply's reference and expiry against ref.
func checkDownload(m *v1.BeginArtifactDownloadResponse, ref artifact.Ref) error {
	got := m.Reference
	if got == nil || got.Id != ref.ID || got.Sha256 != ref.SHA256 || got.Size != ref.Size {
		return fmt.Errorf("%w: the download's reference is not the result's", ErrMismatch)
	}
	if m.ExpiresAt == nil || m.ExpiresAt.CheckValid() != nil {
		return errors.New("ec2job: server returned an invalid download expiry")
	}
	return nil
}

// readDownload reads the known download of ref in bounded chunks and
// verifies it. It never retries a request.
func (j *Job) readDownload(ctx context.Context, c skyv1connect.ControlServiceClient, ref artifact.Ref) ([]byte, error) {
	var b archiveBuffer
	for off := int64(0); off < ref.Size; {
		limit := min(int64(ChunkSize), ref.Size-off)
		chunk, err := c.ReadArtifactChunk(ctx, connect.NewRequest(&v1.ReadArtifactChunkRequest{DownloadId: j.st.DownloadID, Offset: off, Limit: int32(limit)}))
		if err != nil {
			return nil, remote("ReadArtifactChunk", err)
		}
		if chunk == nil || chunk.Msg == nil {
			return nil, errors.New("ec2job: server returned no download chunk")
		}
		p := chunk.Msg
		n := int64(len(p.Data))
		if n != limit || p.NextOffset != off+n || p.Complete != (p.NextOffset == ref.Size) {
			return nil, errors.New("ec2job: server returned an invalid or partial download chunk")
		}
		if _, err := b.Write(p.Data); err != nil {
			return nil, err
		}
		off = p.NextOffset
	}
	if _, err := artifact.Verify(ref, bytes.NewReader(b.Bytes())); err != nil {
		return nil, fmt.Errorf("ec2job: downloaded output does not verify: %w", err)
	}
	return b.Bytes(), nil
}

// closeDownload closes the known download once: it stores that the Close
// may be sent, sends it, and stores the release only on a nil reply. Any
// failure of the request holds the job. The request has its own timeout
// and is sent after ctx ends too. If the intent's save fails, nothing is
// sent and this Job remembers it.
func (j *Job) closeDownload(ctx context.Context, c skyv1connect.ControlServiceClient) error {
	next := j.st
	next.Download = downloadClose
	if err := j.commit(next); err != nil {
		j.unsent = frameOf(j.st)
		return err
	}
	return j.sendClose(ctx, c)
}

// sendClose sends the stored download Close once.
func (j *Job) sendClose(ctx context.Context, c skyv1connect.ControlServiceClient) error {
	j.unsent = frame{}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()
	if _, err := c.CloseArtifactDownload(cctx, connect.NewRequest(&v1.CloseArtifactDownloadRequest{DownloadId: j.st.DownloadID})); err != nil {
		return errors.Join(remote("CloseArtifactDownload", err), j.hold(HoldDownloadClose))
	}
	next := j.st
	next.Download = downloadClosed
	return j.commit(next)
}

// settleDownload settles a download an earlier call left: a Begin or a
// Close whose outcome is unknown holds the job, and a download whose ID
// is known but was never closed is closed, without being read. A Begin or
// Close this Job stored but never sent is sent now, once. A closed
// download needs nothing.
func (j *Job) settleDownload(ctx context.Context, c skyv1connect.ControlServiceClient) error {
	switch {
	case j.st.Download == downloadBegin && j.unsentFrame():
		return j.fetch(ctx, c)
	case j.st.Download == downloadClose && j.unsentFrame():
		return j.sendClose(ctx, c)
	case j.st.Download == downloadBegin:
		return j.hold(HoldDownloadBegin)
	case j.st.Download == downloadClose:
		return j.hold(HoldDownloadClose)
	case j.st.Download == downloadOpen:
		return j.closeDownload(ctx, c)
	}
	return nil
}

// A frame is one download request this Job stored the intent of: its
// stage (downloadBegin or downloadClose) and the download it is for.
type frame struct {
	stage   string
	id      string
	ref     artifact.Ref
	attempt string
}

func frameOf(st state) frame {
	return frame{st.Download, st.DownloadID, st.DownloadRef, st.DownloadAttempt}
}

// unsentFrame reports whether the journal's download intent is exactly
// the one this Job failed to save and so never sent. Only this Job, while
// open, knows that: a reopened journal's intent may have been sent.
func (j *Job) unsentFrame() bool {
	return j.unsent != (frame{}) && j.unsent == frameOf(j.st)
}
