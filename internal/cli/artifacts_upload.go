package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
)

// archiveBuffer stops the shared collector before it can grow beyond MaxSize.
// The bytes remain private until validation; no partial local archive is exposed.
type archiveBuffer struct{ bytes.Buffer }

func (b *archiveBuffer) Write(p []byte) (int, error) {
	if int64(len(p)) > artifact.MaxSize-int64(b.Len()) {
		return 0, errors.New("artifact archive exceeds 32 MiB")
	}
	return b.Buffer.Write(p)
}

func (o *options) uploadArtifact(cmd *cobra.Command, directory, resume string) error {
	archive, _, err := snapshotDirectory(directory)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
	defer cancel()
	ref, err := o.uploadSnapshot(ctx, archive, resume, cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	return writeArtifactRef(cmd.OutOrStdout(), ref)
}

// snapshotDirectory owns the opened root until snapshotRoot closes it.
func snapshotDirectory(directory string) ([]byte, string, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, "", fmt.Errorf("open snapshot directory: %w", err)
	}
	return snapshotRoot(root)
}

// snapshotRoot consumes root. On error every archive byte is discarded; a
// failed Close is returned with the snapshot error and retains the same root.
func snapshotRoot(root *os.Root) ([]byte, string, error) {
	if root == nil {
		return nil, "", errors.New("nil snapshot root")
	}
	var archive archiveBuffer
	snapshotErr := artifact.Snapshot(root, &archive)
	closeErr := root.Close()
	if closeErr != nil {
		closeErr = &snapshotCloseError{root: root, err: closeErr}
	}
	if err := errors.Join(snapshotErr, closeErr); err != nil {
		return nil, "", fmt.Errorf("snapshot directory: %w", err)
	}
	digest, err := artifact.Validate(bytes.NewReader(archive.Bytes()), int64(archive.Len()))
	if err != nil {
		return nil, "", fmt.Errorf("validate snapshot: %w", err)
	}
	return archive.Bytes(), digest, nil
}

// snapshotCloseError keeps the actual failed owner with its cause. It does not
// assert that Close joined the root or permit a second close on a reused handle.
type snapshotCloseError struct {
	root *os.Root
	err  error
}

func (e *snapshotCloseError) Error() string { return e.err.Error() }
func (e *snapshotCloseError) Unwrap() error { return e.err }

// uploadSnapshot returns the server's exact finalized Ref without rendering it.
// The caller keeps archive immutable through the call. Nil progress discards
// hints; the shared retry budget and original upload identity remain unchanged.
func (o *options) uploadSnapshot(ctx context.Context, archive []byte, resume string, progress io.Writer) (artifact.Ref, error) {
	size := int64(len(archive))
	digest, err := artifact.Validate(bytes.NewReader(archive), size)
	if err != nil {
		return artifact.Ref{}, fmt.Errorf("validate snapshot: %w", err)
	}
	if progress == nil {
		progress = io.Discard
	}
	if _, err := o.requireEndpointCapabilities(ctx, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1); err != nil {
		return artifact.Ref{}, err
	}
	client := o.artifactClient()
	retries := new(artifactUploadRetries)
	var u *v1.ArtifactUpload
	if resume == "" {
		r, err := o.beginArtifactUpload(ctx, client, digest, size, retries)
		if err != nil {
			return artifact.Ref{}, err
		}
		if r != nil && r.Msg != nil {
			u = r.Msg.Upload
		}
	} else {
		r, err := client.GetArtifactUpload(ctx, request(o, &v1.GetArtifactUploadRequest{Id: resume}))
		if err != nil {
			return artifact.Ref{}, err
		}
		if r != nil && r.Msg != nil {
			u = r.Msg.Upload
		}
	}
	if err := checkUpload(u, resume, nil); err != nil {
		return artifact.Ref{}, err
	}
	ref, _ := checkedArtifactRef(u.Reference)
	if ref.SHA256 != digest || ref.Size != size {
		return artifact.Ref{}, errors.New("upload reference does not match the snapshot")
	}
	if _, err := fmt.Fprintf(progress, "upload-id: %s\n", u.Id); err != nil {
		return artifact.Ref{}, err
	}
	finalizing := resume != "" && u.State == v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_FINALIZING
	if u.State != v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE && u.State != v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_COMPLETE && !finalizing {
		return artifact.Ref{}, fmt.Errorf("upload is not writable: %s", u.State)
	}
	// checkUpload requires a full acknowledgment for FINALIZING. Explicit
	// resume retries Finalize on the same upload without beginning or writing
	// any bytes; the server owns its original-inode crash-prefix recovery.
	for u.Acknowledged < size {
		offset := u.Acknowledged
		end := min(size, offset+artifactChunkSize)
		p := archive[offset:end]
		id := u.Id
		u, err = o.writeArtifactChunk(ctx, client, u.Id, ref, offset, p, retries)
		if err != nil {
			return artifact.Ref{}, fmt.Errorf("upload chunk at %d (resume with --resume %s): %w", offset, id, err)
		}
	}
	r, err := o.finalizeArtifactUpload(ctx, client, u.Id, retries)
	if err != nil {
		return artifact.Ref{}, fmt.Errorf("finalize upload (resume with --resume %s): %w", u.Id, err)
	}
	if r == nil || r.Msg == nil {
		return artifact.Ref{}, errors.New("server returned no finalized reference")
	}
	got, err := checkedArtifactRef(r.Msg.Reference)
	if err != nil {
		return artifact.Ref{}, err
	}
	if got != ref {
		return artifact.Ref{}, errors.New("finalized artifact reference changed")
	}
	return got, nil
}

func checkUpload(u *v1.ArtifactUpload, id string, expected *artifact.Ref) error {
	if u == nil || !validHexID(u.Id) || (id != "" && u.Id != id) {
		return errors.New("server returned an invalid upload ID")
	}
	ref, err := checkedArtifactRef(u.Reference)
	if err != nil {
		return err
	}
	if expected != nil && ref != *expected {
		return errors.New("upload artifact reference changed")
	}
	if u.Acknowledged < 0 || u.Acknowledged > ref.Size {
		return errors.New("server returned an invalid acknowledged offset")
	}
	if u.ExpiresAt == nil || u.ExpiresAt.CheckValid() != nil {
		return errors.New("server returned an invalid upload expiry")
	}
	switch u.State {
	case v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_CREATING,
		v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE,
		v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_FINALIZING,
		v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_COMPLETE,
		v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_DELETING:
	default:
		return errors.New("server returned an invalid upload state")
	}
	if (u.State == v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_COMPLETE || u.State == v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_FINALIZING) && u.Acknowledged != ref.Size {
		return errors.New("completed upload has an incomplete acknowledgment")
	}
	return nil
}

// artifactUploadRetries is shared by every stage under the original upload
// deadline. A client retry neither releases a server owner nor reserves space.
type artifactUploadRetries struct{ used int }

func (r *artifactUploadRetries) wait(ctx context.Context, cause error) error {
	if err := ctx.Err(); err != nil {
		return errors.Join(cause, err)
	}
	if r.used >= 20 {
		return fmt.Errorf("artifact upload retry limit reached: %w", cause)
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

func artifactUploadRetryable(err error) bool {
	switch connect.CodeOf(err) {
	case connect.CodeResourceExhausted, connect.CodeUnavailable, connect.CodeDeadlineExceeded:
		return true
	}
	return false
}

func artifactBeginNotStarted(err error) bool {
	var wire *connect.Error
	if !connect.IsWireError(err) || !errors.As(err, &wire) || wire.Code() != connect.CodeResourceExhausted {
		return false
	}
	values := wire.Meta().Values("Skygo-Artifact-Begin-Not-Started")
	return len(values) == 1 && values[0] == "v1"
}

func (o *options) beginArtifactUpload(ctx context.Context, client skyv1connect.ControlServiceClient, digest string, size int64, retries *artifactUploadRetries) (*connect.Response[v1.BeginArtifactUploadResponse], error) {
	for {
		r, err := client.BeginArtifactUpload(ctx, request(o, &v1.BeginArtifactUploadRequest{Sha256: digest, Size: size}))
		if err == nil {
			return r, nil
		}
		if !artifactBeginNotStarted(err) {
			return nil, fmt.Errorf("begin artifact upload outcome may be unknown; automatic retry disabled: %w", err)
		}
		if err := retries.wait(ctx, err); err != nil {
			return nil, fmt.Errorf("begin artifact upload; no upload admitted by marked attempts: %w", err)
		}
	}
}

func (o *options) finalizeArtifactUpload(ctx context.Context, client skyv1connect.ControlServiceClient, id string, retries *artifactUploadRetries) (*connect.Response[v1.FinalizeArtifactUploadResponse], error) {
	for {
		r, err := client.FinalizeArtifactUpload(ctx, request(o, &v1.FinalizeArtifactUploadRequest{Id: id}))
		if err == nil {
			return r, nil
		}
		if !artifactUploadRetryable(err) {
			return nil, err
		}
		if err := retries.wait(ctx, err); err != nil {
			return nil, err
		}
	}
}

func (o *options) writeArtifactChunk(ctx context.Context, client skyv1connect.ControlServiceClient, id string, ref artifact.Ref, offset int64, p []byte, retries *artifactUploadRetries) (*v1.ArtifactUpload, error) {
	// Retain the submitted bytes independently of the caller for every replay.
	p = bytes.Clone(p)
	for {
		r, err := client.WriteArtifactChunk(ctx, request(o, &v1.WriteArtifactChunkRequest{Id: id, Offset: offset, Data: p}))
		if err == nil {
			if r == nil || r.Msg == nil {
				return nil, errors.New("server returned no chunk acknowledgment")
			}
			u := r.Msg.Upload
			if err := checkUpload(u, id, &ref); err != nil {
				return nil, err
			}
			if u.State != v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE || u.Acknowledged != offset+int64(len(p)) {
				return nil, errors.New("server returned a partial or conflicting chunk acknowledgment")
			}
			return u, nil
		}
		if !artifactUploadRetryable(err) {
			return nil, err
		}
		if ctx.Err() != nil {
			return nil, errors.Join(err, ctx.Err())
		}
		if connect.CodeOf(err) != connect.CodeResourceExhausted {
			status, getErr := client.GetArtifactUpload(ctx, request(o, &v1.GetArtifactUploadRequest{Id: id}))
			if getErr != nil {
				return nil, fmt.Errorf("recover ambiguous write: %w", getErr)
			}
			if status == nil || status.Msg == nil {
				return nil, errors.New("server returned no recovery upload")
			}
			u := status.Msg.Upload
			if err := checkUpload(u, id, &ref); err != nil {
				return nil, err
			}
			if u.State != v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE {
				return nil, errors.New("upload state changed during recovery")
			}
			if u.Acknowledged != offset && u.Acknowledged != offset+int64(len(p)) {
				return nil, errors.New("upload acknowledgment conflicts with the submitted chunk")
			}
			// Even a full status acknowledgment requires an identical Write:
			// the server must compare the actual earlier range and acknowledge it.
		}
		if err := retries.wait(ctx, err); err != nil {
			return nil, err
		}
	}
}
