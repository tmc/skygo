package remotehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"time"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/artifact"
)

func collect(ctx context.Context, c *client, names []string, p paths) (err error) {
	if err = waitRelease(ctx, c); err != nil {
		return err
	}
	root, err := os.OpenRoot(filepath.Join(p.workspace, "work"))
	if err != nil {
		return err
	}
	var archive boundedArchive
	collectErr := artifact.Collect(root, names, &archive)
	collectErr = errors.Join(collectErr, closeOwned(root))
	if collectErr != nil {
		return collectErr
	}
	digest, err := artifact.Validate(bytes.NewReader(archive.Bytes()), int64(archive.Len()))
	if err != nil {
		return err
	}
	hash := sha256.Sum256(archive.Bytes())
	if digest != hex.EncodeToString(hash[:]) {
		return errors.New("output digest mismatch")
	}
	return upload(ctx, c, digest, archive.Bytes())
}

// boundedArchive never retains a partial archive beyond the encoded limit.
type boundedArchive struct{ bytes.Buffer }

func (b *boundedArchive) Write(data []byte) (int, error) {
	if len(data) > (32<<20)-b.Len() {
		return 0, errors.New("output exceeds archive bound")
	}
	return b.Buffer.Write(data)
}
func pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func waitRelease(ctx context.Context, c *client) error {
	// The failure budget spans the whole wait; a WAIT reply does not reset it.
	failures := 0
	for {
		began := time.Now()
		response, err := c.remote.PollRelease(ctx, connect.NewRequest(&skyv1.PollReleaseRequest{}))
		if closeErr := c.takeError(); closeErr != nil {
			return closeErr
		}
		if err != nil {
			failures++
			if !retryable(err) || failures >= 3 {
				return err
			}
			began = time.Now() // A failure pauses a full 2s, as in upload.
		} else {
			m := response.Msg
			switch m.State {
			case skyv1.RemoteReleaseState_REMOTE_RELEASE_STATE_WAIT:
			case skyv1.RemoteReleaseState_REMOTE_RELEASE_STATE_COLLECT:
				if m.Revision == 0 || !containerID(m.TaskContainerId) || m.TaskExitCode == nil || *m.TaskExitCode != 0 {
					return errors.New("invalid collection release")
				}
				return nil
			case skyv1.RemoteReleaseState_REMOTE_RELEASE_STATE_ABORT:
				return errors.New("collection aborted")
			default:
				return errors.New("invalid collection release")
			}
		}
		if err = pause(ctx, time.Until(began.Add(2*time.Second))); err != nil {
			return err
		}
	}
}

// retryable reports whether a failed call may be repeated unchanged. Busy
// (ResourceExhausted) qualifies only when the server sent it: a refusal by the
// local transport policy or read limit is never retried.
func retryable(err error) bool {
	switch connect.CodeOf(err) {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded:
		return true
	case connect.CodeResourceExhausted:
		return connect.IsWireError(err)
	}
	return false
}
func upload(ctx context.Context, c *client, digest string, data []byte) error {
	var begin *skyv1.BeginOutputResponse
	for attempt := 0; attempt < 3; attempt++ {
		response, err := c.remote.BeginOutput(ctx, connect.NewRequest(&skyv1.BeginOutputRequest{Sha256: digest, Size: int64(len(data))}))
		if e := c.takeError(); e != nil {
			return e
		}
		if err == nil {
			begin = response.Msg
			break
		}
		if !retryable(err) || attempt == 2 {
			return err
		}
		if err = pause(ctx, 2*time.Second); err != nil {
			return err
		}
	}
	if begin == nil || begin.Acknowledged < 0 || begin.Acknowledged > int64(len(data)) {
		return errors.New("invalid output prefix")
	}
	switch begin.State {
	case skyv1.RemoteOutputState_REMOTE_OUTPUT_STATE_SEALED:
		if begin.Acknowledged != int64(len(data)) {
			return errors.New("invalid sealed prefix")
		}
		return nil
	case skyv1.RemoteOutputState_REMOTE_OUTPUT_STATE_SEALING:
		if begin.Acknowledged != int64(len(data)) {
			return errors.New("invalid sealing prefix")
		}
	case skyv1.RemoteOutputState_REMOTE_OUTPUT_STATE_WRITABLE:
		offset := begin.Acknowledged
		for offset < int64(len(data)) {
			end := offset + (64 << 10)
			if end > int64(len(data)) {
				end = int64(len(data))
			}
			var ack int64
			for attempt := 0; attempt < 3; attempt++ {
				response, err := c.remote.WriteOutput(ctx, connect.NewRequest(&skyv1.WriteOutputRequest{Offset: offset, Data: data[offset:end]}))
				if e := c.takeError(); e != nil {
					return e
				}
				if err == nil {
					ack = response.Msg.Acknowledged
					break
				}
				if !retryable(err) || attempt == 2 {
					return err
				}
				if err = pause(ctx, 2*time.Second); err != nil {
					return err
				}
			}
			if ack != end {
				return errors.New("invalid output acknowledgement")
			}
			offset = ack
		}
	default:
		return errors.New("invalid output state")
	}
	failures := 0
	for {
		response, err := c.remote.FinishOutput(ctx, connect.NewRequest(&skyv1.FinishOutputRequest{}))
		if e := c.takeError(); e != nil {
			return e
		}
		if err != nil {
			failures++
			if !retryable(err) || failures >= 3 {
				return err
			}
		} else {
			switch response.Msg.State {
			case skyv1.RemoteOutputSealState_REMOTE_OUTPUT_SEAL_STATE_SEALED:
				return nil
			case skyv1.RemoteOutputSealState_REMOTE_OUTPUT_SEAL_STATE_SEALING:
			default:
				return errors.New("invalid output seal")
			}
		}
		if err = pause(ctx, 2*time.Second); err != nil {
			return err
		}
	}
}

func containerID(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}
