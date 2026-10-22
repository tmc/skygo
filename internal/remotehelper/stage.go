package remotehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strconv"
	"time"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/artifact"
)

func stage(ctx context.Context, c *client, env map[string]string, p paths) (err error) {
	size, err := strconv.ParseInt(env["SKYGO_DATA_INPUT_SIZE"], 10, 64)
	if err != nil || strconv.FormatInt(size, 10) != env["SKYGO_DATA_INPUT_SIZE"] {
		return errConfiguration
	}
	ref := artifact.Ref{ID: env["SKYGO_DATA_INPUT_ID"], SHA256: env["SKYGO_DATA_INPUT_SHA256"], Size: size}
	if ref.Validate() != nil || !lowerHex(env["SKYGO_DATA_HELPER_SHA256"], 64) {
		return errConfiguration
	}
	archive, err := download(ctx, c, ref)
	if err != nil {
		return err
	}
	if _, err = artifact.Verify(ref, bytes.NewReader(archive)); err != nil {
		return err
	}
	workspace, err := os.OpenRoot(p.workspace)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeOwned(workspace)) }()
	for _, name := range []string{".stage", "work"} {
		if _, e := workspace.Lstat(name); !errors.Is(e, os.ErrNotExist) {
			return errors.New("workspace is not fresh")
		}
	}
	if err = workspace.Mkdir(".stage", 0700); err != nil {
		return err
	}
	extracted, err := workspace.OpenRoot(".stage")
	if err != nil {
		return err
	}
	hash := sha256.New()
	extractErr := artifact.Extract(extracted, io.TeeReader(bytes.NewReader(archive), hash), ref.Size)
	extractErr = errors.Join(extractErr, closeOwned(extracted))
	if extractErr != nil {
		return extractErr
	}
	if hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
		return errors.New("input digest changed")
	}
	if err = copyHelper(p, env["SKYGO_DATA_HELPER_SHA256"]); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	parent, err := workspace.Open(".")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeOwned(parent)) }()
	if err = publish(int(parent.Fd())); err != nil {
		return err
	}
	return parent.Sync()
}
func download(ctx context.Context, c *client, ref artifact.Ref) ([]byte, error) {
	data := make([]byte, 0, int(ref.Size))
	var offset int64
	for offset < ref.Size {
		limit := int64(64 << 10)
		if ref.Size-offset < limit {
			limit = ref.Size - offset
		}
		var m *skyv1.ReadInputResponse
		for attempt := 0; attempt < 3; attempt++ {
			response, err := c.remote.ReadInput(ctx, connect.NewRequest(&skyv1.ReadInputRequest{Offset: offset, Limit: int32(limit)}))
			if closeErr := c.takeError(); closeErr != nil {
				return nil, closeErr
			}
			if err == nil {
				m = response.Msg
				break
			}
			if !retryable(err) || attempt == 2 {
				return nil, err
			}
			if err = pause(ctx, 2*time.Second); err != nil {
				return nil, err
			}
		}
		if m.Reference == nil || m.Reference.Id != ref.ID || m.Reference.Sha256 != ref.SHA256 || m.Reference.Size != ref.Size || len(m.Data) == 0 || int64(len(m.Data)) > limit || m.NextOffset != offset+int64(len(m.Data)) || m.Complete != (m.NextOffset == ref.Size) {
			return nil, errors.New("invalid input response")
		}
		data = append(data, m.Data...)
		offset = m.NextOffset
	}
	return data, nil
}
func copyHelper(p paths, want string) (err error) {
	before, err := os.Lstat(p.self)
	if err != nil || !before.Mode().IsRegular() {
		return errors.New("invalid helper executable")
	}
	source, err := os.Open(p.self)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeOwned(source)) }()
	info, err := source.Stat()
	if err != nil || !os.SameFile(before, info) || info.Size() < 1 || info.Size() > 128<<20 {
		return errors.New("invalid helper executable")
	}
	root, err := os.OpenRoot(p.helper)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeOwned(root)) }()
	if err = root.Mkdir("bin", 0755); err != nil {
		return err
	}
	target, err := root.OpenFile("bin/skygo-data", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0555)
	if err != nil {
		return err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(target, hash), io.LimitReader(source, (128<<20)+1))
	if copyErr == nil && (written != info.Size() || hex.EncodeToString(hash.Sum(nil)) != want) {
		copyErr = errors.New("helper digest mismatch")
	}
	if copyErr == nil {
		copyErr = target.Chmod(0555)
	}
	if copyErr == nil {
		copyErr = target.Sync()
	}
	return errors.Join(copyErr, closeOwned(target))
}
func lowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
