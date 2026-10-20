package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/artifact"
)

func (o *options) downloadArtifact(cmd *cobra.Command, ref artifact.Ref, destination string) (result error) {
	if destination == "" {
		return errors.New("download requires a new destination directory")
	}
	if _, err := os.Lstat(destination); err == nil {
		return errors.New("download destination already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
	defer cancel()
	if _, err := o.requireEndpointCapabilities(ctx, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1); err != nil {
		return err
	}
	client := o.artifactClient()
	r, err := client.BeginArtifactDownload(ctx, request(o, &v1.BeginArtifactDownloadRequest{Reference: wireArtifactRef(ref)}))
	if err != nil {
		return err
	}
	if r == nil || r.Msg == nil || !validHexID(r.Msg.Id) {
		return errors.New("server returned an invalid download ID")
	}
	id := r.Msg.Id
	closed := false
	defer func() {
		if !closed {
			// Release our fixed lease even when the caller's deadline expired.
			// This owns no process or background retry loop.
			closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(cmd.Context()), min(o.timeout, 2*time.Second))
			defer closeCancel()
			_, err := client.CloseArtifactDownload(closeCtx, request(o, &v1.CloseArtifactDownloadRequest{DownloadId: id}))
			if err != nil {
				result = errors.Join(result, fmt.Errorf("close artifact download: %w", err))
			}
		}
	}()
	got, err := checkedArtifactRef(r.Msg.Reference)
	if err != nil || got != ref {
		return errors.New("download artifact reference changed")
	}
	if r.Msg.ExpiresAt == nil || r.Msg.ExpiresAt.CheckValid() != nil {
		return errors.New("server returned an invalid download expiry")
	}
	var archive archiveBuffer
	for offset := int64(0); offset < ref.Size; {
		limit := min(int64(artifactChunkSize), ref.Size-offset)
		chunk, err := client.ReadArtifactChunk(ctx, request(o, &v1.ReadArtifactChunkRequest{DownloadId: id, Offset: offset, Limit: int32(limit)}))
		if err != nil {
			return fmt.Errorf("read artifact chunk at %d: %w", offset, err)
		}
		if chunk == nil || chunk.Msg == nil {
			return errors.New("server returned no download chunk")
		}
		p := chunk.Msg
		n := int64(len(p.Data))
		if n != limit || p.NextOffset != offset+n || p.Complete != (p.NextOffset == ref.Size) {
			return errors.New("server returned an invalid or partial download chunk")
		}
		if _, err := archive.Write(p.Data); err != nil {
			return err
		}
		offset = p.NextOffset
	}
	if int64(archive.Len()) != ref.Size {
		return errors.New("download size does not match reference")
	}
	digest, err := artifact.Validate(bytes.NewReader(archive.Bytes()), ref.Size)
	if err != nil {
		return fmt.Errorf("validate downloaded archive: %w", err)
	}
	if digest != ref.SHA256 {
		return errors.New("download digest does not match reference")
	}
	closeResponse, err := client.CloseArtifactDownload(ctx, request(o, &v1.CloseArtifactDownloadRequest{DownloadId: id}))
	if err != nil {
		return fmt.Errorf("close artifact download: %w", err)
	}
	closed = true
	if closeResponse == nil || closeResponse.Msg == nil {
		return errors.New("server returned no download close response")
	}
	// No destination is created before the entire archive and digest are valid.
	// Mkdir is exclusive, including when a competing process created a symlink.
	parent, err := os.OpenRoot(filepath.Dir(destination))
	if err != nil {
		return err
	}
	defer parent.Close()
	name := filepath.Base(destination)
	if err := parent.Mkdir(name, 0700); err != nil {
		return fmt.Errorf("create new download destination: %w", err)
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		return fmt.Errorf("open newly created download destination %q (retained): %w", destination, err)
	}
	defer root.Close()
	// Extract validates again; its independent full digest is checked against
	// the same immutable in-memory archive rather than trusting a prior pass.
	hash := sha256.New()
	if err := artifact.Extract(root, io.TeeReader(bytes.NewReader(archive.Bytes()), hash), ref.Size); err != nil {
		return fmt.Errorf("extract snapshot; private destination %q retained: %w", destination, err)
	}
	if hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
		return fmt.Errorf("extracted snapshot digest changed; private destination %q retained", destination)
	}
	if o.json {
		return writeArtifactRef(cmd.OutOrStdout(), ref)
	}
	_, err = fmt.Fprintln(cmd.OutOrStdout(), destination)
	return err
}
