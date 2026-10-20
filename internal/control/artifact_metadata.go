package control

import (
	"context"
	"fmt"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/store"
)

// GetArtifactUpload returns only the caller's current durable upload metadata.
// It performs no filesystem work and does not acquire a file operation slot.
func (c *Control) GetArtifactUpload(ctx context.Context, actor auth.Actor, id string) (store.ArtifactUpload, error) {
	actor, err := c.engine.Check(actor, auth.Read)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	if err = ctx.Err(); err != nil {
		return store.ArtifactUpload{}, err
	}
	if !jobName.MatchString(id) {
		return store.ArtifactUpload{}, fmt.Errorf("%w: invalid upload id", ErrInvalid)
	}
	if c.artifacts == nil {
		return store.ArtifactUpload{}, ErrUnsupported
	}
	upload, err := c.store.GetArtifactUpload(actor, id)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	if _, err = c.artifactActor(ctx, actor, auth.Read); err != nil {
		return store.ArtifactUpload{}, err
	}
	return upload, nil
}

// CancelArtifactUpload marks an owned incomplete upload for cleanup. It neither
// unlinks bytes nor releases charges. The sole maintenance owner must wait for
// actual in-flight operation and HTTP owners before applying the durable intent.
func (c *Control) CancelArtifactUpload(ctx context.Context, actor auth.Actor, call *ArtifactCall, id string) error {
	actor, upload, _, end, err := c.uploadForIO(ctx, actor, call, id)
	if err != nil {
		return err
	}
	defer end()
	actor, err = c.artifactActor(ctx, actor, auth.Mutate)
	if err != nil {
		return err
	}
	if upload.State == "deleting" {
		return nil
	}
	_, err = c.store.CancelArtifactUpload(actor, store.ArtifactCursor{ID: id, Revision: upload.Revision})
	return err
}

// RemoveArtifact marks only an unconsumed owned artifact for eventual cleanup.
// It cannot bypass a durable consumer, reader or deletion-not-before floor.
func (c *Control) RemoveArtifact(ctx context.Context, actor auth.Actor, ref artifact.Ref) error {
	actor, err := c.engine.Check(actor, auth.Mutate)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if c.artifacts == nil {
		return ErrUnsupported
	}
	converted, err := artifactStoreRef(ref)
	if err != nil {
		return err
	}
	_, err = c.store.RemoveArtifact(actor, converted)
	return err
}

func artifactStoreRef(ref artifact.Ref) (store.ArtifactRef, error) {
	if err := ref.Validate(); err != nil {
		return store.ArtifactRef{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	return store.ArtifactRef{ID: ref.ID, SHA256: ref.SHA256, Size: uint64(ref.Size)}, nil
}
func artifactPortableRef(ref store.ArtifactRef) (artifact.Ref, error) {
	if ref.Size == 0 || ref.Size > 32<<20 {
		return artifact.Ref{}, ErrUnavailable
	}
	out := artifact.Ref{ID: ref.ID, SHA256: ref.SHA256, Size: int64(ref.Size)}
	if err := out.Validate(); err != nil {
		return artifact.Ref{}, ErrUnavailable
	}
	return out, nil
}
