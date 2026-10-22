package control

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/store"
)

// artifactBeginNotStartedError is constructed only by the growth guard before
// Store.BeginArtifactUpload. It is advice about this invocation, not a lease.
type artifactBeginNotStartedError struct{ cause error }

func (e *artifactBeginNotStartedError) Error() string { return e.cause.Error() }
func (e *artifactBeginNotStartedError) Unwrap() error { return e.cause }

// IsArtifactBeginNotStarted reports the exact pre-Store.Begin guard refusal.
// RPC may attach non-admission advice only when converting this Begin error.
func IsArtifactBeginNotStarted(err error) bool {
	var refused *artifactBeginNotStartedError
	return errors.As(err, &refused)
}

func (c *Control) artifactActor(ctx context.Context, actor auth.Actor, permission auth.Action) (auth.Actor, error) {
	actor, err := c.engine.Check(actor, permission)
	if err != nil {
		return auth.Actor{}, err
	}
	if err = ctx.Err(); err != nil {
		return auth.Actor{}, err
	}
	return actor, nil
}

// uploadForIO authorizes owner identity before consulting the shared upload
// operation map, so a foreign ID never exposes whether that upload is busy.
func (c *Control) uploadForIO(ctx context.Context, actor auth.Actor, call *ArtifactCall, id string) (auth.Actor, store.ArtifactUpload, store.ArtifactFile, func(), error) {
	actor, err := c.artifactActor(ctx, actor, auth.Mutate)
	if err != nil {
		return auth.Actor{}, store.ArtifactUpload{}, store.ArtifactFile{}, nil, err
	}
	if !jobName.MatchString(id) {
		return auth.Actor{}, store.ArtifactUpload{}, store.ArtifactFile{}, nil, ErrInvalid
	}
	if c.artifacts == nil {
		return auth.Actor{}, store.ArtifactUpload{}, store.ArtifactFile{}, nil, ErrUnsupported
	}
	u, err := c.store.GetArtifactUpload(actor, id)
	if err != nil {
		return auth.Actor{}, store.ArtifactUpload{}, store.ArtifactFile{}, nil, err
	}
	end, err := c.beginArtifactCall(call, id)
	if err != nil {
		return auth.Actor{}, store.ArtifactUpload{}, store.ArtifactFile{}, nil, err
	}
	u, file, err := c.store.GetArtifactUploadFile(actor, store.ArtifactCursor{ID: id, Revision: u.Revision})
	if err == nil {
		actor, err = c.artifactActor(ctx, actor, auth.Mutate)
	}
	if err != nil {
		end()
		return auth.Actor{}, store.ArtifactUpload{}, store.ArtifactFile{}, nil, err
	}
	return actor, u, file, end, nil
}

// WriteArtifactChunk syncs a new contiguous range or verifies an entirely
// acknowledged earlier range. It never acknowledges a partial or failed write.
// call.Done belongs to the enclosing response owner, not this method.
func (c *Control) WriteArtifactChunk(ctx context.Context, actor auth.Actor, call *ArtifactCall, id string, offset int64, data []byte) (store.ArtifactUpload, error) {
	actor, u, file, end, err := c.uploadForIO(ctx, actor, call, id)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	defer end()
	ref, err := artifactPortableRef(u.Reference)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	if u.State != "writable" || file.Device == 0 || file.Inode == 0 {
		return store.ArtifactUpload{}, store.ErrArtifactConflict
	}
	if !time.Now().Before(u.ExpiresAt) {
		return store.ArtifactUpload{}, store.ErrArtifactExpired
	}
	if offset < 0 || len(data) < 1 || len(data) > 64<<10 || offset > ref.Size || int64(len(data)) > ref.Size-offset || uint64(offset) > u.Acknowledged || uint64(offset) < u.Acknowledged && uint64(len(data)) > u.Acknowledged-uint64(offset) {
		return store.ArtifactUpload{}, fmt.Errorf("%w: invalid artifact chunk range", ErrInvalid)
	}
	payload := bytes.Clone(data)
	identity := artifact.FileID{Device: file.Device, Inode: file.Inode}
	// Repair of the already-owned prefix precedes the growth guard, so retained
	// allocation or an interrupted trim cannot hold the call that removes it.
	// RecoverPart never appends, and its trim is no release credit: only a new
	// physical sample can clear an accounting hold.
	if err = c.artifacts.files.RecoverPart(ref, identity, int64(u.Acknowledged)); err != nil {
		return store.ArtifactUpload{}, c.artifactFileError(call, err)
	}
	actor, err = c.artifactActor(ctx, actor, auth.Mutate)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	// Settled same-inode writes and acknowledgement revisions do not change the
	// prepaid identity/allowance cohort. Every new append still checks freshness.
	if uint64(offset) == u.Acknowledged {
		if err = c.cachedLinkGrowthCheck(); err != nil {
			return store.ArtifactUpload{}, err
		}
	}
	receipt, err := c.artifacts.files.WritePart(ref, identity, int64(u.Acknowledged), offset, payload)
	if err != nil {
		return store.ArtifactUpload{}, c.artifactFileError(call, err)
	}
	if receipt.File != identity || receipt.Offset != offset || receipt.Size != int64(len(payload)) {
		c.physicalChanged()
		return store.ArtifactUpload{}, ErrUnavailable
	}
	actor, err = c.artifactActor(ctx, actor, auth.Mutate)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	cursor := store.ArtifactCursor{ID: id, Revision: u.Revision}
	if uint64(offset) < u.Acknowledged {
		// Earlier wholly acknowledged ranges do not advance/revise durable ack.
		latest, latestFile, err := c.store.GetArtifactUploadFile(actor, cursor)
		if err != nil {
			return store.ArtifactUpload{}, err
		}
		if latestFile != file || latest.State != "writable" || latest.Acknowledged != u.Acknowledged || !time.Now().Before(latest.ExpiresAt) {
			return store.ArtifactUpload{}, store.ErrArtifactConflict
		}
		actor, err = c.artifactActor(ctx, actor, auth.Mutate)
		if err != nil {
			return store.ArtifactUpload{}, err
		}
		return latest, nil
	}
	return c.store.ArtifactChunkSynced(actor, cursor, file, uint64(offset), uint64(len(payload)), receipt.SHA256)
}

// FinalizeArtifactUpload returns the same complete reference on replay, or
// publishes only after the original Files seal and fresh authorized Store CAS.
// A failed or revoked publication retains its durable publishing protection.
func (c *Control) FinalizeArtifactUpload(ctx context.Context, actor auth.Actor, call *ArtifactCall, id string) (artifact.Ref, error) {
	actor, u, file, end, err := c.uploadForIO(ctx, actor, call, id)
	if err != nil {
		return artifact.Ref{}, err
	}
	defer end()
	ref, err := artifactPortableRef(u.Reference)
	if err != nil {
		return artifact.Ref{}, err
	}
	if u.State == "complete" {
		return ref, nil
	}
	if !time.Now().Before(u.ExpiresAt) {
		return artifact.Ref{}, store.ErrArtifactExpired
	}
	if u.Acknowledged != u.Reference.Size || file.Device == 0 || file.Inode == 0 {
		return artifact.Ref{}, store.ErrArtifactConflict
	}
	if u.State != "writable" && u.State != "finalizing" {
		return artifact.Ref{}, store.ErrArtifactConflict
	}
	cursor := store.ArtifactCursor{ID: id, Revision: u.Revision}
	actor, err = c.artifactActor(ctx, actor, auth.Mutate)
	if err != nil {
		return artifact.Ref{}, err
	}
	if u.State == "writable" {
		// The same pre-guard repair as WriteArtifactChunk, at the full ack.
		if err = c.artifacts.files.RecoverPart(ref, artifact.FileID{Device: file.Device, Inode: file.Inode}, int64(u.Acknowledged)); err != nil {
			return artifact.Ref{}, c.artifactFileError(call, err)
		}
		actor, err = c.artifactActor(ctx, actor, auth.Mutate)
		if err != nil {
			return artifact.Ref{}, err
		}
		if err = c.cachedLinkGrowthCheck(); err != nil {
			return artifact.Ref{}, err
		}
		finish := c.beginUploadStructure(call)
		_, err = c.store.BeginArtifactFinalize(actor, cursor)
		finish()
		if err != nil {
			return artifact.Ref{}, err
		}
		// The new publishing subject invalidated the interval. The actual HTTP
		// response must finish before a new call can use a renewed sample.
		return artifact.Ref{}, artifact.ErrFilesBusy
	}
	u, err = c.store.BeginArtifactFinalize(actor, cursor)
	if err != nil {
		return artifact.Ref{}, err
	}
	cursor.Revision = u.Revision
	sealed, err := c.artifacts.files.SealPart(ref, artifact.FileID{Device: file.Device, Inode: file.Inode}, c.uploadLinkGrowthCheck)
	if err != nil {
		// Files' ordinary Busy is before a new link and positively closes any
		// validation descriptor. Do not restart the very yielded census needed
		// to admit its next attempt. An already held Files.ready owner remains
		// held; Busy is not a receipt settling that owner's original uncertainty.
		if !errors.Is(err, artifact.ErrFilesBusy) || errors.Is(err, artifact.ErrFileCloseUncertain) {
			c.physicalChanged()
		}
		return artifact.Ref{}, c.artifactFileError(call, err)
	}
	// A positive same-inode crash-prefix recovery may have removed .part.
	// New links were already pre-invalidated by the actual late callback.
	c.physicalChanged()
	if sealed.File != (artifact.FileID{Device: file.Device, Inode: file.Inode}) || sealed.Manifest.Reference != ref {
		return artifact.Ref{}, ErrUnavailable
	}
	actor, err = c.artifactActor(ctx, actor, auth.Mutate)
	if err != nil {
		return artifact.Ref{}, err
	}
	manifest := store.ArtifactManifest{Reference: u.Reference, ExpandedBytes: sealed.Manifest.ExpandedBytes, RegularFiles: sealed.Manifest.RegularFiles, Directories: sealed.Manifest.Directories}
	finish := c.beginUploadStructure(call)
	completed, err := c.store.CompleteArtifactUpload(actor, cursor, u.Reference, file, manifest)
	finish()
	if err != nil {
		return artifact.Ref{}, err
	}
	return artifactPortableRef(completed)
}

// BeginArtifactUpload reserves durable ownership before exclusive part creation.
// Any uncertain create remains charged; an existing file is never adopted.
func (c *Control) BeginArtifactUpload(ctx context.Context, actor auth.Actor, call *ArtifactCall, sha256 string, size int64) (store.ArtifactUpload, error) {
	actor, err := c.artifactActor(ctx, actor, auth.Mutate)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	if c.artifacts == nil {
		return store.ArtifactUpload{}, ErrUnsupported
	}
	if size < 1 || size > 32<<20 || len(sha256) != 64 || strings.Trim(sha256, "0123456789abcdef") != "" {
		return store.ArtifactUpload{}, ErrInvalid
	}
	end, err := c.beginArtifactCall(call, "")
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	defer end()
	// ControlBeginGuardOnly: this is the ONLY non-admission wrapper constructor.
	// No Store Begin, exclusive create, or structural mutation has been entered.
	if err = c.cachedLinkGrowthCheck(); err != nil {
		return store.ArtifactUpload{}, &artifactBeginNotStartedError{cause: err}
	}
	finish := c.beginUploadStructure(call)
	defer finish()
	upload, err := c.store.BeginArtifactUpload(actor, sha256, uint64(size))
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	service := c.artifacts
	service.mu.Lock()
	if service.uploads[upload.Reference.ID] != nil {
		service.mu.Unlock()
		return store.ArtifactUpload{}, ErrArtifactBusy
	}
	service.uploads[upload.Reference.ID] = call
	call.upload = upload.Reference.ID
	service.mu.Unlock()
	ref, err := artifactPortableRef(upload.Reference)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	actor, err = c.artifactActor(ctx, actor, auth.Mutate)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	file, err := c.artifacts.files.CreatePart(ref)
	if err != nil {
		// Even a nonzero observed inode cannot prove FD Close succeeded on error.
		// Keep the creating reservation held; never publish or credit this interval.
		return store.ArtifactUpload{}, c.artifactFileError(call, err)
	}
	if file.Device != c.artifacts.root.File.Device || file.Inode == 0 {
		return store.ArtifactUpload{}, ErrUnavailable
	}
	cursor := store.ArtifactCursor{ID: upload.Reference.ID, Revision: upload.Revision}
	observed := store.ArtifactFile{Device: file.Device, Inode: file.Inode}
	fresh, checkErr := c.artifactActor(ctx, actor, auth.Mutate)
	if checkErr == nil {
		upload, err = c.store.ArtifactPartCreated(fresh, cursor, observed)
		if err == nil {
			return upload, nil
		}
	} else {
		err = checkErr
	}
	// Only the nil-error Create receipt proves the original FD owner joined.
	// This trusted cleanup does not grant, acknowledge, publish, or free quota.
	_, settleErr := c.store.AbandonArtifactCreate(store.ArtifactCreateAbandonment{Owner: actor.Principal(), Cursor: cursor, Ref: store.ArtifactRef{ID: ref.ID, SHA256: ref.SHA256, Size: uint64(ref.Size)}, File: observed, RootID: c.artifacts.root.ID})
	if settleErr != nil {
		return store.ArtifactUpload{}, fmt.Errorf("artifact create publication: %w; cleanup remains held: %v", err, settleErr)
	}
	return store.ArtifactUpload{}, err
}
