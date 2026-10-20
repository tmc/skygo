package control

import (
	"context"
	"fmt"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/store"
)

// ArtifactResultFor returns fresh owner-authorized metadata for an exact accepted
// execution. A reference is metadata, not a byte grant or native writer proof.
// Separate calls are separate snapshots; no whole-response atomicity is implied.
func (c *Control) ArtifactResultFor(actor auth.Actor, jobID, attemptID string) (store.ArtifactAttemptResult, error) {
	actor, err := c.engine.Check(actor, auth.Read)
	if err != nil {
		return store.ArtifactAttemptResult{}, err
	}
	if !jobName.MatchString(jobID) || !jobName.MatchString(attemptID) {
		return store.ArtifactAttemptResult{}, ErrInvalid
	}
	if c.artifacts == nil {
		return store.ArtifactAttemptResult{}, ErrUnsupported
	}
	result, err := c.store.ArtifactResultFor(actor, jobID, attemptID)
	if err != nil {
		return store.ArtifactAttemptResult{}, err
	}
	if _, err = c.engine.Check(actor, auth.Read); err != nil {
		return store.ArtifactAttemptResult{}, err
	}
	return copyArtifactResult(result, attemptID)
}

func copyArtifactResult(result store.ArtifactAttemptResult, attemptID string) (store.ArtifactAttemptResult, error) {
	if result.AttemptID != attemptID {
		return store.ArtifactAttemptResult{}, ErrUnavailable
	}
	switch result.State {
	case "available":
		if result.Reference == nil {
			return store.ArtifactAttemptResult{}, ErrUnavailable
		}
		if _, err := artifactPortableRef(*result.Reference); err != nil {
			return store.ArtifactAttemptResult{}, err
		}
		ref := *result.Reference
		result.Reference = &ref
	case "absent", "expired", "held":
		if result.Reference != nil {
			return store.ArtifactAttemptResult{}, ErrUnavailable
		}
	default:
		return store.ArtifactAttemptResult{}, ErrUnavailable
	}
	return result, nil
}

// artifactPruneCursor is a separate purpose from retained job/log pagination.
// Disabled request retention omits lineage entirely; it never invents an epoch.
// Existing cursor encodings and default request behavior are unchanged.
type artifactPruneCursor struct {
	Version   uint8  `json:"v"`
	Kind      string `json:"k"`
	Owner     string `json:"i"`
	Root      string `json:"r"`
	Principal string `json:"p"`
	Global    bool   `json:"g"`
	Scoped    bool   `json:"h,omitempty"`
	Lineage   string `json:"l,omitempty"`
	Attempt   uint32 `json:"n"`
	Before    string `json:"t"`
	Execute   bool   `json:"x"`
	After     uint64 `json:"a"`
	Max       uint64 `json:"m"`
}

func (c *Control) resultPruneScope(actor auth.Actor, info store.RetentionInfo, selection store.PruneSelection, execute bool) (artifactPruneCursor, error) {
	if c.artifacts == nil {
		return artifactPruneCursor{}, ErrUnsupported
	}
	if !jobName.MatchString(c.artifacts.root.ID) {
		return artifactPruneCursor{}, ErrUnavailable
	}
	cur := artifactPruneCursor{Version: 1, Kind: "artifact-prune", Owner: c.store.Owner(), Root: c.artifacts.root.ID, Principal: actor.Principal(), Global: actor.Role() == auth.Admin, Scoped: c.store.Scoped(), Attempt: selection.Attempt, Execute: execute}
	if !selection.Before.IsZero() {
		cutoff := selection.Before.UTC()
		if cutoff.Year() < 1 || cutoff.Year() > 9999 {
			return artifactPruneCursor{}, ErrInvalid
		}
		cur.Before = cutoff.Format(time.RFC3339Nano)
	}
	if info.Enabled {
		if !jobName.MatchString(info.Domain.Lineage) {
			return artifactPruneCursor{}, ErrUnavailable
		}
		cur.Lineage = info.Domain.Lineage
	}
	return cur, nil
}

func (c *Control) pruneArtifactResults(ctx context.Context, actor auth.Actor, selection store.PruneSelection, execute bool, token string) (store.PrunePage, string, error) {
	if c.artifacts == nil {
		return store.PrunePage{}, "", ErrUnsupported
	}
	c.retention.mu.Lock()
	if c.retention.inspecting {
		c.retention.mu.Unlock()
		return store.PrunePage{}, "", ErrUnavailable
	}
	c.retention.inspecting = true
	c.retention.mu.Unlock()
	defer func() { c.retention.mu.Lock(); c.retention.inspecting = false; c.retention.mu.Unlock() }()
	info, err := c.store.RetentionInfo(actor)
	if err != nil {
		return store.PrunePage{}, "", err
	}
	cur, err := c.resultPruneScope(actor, info, selection, execute)
	if err != nil {
		return store.PrunePage{}, "", err
	}
	if token != "" {
		var parsed artifactPruneCursor
		if err = decodeCursor(token, &parsed, 512); err != nil {
			return store.PrunePage{}, "", err
		}
		expected := cur
		expected.After = parsed.After
		expected.Max = parsed.Max
		if parsed != expected || parsed.After == 0 || parsed.After >= parsed.Max {
			return store.PrunePage{}, "", fmt.Errorf("%w: artifact prune cursor scope or bounds differ", ErrInvalid)
		}
		max, err := c.store.LastSequenceFor(actor)
		if err != nil {
			return store.PrunePage{}, "", err
		}
		if parsed.Max > max {
			return store.PrunePage{}, "", fmt.Errorf("%w: artifact prune cursor exceeds high-water", ErrInvalid)
		}
		cur = parsed
	} else if selection.ObligationID == "" {
		cur.Max, err = c.store.LastSequenceFor(actor)
		if err != nil {
			return store.PrunePage{}, "", err
		}
	}
	selection.After = cur.After
	selection.Max = cur.Max
	if err = ctx.Err(); err != nil {
		return store.PrunePage{}, "", err
	}
	if execute {
		if err = c.storageGuard(true, false); err != nil {
			return store.PrunePage{}, "", err
		}
	}
	// Result-only deliberately performs no backend output-closure inspection.
	c.retention.mu.Lock()
	actor, err = c.checkAdmin(actor)
	if err != nil {
		c.retention.mu.Unlock()
		return store.PrunePage{}, "", err
	}
	blocked, err := c.blockedLocked()
	if err != nil {
		c.retention.mu.Unlock()
		return store.PrunePage{}, "", err
	}
	if err = ctx.Err(); err != nil {
		c.retention.mu.Unlock()
		return store.PrunePage{}, "", err
	}
	if execute && c.retention.enabled {
		if condition := storageCondition(c.retention.sample, true, false, 0, 0, time.Now()); condition != "" {
			c.retention.wantSample.Store(true)
			c.retention.mu.Unlock()
			return store.PrunePage{}, "", fmt.Errorf("%w: %s", ErrUnavailable, condition)
		}
	}
	actor, err = c.checkAdmin(actor)
	if err != nil {
		c.retention.mu.Unlock()
		return store.PrunePage{}, "", err
	}
	page, err := c.store.Prune(actor, selection, execute, blocked, nil, time.Now().UTC())
	c.retention.mu.Unlock()
	if err != nil {
		return store.PrunePage{}, "", err
	}
	if execute && page.Marked != 0 {
		c.retention.wantSample.Store(true)
		_ = c.RecordAccess(actor, "prune", "", "allowed")
	}
	next := ""
	if page.More {
		cur.After = page.After
		next, err = encodeCursor(cur, 512)
		if err != nil {
			return store.PrunePage{}, "", err
		}
	}
	return page, next, nil
}
