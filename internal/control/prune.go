package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

func (c *Control) blockedLocked() ([]string, error) {
	ids := make(map[string]bool, 64)
	for l := range c.retention.leases {
		ids[l.attemptID] = true
	}
	for id := range c.retention.pending {
		ids[id] = true
	}
	if len(ids) > 64 {
		return nil, fmt.Errorf("%w: pending execution invariant exceeded", ErrUnavailable)
	}
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// Prune inspects or marks one bounded page. Dry-run makes no persistent writes.
// Closure inspection is try-serialized and may outlive cancellation if an adapter
// ignores its context; no replacement query or cleanup worker is started.
func (c *Control) Prune(ctx context.Context, actor auth.Actor, selection store.PruneSelection, execute bool, token string) (store.PrunePage, string, error) {
	actor, err := c.checkAdmin(actor)
	if err != nil {
		return store.PrunePage{}, "", err
	}
	if selection.Mode > store.OutputOnly || selection.Limit < 0 || selection.Limit > 100 || (selection.Mode == store.MetadataOnly) != selection.AcknowledgeUnverifiedTerminal || selection.After != 0 || selection.Max != 0 {
		return store.PrunePage{}, "", fmt.Errorf("%w: invalid prune selection", ErrInvalid)
	}
	if selection.ObligationID != "" {
		if !jobName.MatchString(selection.ObligationID) || selection.Mode != store.OutputOnly || !selection.Before.IsZero() || selection.Limit != 0 || token != "" {
			return store.PrunePage{}, "", fmt.Errorf("%w: invalid obligation selector", ErrInvalid)
		}
	} else if selection.Before.IsZero() || selection.Before.Year() < 1 || selection.Before.Year() > 9999 {
		return store.PrunePage{}, "", fmt.Errorf("%w: invalid prune cutoff", ErrInvalid)
	}
	c.retention.mu.Lock()
	if c.retention.inspecting {
		c.retention.mu.Unlock()
		return store.PrunePage{}, "", fmt.Errorf("%w: prune inspection already active", ErrUnavailable)
	}
	c.retention.inspecting = true
	c.retention.mu.Unlock()
	defer func() { c.retention.mu.Lock(); c.retention.inspecting = false; c.retention.mu.Unlock() }()
	info, err := c.store.RetentionInfo(actor)
	if err != nil {
		return store.PrunePage{}, "", err
	}
	raw, err := json.Marshal(struct {
		Before  string          `json:"before"`
		Mode    store.PruneMode `json:"mode"`
		Ack     bool            `json:"ack"`
		Execute bool            `json:"execute"`
	}{selection.Before.UTC().Format(time.RFC3339Nano), selection.Mode, selection.AcknowledgeUnverifiedTerminal, execute})
	if err != nil {
		return store.PrunePage{}, "", err
	}
	cur := c.scopeCursor(actor, "prune", info.Domain.Lineage)
	cur.Selector = fmt.Sprintf("%x", sha256.Sum256(raw))
	if token != "" {
		if selection.ObligationID != "" {
			return store.PrunePage{}, "", fmt.Errorf("%w: obligation selection cannot continue", ErrInvalid)
		}
		parsed, e := decodeRetentionCursor(token)
		if e != nil {
			return store.PrunePage{}, "", e
		}
		expected := cur
		expected.After, expected.Max = parsed.After, parsed.Max
		if parsed != expected || parsed.After == 0 || parsed.After >= parsed.Max {
			return store.PrunePage{}, "", fmt.Errorf("%w: prune cursor scope or bounds differ", ErrInvalid)
		}
		max, e := c.store.LastSequenceFor(actor)
		if e != nil {
			return store.PrunePage{}, "", e
		}
		if parsed.Max > max {
			return store.PrunePage{}, "", fmt.Errorf("%w: prune cursor exceeds high-water", ErrInvalid)
		}
		cur = parsed
	} else if selection.ObligationID == "" {
		cur.Max, err = c.store.LastSequenceFor(actor)
		if err != nil {
			return store.PrunePage{}, "", err
		}
	}
	selection.After, selection.Max = cur.After, cur.Max
	subjects, err := c.store.PruneSubjects(actor, selection)
	if err != nil {
		return store.PrunePage{}, "", err
	}
	var ephemeral []backend.OutputReference
	queried := 0
	for _, subject := range subjects {
		if ctx.Err() != nil {
			return store.PrunePage{}, "", ctx.Err()
		}
		// Preserve a complete per-subject evidence set within the fixed query budget.
		if len(subject.Output) > 100-queried {
			continue
		}
		targetName, binding := subject.Job.Target, subject.Job.TargetBinding
		if subject.Obligation != nil {
			targetName, binding = subject.Obligation.Target, subject.Obligation.Binding
		}
		target, ok := c.target(targetName)
		if !ok || binding == "" || target.Binding != binding || target.OutputStorage != backend.LocalOutput || target.Retention == nil || target.Retention.CheckOutputClosed == nil {
			continue
		}
		for _, ref := range subject.Output {
			queried++
			query, cancel := context.WithTimeout(ctx, 10*time.Second)
			receipt, queryErr := target.Retention.CheckOutputClosed(query, ref)
			cancel()
			// Authority captured before a blocking query is never mutation authority.
			actor, err = c.checkAdmin(actor)
			if err != nil {
				return store.PrunePage{}, "", err
			}
			if ctx.Err() != nil {
				return store.PrunePage{}, "", ctx.Err()
			}
			if queryErr != nil || !validClosureID(receipt.ClosureID) {
				continue
			}
			ref.ClosureID, ref.PayloadReleased = receipt.ClosureID, receipt.PayloadReleased
			if !execute {
				ephemeral = append(ephemeral, ref)
				continue
			}
			// No blocking call or metrics query between this fresh check and proof txn.
			if err = c.storageGuard(true, false); err != nil {
				return store.PrunePage{}, "", err
			}
			actor, err = c.checkAdmin(actor)
			if err != nil {
				return store.PrunePage{}, "", err
			}
			if err = c.store.RecordOutputClosure(ref, subject.Digest); err != nil {
				return store.PrunePage{}, "", err
			}
			c.retention.wantSample.Store(true)
			_ = c.RecordAccess(actor, "prune", ref.JobID, "allowed")
		}
	}
	if execute {
		if err = c.storageGuard(true, false); err != nil {
			return store.PrunePage{}, "", err
		}
	}
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
	if ctx.Err() != nil {
		c.retention.mu.Unlock()
		return store.PrunePage{}, "", ctx.Err()
	}
	// Lock acquisition may have outlived the observed maintenance guard.
	if execute && c.retention.enabled {
		if condition := storageCondition(c.retention.sample, true, false, 0, 0, time.Now()); condition != "" {
			c.retention.wantSample.Store(true)
			c.retention.mu.Unlock()
			return store.PrunePage{}, "", fmt.Errorf("%w: %s", ErrUnavailable, condition)
		}
	}
	// Lock acquisition and bookkeeping may have waited: recheck immediately at mark.
	actor, err = c.checkAdmin(actor)
	if err != nil {
		c.retention.mu.Unlock()
		return store.PrunePage{}, "", err
	}
	page, err := c.store.Prune(actor, selection, execute, blocked, ephemeral, time.Now().UTC())
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
		next, err = encodeRetentionCursor(cur)
		if err != nil {
			return store.PrunePage{}, "", err
		}
	}
	return page, next, nil
}

func validClosureID(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for i := range len(s) {
		if s[i] < 32 || s[i] > 126 {
			return false
		}
	}
	return true
}
