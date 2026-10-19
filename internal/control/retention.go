package control

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// LocalOutputConfig connects only the server's actual built-in local backend.
// Its zero value installs no local retention callbacks.
type LocalOutputConfig struct {
	Target      string
	Binding     string
	BindOutput  func(backend.OutputReference) error
	OutputUsage func(context.Context) (allocatedBytes, fileCount uint64, err error)
}

// RetentionView is authenticated discovery, not a reservation or expiry action.
// Admin is nil unless current authority permits installation-wide diagnostics.
type RetentionView struct {
	Info    store.RetentionInfo
	Targets []backend.Target
	Admin   *RetentionAdmin
}

// RetentionAdmin contains bounded diagnostics without Tasks or filesystem paths.
// Nil physical fields mean unavailable or stale, never measured zero.
type RetentionAdmin struct {
	Obligations            []store.Obligation
	NextPageToken          string
	DatabaseFileBytes      *uint64
	DatabaseAllocatedBytes *uint64
	FilesystemFreeBytes    *uint64
	OutputAllocatedBytes   *uint64
	OutputFileCount        *uint64
	ReusableDatabaseBytes  uint64
	PendingCleanupRecords  uint64
	Conditions             []string
	ObservedAt             time.Time
}

type retentionState struct {
	mu            sync.Mutex
	leases        map[*LogLease]struct{}
	pending       map[string]struct{}
	local         LocalOutputConfig
	enabled       bool
	reservedBytes uint64
	reservedFiles uint64
	sample        *RetentionAdmin
	wantSample    atomic.Bool
	inspecting    bool
}

func (c *Control) checkAdmin(actor auth.Actor) (auth.Actor, error) {
	fresh, err := c.engine.Check(actor, auth.Mutate)
	if err != nil {
		return auth.Actor{}, err
	}
	if fresh.Role() != auth.Admin {
		return auth.Actor{}, auth.ErrPermissionDenied
	}
	return fresh, nil
}

// RetentionInfo reads current metadata without latching epochs, persisting audit
// or performing filesystem I/O. Admin diagnostics use the Run-owned sample cache.
func (c *Control) RetentionInfo(actor auth.Actor, size int, token string) (RetentionView, error) {
	actor, err := c.engine.Check(actor, auth.Read)
	if err != nil {
		return RetentionView{}, err
	}
	if size < 0 || size > 100 {
		return RetentionView{}, fmt.Errorf("%w: retention page size outside 0..100", ErrInvalid)
	}
	if actor.Role() != auth.Admin && (size != 0 || token != "") {
		return RetentionView{}, auth.ErrPermissionDenied
	}
	if size == 0 {
		size = 50
	}
	info, err := c.store.RetentionInfo(actor)
	if err != nil {
		return RetentionView{}, err
	}
	targets, err := c.Targets(actor)
	if err != nil {
		return RetentionView{}, err
	}
	out := RetentionView{Info: info, Targets: targets}
	if actor.Role() != auth.Admin {
		if token != "" {
			return RetentionView{}, auth.ErrPermissionDenied
		}
		return out, nil
	}
	cur := c.scopeCursor(actor, "obligations", info.Domain.Lineage)
	if token != "" {
		parsed, e := decodeRetentionCursor(token)
		if e != nil {
			return RetentionView{}, e
		}
		expected := cur
		expected.After, expected.Max = parsed.After, parsed.Max
		if parsed != expected || parsed.After == 0 || parsed.After >= parsed.Max {
			return RetentionView{}, fmt.Errorf("%w: retention cursor scope or bounds differ", ErrInvalid)
		}
		snapshot, e := c.store.ListObligations(actor, 0, 0, 1)
		if e != nil {
			return RetentionView{}, e
		}
		if parsed.Max > snapshot.Max {
			return RetentionView{}, fmt.Errorf("%w: obligation cursor exceeds high-water", ErrInvalid)
		}
		cur = parsed
	}
	actor, err = c.checkAdmin(actor)
	if err != nil {
		return RetentionView{}, err
	}
	page, err := c.store.ListObligations(actor, cur.After, cur.Max, size)
	if err != nil {
		return RetentionView{}, err
	}
	c.retention.mu.Lock()
	admin := cloneRetentionAdmin(c.retention.sample)
	c.retention.mu.Unlock()
	if admin == nil {
		admin = &RetentionAdmin{Conditions: []string{"metrics_unavailable"}}
	}
	if !admin.ObservedAt.IsZero() && (time.Now().Before(admin.ObservedAt) || time.Since(admin.ObservedAt) > 5*time.Second) {
		admin.DatabaseFileBytes = nil
		admin.DatabaseAllocatedBytes = nil
		admin.FilesystemFreeBytes = nil
		admin.OutputAllocatedBytes = nil
		admin.OutputFileCount = nil
		admin.Conditions = []string{"metrics_stale"}
	}
	var reservedBytes, reservedFiles uint64
	for _, usage := range info.Usage {
		switch usage.Domain {
		case store.OutputBytesQuota:
			reservedBytes = usage.Used
		case store.OutputFilesQuota:
			reservedFiles = usage.Used
		}
	}
	if condition := storageCondition(admin, false, c.retention.local.BindOutput != nil, reservedBytes, reservedFiles, time.Now()); condition != "" {
		found := false
		for _, existing := range admin.Conditions {
			if existing == condition {
				found = true
				break
			}
		}
		if !found {
			admin.Conditions = append(admin.Conditions, condition)
		}
	}
	admin.ReusableDatabaseBytes, err = c.store.ReusableBytes()
	if err != nil {
		return RetentionView{}, err
	}
	admin.Obligations = page.Items
	admin.PendingCleanupRecords = info.PendingCleanupRecords
	if page.More {
		cur.After, cur.Max = page.After, page.Max
		admin.NextPageToken, err = encodeRetentionCursor(cur)
		if err != nil {
			return RetentionView{}, err
		}
	}
	// A role change while metadata reads were blocked must not publish Admin data.
	if _, err = c.checkAdmin(actor); err != nil {
		return RetentionView{}, err
	}
	c.retention.wantSample.Store(true)
	out.Admin = admin
	return out, nil
}

func cloneRetentionAdmin(in *RetentionAdmin) *RetentionAdmin {
	if in == nil {
		return nil
	}
	out := *in
	ptr := func(p *uint64) *uint64 {
		if p == nil {
			return nil
		}
		v := *p
		return &v
	}
	out.DatabaseFileBytes = ptr(in.DatabaseFileBytes)
	out.DatabaseAllocatedBytes = ptr(in.DatabaseAllocatedBytes)
	out.FilesystemFreeBytes = ptr(in.FilesystemFreeBytes)
	out.OutputAllocatedBytes = ptr(in.OutputAllocatedBytes)
	out.OutputFileCount = ptr(in.OutputFileCount)
	out.Conditions = append([]string(nil), in.Conditions...)
	out.Obligations = append([]store.Obligation(nil), in.Obligations...)
	for i := range out.Obligations {
		out.Obligations[i].ExecutionIDs = append([]string(nil), in.Obligations[i].ExecutionIDs...)
	}
	return &out
}

func validRetentionTarget(t backend.Target) bool {
	if !validTargetName(t.Name) {
		return false
	}
	if t.OutputStorage > backend.RemoteOutput {
		return false
	}
	if t.OutputStorage == backend.LocalOutput && t.OutputReservation == 0 || t.OutputStorage != backend.LocalOutput && t.OutputReservation != 0 {
		return false
	}
	if t.Retention == nil {
		return true
	}
	pair := t.Retention.CheckOutputClosed != nil
	if pair != (t.Retention.RemoveOutput != nil) {
		return false
	}
	return !pair || t.OutputStorage == backend.LocalOutput
}

func validTargetName(name string) bool {
	if len(name) == 0 || len(name) > 128 {
		return false
	}
	for i := range len(name) {
		if name[i] < 32 || name[i] > 126 {
			return false
		}
	}
	return true
}
