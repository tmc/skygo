package rpc

import (
	"fmt"
	"time"
	"unicode/utf8"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func retentionUnavailable() error {
	return fmt.Errorf("%w: retention response unavailable", control.ErrUnavailable)
}

func retentionTime(value time.Time, required bool) (*timestamppb.Timestamp, error) {
	if value.IsZero() {
		if required {
			return nil, retentionUnavailable()
		}
		return nil, nil
	}
	out := timestamppb.New(value)
	if out.CheckValid() != nil {
		return nil, fmt.Errorf("%w: retention observation unavailable", control.ErrUnavailable)
	}
	return out, nil
}

func retentionText(value string, maximum int, empty bool) bool {
	return (empty || value != "") && len(value) <= maximum && utf8.ValidString(value)
}

func retentionCode(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for _, c := range value {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}

func wireRetention(view control.RetentionView) (*v1.GetRetentionResponse, error) {
	info := view.Info
	out := &v1.GetRetentionResponse{Enabled: info.Enabled, LegacyReplayOpen: info.LegacyReplayOpen}
	if info.Enabled {
		if !hexID(info.Domain.Lineage) || info.Domain.Epoch == 0 {
			return nil, retentionUnavailable()
		}
		out.Domain = &v1.RequestDomain{Lineage: info.Domain.Lineage, Epoch: info.Domain.Epoch}
		var err error
		if out.AcceptUntil, err = retentionTime(info.AcceptUntil, true); err != nil {
			return nil, err
		}
		if out.RetryUntil, err = retentionTime(info.RetryUntil, true); err != nil {
			return nil, err
		}
		if out.EpochZeroCutoff, err = retentionTime(info.LegacyCutoff, true); err != nil {
			return nil, err
		}
	}
	if len(view.Targets) > 32 {
		return nil, retentionUnavailable()
	}
	for _, target := range view.Targets {
		if !retentionText(target.Name, 128, false) {
			return nil, retentionUnavailable()
		}
		storage := v1.OutputStorage_OUTPUT_STORAGE_UNSPECIFIED
		switch target.OutputStorage {
		case backend.UnspecifiedOutput:
		case backend.LocalOutput:
			storage = v1.OutputStorage_OUTPUT_STORAGE_LOCAL
		case backend.RemoteOutput:
			storage = v1.OutputStorage_OUTPUT_STORAGE_REMOTE
		default:
			return nil, retentionUnavailable()
		}
		wire := &v1.RetentionTarget{Target: target.Name, OutputStorage: storage, OutputReservationBytes: target.OutputReservation}
		if target.Retention != nil {
			if (target.Retention.CheckOutputClosed == nil) != (target.Retention.RemoveOutput == nil) {
				return nil, retentionUnavailable()
			}
			wire.OutputCleanup = target.Retention.CheckOutputClosed != nil
			wire.ExecutionCleanup = target.Retention.RemoveExecution != nil
		}
		if target.OutputStorage == backend.RemoteOutput && (target.OutputReservation != 0 || wire.OutputCleanup) {
			return nil, retentionUnavailable()
		}
		out.Targets = append(out.Targets, wire)
	}
	if view.Admin == nil {
		return out, nil
	}
	admin := view.Admin
	if len(info.Usage) != 12 || len(admin.Obligations) > 100 || len(admin.Conditions) > 16 ||
		len(admin.NextPageToken) > 683 || !utf8.ValidString(admin.NextPageToken) {
		return nil, retentionUnavailable()
	}
	wire := &v1.RetentionAdmin{NextPageToken: admin.NextPageToken, ReusableDatabaseBytes: admin.ReusableDatabaseBytes, PendingCleanupRecords: admin.PendingCleanupRecords}
	seen := make(map[store.QuotaDomain]bool)
	for _, usage := range info.Usage {
		domain, ok := quotaDomain(usage.Domain)
		if !ok || seen[usage.Domain] {
			return nil, retentionUnavailable()
		}
		seen[usage.Domain] = true
		wire.Usage = append(wire.Usage, &v1.RetentionUsage{Domain: domain, Used: usage.Used, Limit: usage.Limit})
	}
	for _, obligation := range admin.Obligations {
		if !hexID(obligation.ID) || !hexID(obligation.JobID) || len(obligation.ExecutionIDs) > 10 ||
			!retentionText(obligation.Target, 128, false) || !retentionText(obligation.Binding, 256, true) ||
			!retentionCode(obligation.Reason, 128) {
			return nil, retentionUnavailable()
		}
		item := &v1.RetentionObligation{Id: obligation.ID, JobId: obligation.JobID, Target: obligation.Target, Binding: obligation.Binding, NeedsManual: obligation.NeedsManual, OutputPending: obligation.OutputPending, ExecutionPending: obligation.ExecutionPending, Reason: obligation.Reason}
		for _, id := range obligation.ExecutionIDs {
			if !hexID(id) {
				return nil, retentionUnavailable()
			}
			item.ExecutionIds = append(item.ExecutionIds, id)
		}
		var err error
		if item.CreatedAt, err = retentionTime(obligation.CreatedAt, true); err != nil {
			return nil, err
		}
		wire.Obligations = append(wire.Obligations, item)
	}
	clone := func(value *uint64) *uint64 {
		if value == nil {
			return nil
		}
		copy := *value
		return &copy
	}
	wire.DatabaseFileBytes = clone(admin.DatabaseFileBytes)
	wire.DatabaseAllocatedBytes = clone(admin.DatabaseAllocatedBytes)
	wire.FilesystemFreeBytes = clone(admin.FilesystemFreeBytes)
	wire.OutputAllocatedBytes = clone(admin.OutputAllocatedBytes)
	wire.OutputFileCount = clone(admin.OutputFileCount)
	for _, condition := range admin.Conditions {
		if !retentionCode(condition, 64) {
			return nil, retentionUnavailable()
		}
		wire.Conditions = append(wire.Conditions, condition)
	}
	if admin.ObservedAt.IsZero() && (wire.DatabaseFileBytes != nil || wire.DatabaseAllocatedBytes != nil ||
		wire.FilesystemFreeBytes != nil || wire.OutputAllocatedBytes != nil || wire.OutputFileCount != nil || len(wire.Conditions) == 0) {
		return nil, retentionUnavailable()
	}
	var err error
	if wire.ObservedAt, err = retentionTime(admin.ObservedAt, false); err != nil {
		return nil, err
	}
	out.Admin = wire
	return out, nil
}

func wirePrune(page store.PrunePage, next string, execute bool) (*v1.PruneJobsResponse, error) {
	if page.Inspected > 200 || page.Selected > 100 || page.Marked > page.Selected || (!execute && page.Marked != 0) {
		return nil, retentionUnavailable()
	}
	if uint64(page.Selected)+uint64(page.Skipped) != uint64(page.Inspected) || len(page.Skips) > 16 || len(page.Transfers) > 12 {
		return nil, retentionUnavailable()
	}
	if len(next) > 683 || !utf8.ValidString(next) || page.More != (next != "") {
		return nil, retentionUnavailable()
	}
	out := &v1.PruneJobsResponse{Inspected: page.Inspected, Selected: page.Selected, Marked: page.Marked, Skipped: page.Skipped, NextPageToken: next, Executed: execute}
	for _, skip := range page.Skips {
		if !retentionCode(skip.Reason, 64) {
			return nil, retentionUnavailable()
		}
		out.Skips = append(out.Skips, &v1.PruneSkip{Reason: skip.Reason, Count: skip.Count})
	}
	seen := make(map[store.QuotaDomain]bool)
	for _, transfer := range page.Transfers {
		domain, ok := quotaDomain(transfer.Domain)
		if !ok || seen[transfer.Domain] {
			return nil, retentionUnavailable()
		}
		seen[transfer.Domain] = true
		out.Transfers = append(out.Transfers, &v1.RetentionChargeTransfer{Domain: domain, Added: transfer.Added, Released: transfer.Released})
	}
	return out, nil
}
