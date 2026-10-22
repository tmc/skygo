package rpc

import (
	"fmt"
	"unicode/utf8"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

func requestDomain(domain *v1.RequestDomain) (store.RequestDomain, error) {
	if domain == nil || (domain.Lineage == "" && domain.Epoch == 0) {
		return store.RequestDomain{}, &store.ExpiryError{Reason: store.RequestDomainRequired}
	}
	if !utf8.ValidString(domain.Lineage) || !hexID(domain.Lineage) || domain.Epoch == 0 {
		return store.RequestDomain{}, fmt.Errorf("%w: malformed request domain", control.ErrInvalid)
	}
	return store.RequestDomain{Lineage: domain.Lineage, Epoch: domain.Epoch}, nil
}

func retentionPage(size int32, token string) error {
	if size < 0 || size > 100 || len(token) > 683 || !utf8.ValidString(token) {
		return fmt.Errorf("%w: invalid retention page", control.ErrInvalid)
	}
	return nil
}

func pruneSelection(message *v1.PruneJobsRequest) (store.PruneSelection, error) {
	if err := retentionPage(message.PageSize, message.PageToken); err != nil {
		return store.PruneSelection{}, err
	}
	selection := store.PruneSelection{AcknowledgeUnverifiedTerminal: message.AcknowledgeUnverifiedTerminal, ObligationID: message.ObligationId, Limit: int(message.PageSize), Attempt: message.Attempt}
	switch message.Mode {
	case v1.PruneMode_PRUNE_MODE_UNSPECIFIED, v1.PruneMode_PRUNE_MODE_WHOLE:
		selection.Mode = store.Whole
	case v1.PruneMode_PRUNE_MODE_METADATA_ONLY:
		selection.Mode = store.MetadataOnly
	case v1.PruneMode_PRUNE_MODE_OUTPUT_ONLY:
		selection.Mode = store.OutputOnly
	case v1.PruneMode_PRUNE_MODE_RESULT_ONLY:
		selection.Mode = store.ResultOnly
	default:
		return selection, fmt.Errorf("%w: unsupported prune mode", control.ErrInvalid)
	}
	if (selection.Mode == store.ResultOnly) != (selection.Attempt != 0) {
		return selection, fmt.Errorf("%w: result-only requires a nonzero attempt; other modes require zero", control.ErrInvalid)
	}
	if selection.AcknowledgeUnverifiedTerminal != (selection.Mode == store.MetadataOnly) {
		return selection, fmt.Errorf("%w: metadata-only acknowledgement required only for metadata-only mode", control.ErrInvalid)
	}
	if message.ObligationId != "" {
		if (selection.Mode != store.OutputOnly && selection.Mode != store.ResultOnly) || !utf8.ValidString(message.ObligationId) || !hexID(message.ObligationId) || message.Before != nil || message.PageToken != "" || message.PageSize != 0 {
			return selection, fmt.Errorf("%w: invalid output obligation selector", control.ErrInvalid)
		}
		return selection, nil
	}
	if message.Before == nil || message.Before.CheckValid() != nil || message.Before.AsTime().IsZero() {
		return selection, fmt.Errorf("%w: valid prune cutoff required", control.ErrInvalid)
	}
	selection.Before = message.Before.AsTime().UTC()
	return selection, nil
}

func retentionRequestID(text string) bool {
	if len(text) < 1 || len(text) > 128 {
		return false
	}
	for _, c := range text {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

func validTaskUTF8(task *v1.Task) bool {
	return task != nil && len(task.DataMounts) == 0 && validDataTaskUTF8(task)
}

func validDataTaskUTF8(task *v1.Task) bool {
	if task == nil {
		return false
	}
	for _, text := range append([]string{task.Directory, task.Image, task.Accelerator, task.Target}, task.Argv...) {
		if !utf8.ValidString(text) {
			return false
		}
	}
	for _, values := range [][]string{task.Setup, task.Outputs} {
		for _, text := range values {
			if !utf8.ValidString(text) {
				return false
			}
		}
	}
	if task.Workdir != nil && (!utf8.ValidString(task.Workdir.Id) || !utf8.ValidString(task.Workdir.Sha256)) {
		return false
	}
	for _, mount := range task.DataMounts {
		if mount == nil || !utf8.ValidString(mount.Reference) || !utf8.ValidString(mount.Destination) {
			return false
		}
	}
	for _, values := range []map[string]string{task.Env, task.TargetLabels, task.NodeSelector} {
		for key, value := range values {
			if !utf8.ValidString(key) || !utf8.ValidString(value) {
				return false
			}
		}
	}
	return true
}
