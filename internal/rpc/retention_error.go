package rpc

import (
	"errors"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/store"
)

func quotaDomain(domain store.QuotaDomain) (v1.RetentionQuotaDomain, bool) {
	switch domain {
	case store.JobsQuota:
		return v1.RetentionQuotaDomain_RETENTION_QUOTA_DOMAIN_JOBS, true
	case store.TaskBytesQuota:
		return v1.RetentionQuotaDomain_RETENTION_QUOTA_DOMAIN_TASK_BYTES, true
	case store.RetryBytesQuota:
		return v1.RetentionQuotaDomain_RETENTION_QUOTA_DOMAIN_RETRY_BYTES, true
	case store.MetadataBytesQuota:
		return v1.RetentionQuotaDomain_RETENTION_QUOTA_DOMAIN_METADATA_BYTES, true
	case store.RequestRecordsQuota:
		return v1.RetentionQuotaDomain_RETENTION_QUOTA_DOMAIN_REQUEST_RECORDS, true
	case store.RequestBytesQuota:
		return v1.RetentionQuotaDomain_RETENTION_QUOTA_DOMAIN_REQUEST_BYTES, true
	case store.CleanupRecordsQuota:
		return v1.RetentionQuotaDomain_RETENTION_QUOTA_DOMAIN_CLEANUP_RECORDS, true
	case store.CleanupBytesQuota:
		return v1.RetentionQuotaDomain_RETENTION_QUOTA_DOMAIN_CLEANUP_BYTES, true
	case store.OutputBytesQuota:
		return v1.RetentionQuotaDomain_RETENTION_QUOTA_DOMAIN_OUTPUT_BYTES, true
	case store.OutputDomainsQuota:
		return v1.RetentionQuotaDomain_RETENTION_QUOTA_DOMAIN_OUTPUT_DOMAINS, true
	case store.OutputFilesQuota:
		return v1.RetentionQuotaDomain_RETENTION_QUOTA_DOMAIN_OUTPUT_FILES, true
	case store.PrincipalScopesQuota:
		return v1.RetentionQuotaDomain_RETENTION_QUOTA_DOMAIN_PRINCIPAL_SCOPES, true
	}
	return 0, false
}

func expiryReason(reason store.ExpiryReason) (v1.RetentionExpiryReason, bool) {
	switch reason {
	case store.ResultExpired:
		return v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_RESULT_EXPIRED, true
	case store.RequestEpochExpired:
		return v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_EPOCH_EXPIRED, true
	case store.RequestDomainRequired:
		return v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_DOMAIN_REQUIRED, true
	case store.RequestLineageChanged:
		return v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_LINEAGE_CHANGED, true
	case store.LogsExpired:
		return v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_LOGS_EXPIRED, true
	}
	return 0, false
}

func retentionError(err error) *connect.Error {
	invalid := func() *connect.Error {
		return connect.NewError(connect.CodeInternal, errors.New("internal operation failed"))
	}
	var expired *store.ExpiryError
	if errors.As(err, &expired) {
		if expired == nil {
			return invalid()
		}
		reason, ok := expiryReason(expired.Reason)
		if !ok || (expired.JobID != "" && (expired.Reason != store.ResultExpired || !hexID(expired.JobID))) {
			return invalid()
		}
		out := connect.NewError(connect.CodeFailedPrecondition, errors.New("retention result or request expired"))
		detail, err := connect.NewErrorDetail(&v1.RetentionExpiry{Reason: reason, JobId: expired.JobID})
		if err != nil {
			return invalid()
		}
		out.AddDetail(detail)
		return out
	}
	var capacity *store.CapacityError
	if errors.As(err, &capacity) {
		if capacity == nil {
			return invalid()
		}
		domain, ok := quotaDomain(capacity.Domain)
		if !ok {
			return invalid()
		}
		out := connect.NewError(connect.CodeResourceExhausted, errors.New("retention capacity reached"))
		detail, err := connect.NewErrorDetail(&v1.RetentionCapacity{Domain: domain, Used: capacity.Used, Limit: capacity.Limit})
		if err != nil {
			return invalid()
		}
		out.AddDetail(detail)
		return out
	}
	return nil
}

func hexID(s string) bool {
	if len(s) != 32 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
