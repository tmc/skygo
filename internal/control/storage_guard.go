package control

import (
	"fmt"
	"time"
)

const (
	admissionDatabaseBytes   = 256 << 20
	maintenanceDatabaseBytes = 320 << 20
	admissionFreeBytes       = 2 << 30
	emergencyFreeBytes       = 1 << 30
)

// StorageReady reports recent observed admission health without filesystem I/O.
// It is an operational guard, not a current physical size or free-space guarantee.
// Installed artifacts require their own cached observations even without
// request retention; artifacts-absent defaults retain their existing behavior.
func (c *Control) StorageReady() bool {
	if c.artifacts != nil {
		c.artifacts.mu.Lock()
		stopped := c.artifacts.closing
		c.artifacts.mu.Unlock()
		if stopped {
			return false
		}
		if c.cachedLinkGrowthCheck() != nil {
			return false
		}
	}
	if !c.retention.enabled {
		return true
	}
	c.retention.mu.Lock()
	defer c.retention.mu.Unlock()
	return storageCondition(c.retention.sample, false, c.retention.local.BindOutput != nil, c.retention.reservedBytes, c.retention.reservedFiles, time.Now()) == ""
}

func storageCondition(sample *RetentionAdmin, maintenance, local bool, reservedBytes, reservedFiles uint64, now time.Time) string {
	if sample == nil || sample.ObservedAt.IsZero() {
		return "metrics_unavailable"
	}
	if now.Before(sample.ObservedAt) || now.Sub(sample.ObservedAt) > 5*time.Second {
		return "metrics_stale"
	}
	if sample.DatabaseFileBytes == nil || sample.FilesystemFreeBytes == nil {
		return "metrics_unavailable"
	}
	if *sample.FilesystemFreeBytes < emergencyFreeBytes {
		return "storage_critical"
	}
	if *sample.DatabaseFileBytes >= maintenanceDatabaseBytes {
		return "maintenance_hold"
	}
	if !maintenance && (*sample.DatabaseFileBytes >= admissionDatabaseBytes || *sample.FilesystemFreeBytes < admissionFreeBytes) {
		return "admission_hold"
	}
	if local {
		if sample.OutputAllocatedBytes == nil || sample.OutputFileCount == nil {
			return "output_metrics_unavailable"
		}
		if *sample.OutputAllocatedBytes > reservedBytes || *sample.OutputFileCount > reservedFiles {
			return "output_accounting_hold"
		}
	}
	return ""
}

func (c *Control) storageGuard(maintenance, local bool) error {
	if !c.retention.enabled {
		return nil
	}
	var reservedBytes, reservedFiles uint64
	if local {
		var err error
		reservedBytes, reservedFiles, err = c.store.OutputReservations()
		if err != nil {
			return err
		}
	}
	c.retention.mu.Lock()
	condition := storageCondition(c.retention.sample, maintenance, local, reservedBytes, reservedFiles, time.Now())
	c.retention.mu.Unlock()
	if condition != "" {
		c.retention.wantSample.Store(true)
		return fmt.Errorf("%w: %s", ErrUnavailable, condition)
	}
	return nil
}
