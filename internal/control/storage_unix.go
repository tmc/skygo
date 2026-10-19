//go:build linux || darwin

package control

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func sampleStorage(ctx context.Context, directory string, reusable uint64, usage func(context.Context) (uint64, uint64, error)) *RetentionAdmin {
	sample := &RetentionAdmin{ReusableDatabaseBytes: reusable, ObservedAt: time.Now().UTC()}
	ptr := func(v uint64) *uint64 { return &v }
	info, err := os.Stat(filepath.Join(directory, "jobs.db"))
	if err == nil && info.Mode().IsRegular() && info.Size() >= 0 {
		sample.DatabaseFileBytes = ptr(uint64(info.Size()))
		if s, ok := info.Sys().(*syscall.Stat_t); ok && s.Blocks >= 0 && uint64(s.Blocks) <= math.MaxUint64/512 {
			sample.DatabaseAllocatedBytes = ptr(uint64(s.Blocks) * 512)
		} else {
			sample.Conditions = append(sample.Conditions, "database_allocation_unavailable")
		}
	} else {
		sample.Conditions = append(sample.Conditions, "database_sample_unavailable")
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(directory, &stat); err == nil && stat.Bsize > 0 && uint64(stat.Bavail) <= math.MaxUint64/uint64(stat.Bsize) {
		sample.FilesystemFreeBytes = ptr(uint64(stat.Bavail) * uint64(stat.Bsize))
	} else {
		sample.Conditions = append(sample.Conditions, "filesystem_sample_unavailable")
	}
	if usage != nil && ctx.Err() == nil {
		allocated, files, err := usage(ctx)
		if err == nil {
			sample.OutputAllocatedBytes = ptr(allocated)
			sample.OutputFileCount = ptr(files)
		} else {
			sample.Conditions = append(sample.Conditions, "output_sample_unavailable")
		}
	} else if usage != nil {
		sample.Conditions = append(sample.Conditions, "output_sample_unavailable")
	}
	return sample
}
