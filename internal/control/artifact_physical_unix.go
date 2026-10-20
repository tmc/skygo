//go:build linux || darwin

package control

import (
	"math"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// physicalDatabaseVolume compares actual observed file/directory devices. A
// regular DB file mounted from another volume cannot borrow its parent's free
// capacity. This is a value check, not atomic hostile-path or original-bbolt-FD
// identity proof; every caller still brackets its point with identity checks.
func physicalDatabaseVolume(directory, file os.FileInfo) bool {
	if directory == nil || file == nil || !directory.IsDir() || !file.Mode().IsRegular() {
		return false
	}
	dir, ok := directory.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	db, ok := file.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	return dir.Dev != 0 && db.Dev == dir.Dev
}

// samplePhysicalDatabase does only bounded existing-path point probes. Database
// length and DB-volume free space have DISTINCT actual completion timestamps.
// Failed components replace prior values with unavailable, never fall back to a
// previous fresh value. No DB open/InspectPhysical/adopted live handle is used.
func (c *Control) samplePhysicalDatabase() {
	directory := c.store.Directory()
	points := physicalPoints{}
	dirBefore, dirErr := os.Lstat(directory)
	if dirErr == nil && dirBefore.IsDir() && dirBefore.Mode()&os.ModeSymlink == 0 {
		name := filepath.Join(directory, "jobs.db")
		before, fileErr := os.Lstat(name)
		if fileErr == nil && before.Mode().IsRegular() && before.Size() >= 0 {
			points.databaseBytes = physicalUint(uint64(before.Size()))
			points.databaseAt = time.Now()
			var stat unix.Statfs_t
			// Device comparison is checked BEFORE choosing the directory's free point.
			if physicalDatabaseVolume(dirBefore, before) && unix.Statfs(directory, &stat) == nil && stat.Bsize > 0 && uint64(stat.Bavail) <= math.MaxUint64/uint64(stat.Bsize) {
				points.databaseFree = physicalUint(uint64(stat.Bavail) * uint64(stat.Bsize))
				points.databaseFreeAt = time.Now()
			}
			after, err := os.Lstat(name)
			if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
				points.databaseBytes = nil
				points.databaseAt = time.Time{}
				points.databaseFree = nil
				points.databaseFreeAt = time.Time{}
			} else {
				if after.Size() != before.Size() {
					points.databaseBytes = nil
					points.databaseAt = time.Time{}
				}
				if !physicalDatabaseVolume(dirBefore, after) {
					points.databaseFree = nil
					points.databaseFreeAt = time.Time{}
				}
			}
		}
		dirAfter, err := os.Lstat(directory)
		if err != nil || !dirAfter.IsDir() || !os.SameFile(dirBefore, dirAfter) {
			points.databaseBytes = nil
			points.databaseAt = time.Time{}
			points.databaseFree = nil
			points.databaseFreeAt = time.Time{}
		}
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	p.points.databaseBytes = points.databaseBytes
	p.points.databaseAt = points.databaseAt
	p.points.databaseFree = points.databaseFree
	p.points.databaseFreeAt = points.databaseFreeAt
	p.mu.Unlock()
}
