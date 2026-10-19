//go:build linux || darwin

package store

import (
	"context"
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func openPhysicalFile(path string) (*os.File, error) {
	// O_NOFOLLOW covers the leaf only. The operator chooses the directory;
	// its ancestors are resolved normally, not treated as a sandbox boundary.
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		unix.Close(fd)
		return nil, errors.New("physical file unavailable")
	}
	return f, nil
}

func physicalDescriptor(ctx context.Context, f *os.File, out *PhysicalInspection) (os.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Size() < 2048 || stat.Blocks < 0 || uint64(stat.Blocks) > ^uint64(0)/512 {
		return nil, errors.New("physical descriptor unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var fs unix.Statfs_t
	if err := unix.Fstatfs(int(f.Fd()), &fs); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fs.Bsize <= 0 || uint64(fs.Bavail) > ^uint64(0)/uint64(fs.Bsize) {
		return nil, errors.New("physical free space unavailable")
	}
	size, allocated, free := uint64(info.Size()), uint64(stat.Blocks)*512, uint64(fs.Bavail)*uint64(fs.Bsize)
	device, inode := uint64(stat.Dev), uint64(stat.Ino)
	out.FileBytes = &size
	out.AllocatedBytes = &allocated
	out.FilesystemFreeBytes = &free
	out.Device = &device
	out.Inode = &inode
	return info, nil
}
