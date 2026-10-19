//go:build linux || darwin

package local

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"golang.org/x/sys/unix"
)

func TestRetainedLogsRejectSpecialReplacement(t *testing.T) {
	for _, mode := range []string{"fifo", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			b := retainedBackend(t)
			id := strings.Repeat("e", 32)
			startRetained(t, b, id, defaultTask(0))
			completed(t, b, id)
			path := filepath.Join(b.directory, id+".log")
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			fifo := path
			if mode == "symlink" {
				fifo = filepath.Join(t.TempDir(), "fifo")
			}
			if err := unix.Mkfifo(fifo, 0600); err != nil {
				t.Fatal(err)
			}
			if mode == "symlink" {
				if err := os.Symlink(fifo, path); err != nil {
					t.Fatal(err)
				}
			}
			result := make(chan error, 1)
			go func() { _, err := b.Logs(context.Background(), backend.Reference{ID: id}, 0, 16); result <- err }()
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("special replacement returned output")
				}
			case <-time.After(2 * time.Second):
				// Join the exact test-owned reader if a regression attempted a blocking
				// FIFO open. This handle is solely failure cleanup, never production proof.
				fd, err := unix.Open(fifo, unix.O_RDWR|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
				if err == nil {
					defer unix.Close(fd)
				}
				select {
				case <-result:
				case <-time.After(2 * time.Second):
					t.Error("test-owned log reader did not join")
				}
				t.Fatal("replacement FIFO/symlink blocked Logs")
			}
		})
	}
}

type allocationFileInfo struct {
	os.FileInfo
	blocks int64
}

func (f allocationFileInfo) Sys() any { return &syscall.Stat_t{Blocks: f.blocks} }

func TestAllocatedOutputBlocks(t *testing.T) {
	for _, test := range []struct {
		name   string
		blocks int64
		want   uint64
		bad    bool
	}{
		{"zero", 0, 0, false}, {"one-sector", 1, 512, false}, {"two-sectors", 2, 1024, false},
		{"negative", -1, 0, true}, {"largest", int64(math.MaxUint64 / 512), 18446744073709551104, false},
		{"overflow", int64(math.MaxUint64/512) + 1, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := allocatedOutput(allocationFileInfo{blocks: test.blocks})
			if (err != nil) != test.bad || got != test.want {
				t.Fatalf("allocated=%d,%v want%d,bad%v", got, err, test.want, test.bad)
			}
		})
	}
}
