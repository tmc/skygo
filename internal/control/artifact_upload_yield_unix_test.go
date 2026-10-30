//go:build linux || darwin

package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/store"
)

// Own the actual namespace instead of t.TempDir, whose automatic RemoveAll is
// independent of the original worker/descriptor settlement outcome.
type uploadGrowthResources struct {
	directory            string
	root                 *os.Root
	files                *artifact.Files
	store                *store.Store
	control              *Control
	maintenance          *maintenanceState
	cancel               context.CancelFunc
	inputClosed, keeping bool
	held                 error
	expectedHeld         error
}

func newUploadGrowthResources(t *testing.T) *uploadGrowthResources {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261019-upload-growth-tests")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(parent, "physical-")
	if err != nil {
		t.Fatal(err)
	}
	f := &uploadGrowthResources{directory: dir}
	t.Cleanup(func() { f.retire(t) })
	fail := func(err error) { f.held = err; t.Fatal(err) }
	name := filepath.Join(dir, "root")
	if err := os.Mkdir(name, 0700); err != nil {
		fail(err)
	}
	f.root, err = os.OpenRoot(name)
	if err != nil {
		fail(err)
	}
	info, err := f.root.Lstat(".")
	if err != nil {
		fail(err)
	}
	// Reuse the actual descriptor identity conversion, never an invented inode.
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		fail(errors.New("missing original root identity"))
	}
	id := artifact.FileID{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}
	facts, err := artifact.Initialize(f.root, id, strings.Repeat("a", 32))
	if err != nil {
		fail(err)
	}
	f.files, err = artifact.Open(f.root, id)
	if err != nil {
		fail(err)
	}
	state := filepath.Join(dir, "state")
	if _, err := store.InstallArtifacts(state, store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}); err != nil {
		fail(err)
	}
	f.store, err = store.Open(state)
	if err != nil {
		fail(err)
	}
	service, err := newArtifactService(f.store, f.files)
	if err != nil {
		fail(err)
	}
	f.control = &Control{shutdown: newShutdownState(), store: f.store, artifacts: service, engine: testEngine}
	return f
}

func uploadGrowthPhysicalFixture(t *testing.T) (*Control, *os.Root) {
	t.Helper()
	f := newUploadGrowthResources(t)
	return f.control, f.root
}

func (f *uploadGrowthResources) retain(t *testing.T, err error) {
	t.Helper()
	if f.held == nil {
		f.held = err
	}
	if f.expectedHeld != nil && errors.Is(f.held, f.expectedHeld) {
		t.Logf("expected HOLD original upload fixture %s: %v", f.directory, f.held)
	} else {
		t.Errorf("HOLD original upload fixture %s: %v", f.directory, f.held)
	}
	if !f.keeping {
		f.keeping = true
		original := f.held
		hold := make(chan struct{})
		go func() { <-hold; runtime.KeepAlive(f); runtime.KeepAlive(original) }()
	}
}

func (f *uploadGrowthResources) retire(t *testing.T) {
	t.Helper()
	if f.held != nil {
		f.retain(t, f.held)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if m := f.maintenance; m != nil {
		if m.pending != nil || m.received != nil {
			f.cancel()
			if !f.inputClosed {
				close(m.input)
				f.inputClosed = true
			}
			f.retain(t, errors.New("original maintenance request/result remains unintegrated"))
			return
		}
		if f.control.physicalRegistered() {
			f.control.requestPhysicalClose()
			if err := f.control.dispatchPhysical(m); err != nil {
				f.retain(t, err)
				return
			}
			select {
			case result := <-m.results:
				if err := f.control.receiveMaintenance(ctx, m, result); err != nil {
					f.retain(t, err)
					return
				}
				if result.physical == nil {
					f.retain(t, errors.New("original maintenance close result missing"))
					return
				}
				if result.physical.Err != nil || result.physical.Registered {
					f.retain(t, fmt.Errorf("original maintenance scan close remains held: %w", errors.Join(result.physical.Err, ErrUnavailable)))
					return
				}
			case <-ctx.Done():
				f.retain(t, ctx.Err())
				return
			}
		}
		f.cancel()
		if !f.inputClosed {
			close(m.input)
			f.inputClosed = true
		}
		select {
		case <-m.done:
		case <-ctx.Done():
			f.retain(t, ctx.Err())
			return
		}
	} else if f.control != nil && f.control.physicalRegistered() {
		f.control.requestPhysicalClose()
		q := f.control.physicalQuantum(ctx)
		if q.Err != nil || q.Registered || q.Held {
			f.retain(t, fmt.Errorf("original finite scan close: %w", errors.Join(q.Err, ErrUnavailable)))
			return
		}
	}
	if f.control != nil {
		if err := f.control.DrainArtifacts(ctx); err != nil {
			f.retain(t, err)
			return
		}
	}
	if f.files != nil {
		if err := f.files.Close(); err != nil {
			f.retain(t, err)
			return
		}
	}
	if f.root != nil {
		if err := f.root.Close(); err != nil {
			f.retain(t, err)
			return
		}
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			f.retain(t, err)
			return
		}
	}
	if err := os.RemoveAll(f.directory); err != nil {
		f.retain(t, err)
	}
}

func uploadGrowthWorkerQuantum(t *testing.T, ctx context.Context, c *Control, m *maintenanceState) physicalQuantumResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := c.dispatchPhysical(m); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-m.results:
		if result.physical == nil {
			t.Fatalf("original worker returned no physical result: %v", result.err)
		}
		if err := c.receiveMaintenance(ctx, m, result); err != nil {
			t.Fatal(err)
		}
		return *result.physical
	case <-ctx.Done():
		t.Fatal("original maintenance quantum did not return")
		return physicalQuantumResult{}
	}
}

// The large tree is deliberately unexplained fixture-owned data: it tests real
// cursor progress, NEVER usable inventory. Only after its positive original
// scan Close and removal do we accept a fresh explained sample/finalize.
// All tests remain UNRUN; large explained Local/native qualification is separate.
func TestUploadBusyFinalizePreservesActualYieldedWorkerCursor(t *testing.T) {
	f := newUploadGrowthResources(t)
	c, root := f.control, f.root
	data, digest := uploadGrowthArchive(t)
	u := uploadGrowthBegin(t, c, data, digest)
	for offset := 0; offset < len(data); {
		end := min(offset+(64<<10), len(data))
		a := uploadGrowthCall(t, c)
		next, err := c.WriteArtifactChunk(context.Background(), testActor(), a, u.Reference.ID, int64(offset), data[offset:end])
		a.Done()
		if err != nil {
			t.Fatal(err)
		}
		u = next
		offset = end
	}
	a := uploadGrowthCall(t, c)
	_, err := c.FinalizeArtifactUpload(context.Background(), testActor(), a, u.Reference.ID)
	a.Done()
	if !errors.Is(err, artifact.ErrFilesBusy) {
		t.Fatalf("first publishing transition: %v", err)
	}
	if err := root.Mkdir("bulk", 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4097; i++ {
		file, err := root.OpenFile(fmt.Sprintf("bulk/%04d", i), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			f.held = err
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	f.maintenance = newMaintenance(ctx)
	m := f.maintenance
	yielded := uploadGrowthWorkerQuantum(t, ctx, c, m)
	if yielded.Err != nil || !yielded.Registered || yielded.Complete {
		t.Fatalf("actual >4096-visit yield: %+v", yielded)
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	frame := p.frame
	if frame == nil {
		p.mu.Unlock()
		t.Fatal("actual original scan lost")
	}
	scan := frame.scan
	p.mu.Unlock()
	if scan == nil {
		t.Fatal("actual original cursor lost")
	}
	for range 4 {
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		}
		a := uploadGrowthCall(t, c)
		_, err := c.FinalizeArtifactUpload(ctx, testActor(), a, u.Reference.ID)
		a.Done()
		if !errors.Is(err, artifact.ErrFilesBusy) {
			t.Fatalf("ordinary late guard refusal: %v", err)
		}
		p.mu.Lock()
		same := p.frame == frame && p.frame.scan == scan && p.frame.serial == yielded.Serial && !p.frame.invalidated
		p.mu.Unlock()
		if !same || m.physicalSerial != yielded.Serial {
			t.Fatal("no-link Busy restarted the actual yielded frame/cursor")
		}
	}
	// This must execute the original cursor's NEXT Step, not an invalidation
	// Close quantum. EOF is rejected solely because bulk has no Store subject.
	exhausted := uploadGrowthWorkerQuantum(t, ctx, c, m)
	if exhausted.Serial != yielded.Serial || exhausted.Registered || exhausted.Complete || !errors.Is(exhausted.Err, ErrUnavailable) {
		t.Fatalf("original next Step/EOF/refusal: %+v", exhausted)
	}
	p.mu.Lock()
	refused := p.frame == nil && p.cached == nil
	p.mu.Unlock()
	if !refused {
		t.Fatal("unexplained large interval was promoted or not positively closed")
	}
	// Test authority removes only the exact tree it created, after scan Close.
	for i := 0; i < 4097; i++ {
		if err := root.Remove(fmt.Sprintf("bulk/%04d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := root.Remove("bulk"); err != nil {
		t.Fatal(err)
	}
	directory, err := root.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if closeErr != nil {
		f.held = closeErr
	}
	if err := errors.Join(syncErr, closeErr); err != nil {
		t.Fatal(err)
	}
	c.physicalChanged()
	fresh := uploadGrowthWorkerQuantum(t, ctx, c, m)
	if fresh.Err != nil || !fresh.Complete || fresh.Registered {
		t.Fatalf("same worker's genuinely explained fresh sample: %+v", fresh)
	}
	a = uploadGrowthCall(t, c)
	ref, err := c.FinalizeArtifactUpload(ctx, testActor(), a, u.Reference.ID)
	a.Done()
	if err != nil || ref.ID != u.Reference.ID || ref.SHA256 != digest {
		t.Fatalf("actual same-ID finalization after explained sample: %+v %v", ref, err)
	}
}
