package local

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/backend"
)

// Resource admission errors. Check reports ErrInvalidResources and
// ErrUnsatisfiable; Start may also report backend.ErrResourcesUnavailable when the
// request fits the host but its capacity or devices are reserved by other
// executions of the same Backend. ErrInventory reports a failed host probe.
var (
	ErrUnsupported      = errors.New("unsupported by local execution")
	ErrInvalidResources = errors.New("invalid local resource request")
	ErrUnsatisfiable    = errors.New("local host cannot satisfy resource request")
	ErrInventory        = errors.New("local resource inventory is unavailable")
)

// Bounds on the GPU inventory query. The tool runs with a finite deadline and
// its retained output is capped; exceeding either refuses the request.
const (
	gpuQueryTimeout   = 10 * time.Second
	gpuQueryMaxOutput = 64 << 10
	maxHostGPUs       = 64
)

var gpuQuery = []string{"nvidia-smi", "--query-gpu=uuid,name", "--format=csv,noheader,nounits"}

// hostGPU is one device reported by the inventory tool.
type hostGPU struct{ uuid, name string }

// hostProbe is a private seam for tests. A nil field uses the host.
type hostProbe struct {
	cpus   func() int
	memory func() (int64, error)
	gpus   func(context.Context) ([]hostGPU, error)
}

// hostInventory is one fresh probe of the quantities a task requested.
// GPUs are sorted by UUID.
type hostInventory struct {
	cpuMillis, memoryBytes int64
	gpus                   []hostGPU
}

// resourceReservation is the scheduler accounting for one execution. It
// limits nothing: CPU and memory are admission charges, and GPU visibility
// is advisory to cooperating CUDA processes. Guarded by Backend.mu.
type resourceReservation struct {
	id                     string
	cpuMillis, memoryBytes int64
	gpus                   []hostGPU
}

type resourceState struct {
	probe    hostProbe
	reserved map[string]*resourceReservation
}

// validateResources checks request syntax without probing the host.
func validateResources(t backend.Task) error {
	switch {
	case t.CPUMillis < 0 || t.MemoryBytes < 0 || t.AcceleratorCount < 0:
		return fmt.Errorf("%w: negative quantity", ErrInvalidResources)
	case (t.Accelerator == "") != (t.AcceleratorCount == 0):
		return fmt.Errorf("%w: accelerator name and count must both be set", ErrInvalidResources)
	case len(t.Accelerator) > 256 || !utf8.ValidString(t.Accelerator) || strings.ContainsAny(t.Accelerator, "\x00,\n"):
		return fmt.Errorf("%w: invalid accelerator name", ErrInvalidResources)
	case t.AcceleratorCount > maxHostGPUs:
		return fmt.Errorf("%w: requests %d accelerators", ErrUnsatisfiable, t.AcceleratorCount)
	}
	return nil
}

// checkResources reports whether this host could ever run t, ignoring
// reservations held by other executions.
func (b *Backend) checkResources(t backend.Task) error {
	ctx, cancel := context.WithTimeout(context.Background(), gpuQueryTimeout)
	defer cancel()
	inv, err := b.probeHost(ctx, t)
	if err != nil {
		return err
	}
	_, err = admit(t, inv, nil)
	return err
}

// probeHost reads only the inventory that t requests. It must not be called
// with Backend.mu held: the GPU query runs an external program.
func (b *Backend) probeHost(ctx context.Context, t backend.Task) (hostInventory, error) {
	var inv hostInventory
	p := b.resources.probe
	if t.CPUMillis > 0 {
		n := runtime.NumCPU()
		if p.cpus != nil {
			n = p.cpus()
		}
		inv.cpuMillis = int64(n) * 1000
	}
	if t.MemoryBytes > 0 {
		memory := hostMemory
		if p.memory != nil {
			memory = p.memory
		}
		total, err := memory()
		if err != nil {
			return hostInventory{}, fmt.Errorf("%w: memory: %v", ErrInventory, err)
		}
		inv.memoryBytes = total
	}
	if t.AcceleratorCount > 0 {
		gpus, err := b.hostGPUs(ctx)
		if err != nil {
			return hostInventory{}, err
		}
		inv.gpus = gpus
	}
	return inv, nil
}

// hostGPUs runs the GPU inventory query within gpuQueryTimeout, even when ctx
// has no deadline, and returns a private copy sorted by UUID.
func (b *Backend) hostGPUs(ctx context.Context) ([]hostGPU, error) {
	ctx, cancel := context.WithTimeout(ctx, gpuQueryTimeout)
	defer cancel()
	gpus := func(ctx context.Context) ([]hostGPU, error) { return queryGPUs(ctx, gpuQuery) }
	if b.resources.probe.gpus != nil {
		gpus = b.resources.probe.gpus
	}
	list, err := gpus(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: gpus: %w", ErrInventory, err)
	}
	list = slices.Clone(list)
	slices.SortFunc(list, func(a, b hostGPU) int { return strings.Compare(a.uuid, b.uuid) })
	return list, nil
}

// maxAcceleratorNames is the most accelerator names a target may advertise.
const maxAcceleratorNames = 32

// AcceleratorNames queries the host GPU inventory now and returns the sorted,
// distinct accelerator names a task may request: each device name without an
// optional leading "NVIDIA " prefix, so "NVIDIA GB10" is advertised as "GB10".
// It never consults reservations and caches nothing; Check remains the
// authoritative per-request decision. A host without GPUs returns no names.
// A failed or canceled query returns an error wrapping ErrInventory.
func (b *Backend) AcceleratorNames(ctx context.Context) ([]string, error) {
	gpus, err := b.hostGPUs(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, g := range gpus {
		names = append(names, gpuAlias(g.name))
	}
	slices.Sort(names)
	names = slices.Compact(names)
	if len(names) > maxAcceleratorNames {
		return nil, fmt.Errorf("%w: %d accelerator names exceed %d", ErrInventory, len(names), maxAcceleratorNames)
	}
	return names, nil
}

// gpuAlias is the name gpuMatches accepts for a device without its vendor prefix.
func gpuAlias(name string) string {
	const vendor = "NVIDIA "
	if len(name) > len(vendor) && strings.EqualFold(name[:len(vendor)], vendor) {
		return name[len(vendor):]
	}
	return name
}

// admit selects resources for t from inv, excluding those held. The selection
// is deterministic: the first free matching GPUs in UUID order.
func admit(t backend.Task, inv hostInventory, held map[string]*resourceReservation) (*resourceReservation, error) {
	var cpu, memory int64
	used := make(map[string]bool)
	for _, r := range held {
		cpu += r.cpuMillis
		memory += r.memoryBytes
		for _, g := range r.gpus {
			used[g.uuid] = true
		}
	}
	r := &resourceReservation{cpuMillis: t.CPUMillis, memoryBytes: t.MemoryBytes}
	if t.CPUMillis > inv.cpuMillis {
		return nil, fmt.Errorf("%w: requests %d millicpu of %d", ErrUnsatisfiable, t.CPUMillis, inv.cpuMillis)
	}
	if t.MemoryBytes > inv.memoryBytes {
		return nil, fmt.Errorf("%w: requests %d memory bytes of %d", ErrUnsatisfiable, t.MemoryBytes, inv.memoryBytes)
	}
	if t.AcceleratorCount > 0 {
		var matching, free []hostGPU
		for _, g := range inv.gpus {
			if gpuMatches(g.name, t.Accelerator) {
				matching = append(matching, g)
				if !used[g.uuid] {
					free = append(free, g)
				}
			}
		}
		if len(matching) < int(t.AcceleratorCount) {
			return nil, fmt.Errorf("%w: requests %s:%d, host has %d", ErrUnsatisfiable, t.Accelerator, t.AcceleratorCount, len(matching))
		}
		if len(free) < int(t.AcceleratorCount) {
			return nil, fmt.Errorf("%w: requests %s:%d, %d free", backend.ErrResourcesUnavailable, t.Accelerator, t.AcceleratorCount, len(free))
		}
		r.gpus = free[:t.AcceleratorCount:t.AcceleratorCount]
	}
	// Requests are bounded by capacity above, so these differences cannot overflow.
	if t.CPUMillis > inv.cpuMillis-min(cpu, inv.cpuMillis) {
		return nil, fmt.Errorf("%w: requests %d millicpu, %d reserved of %d", backend.ErrResourcesUnavailable, t.CPUMillis, cpu, inv.cpuMillis)
	}
	if t.MemoryBytes > inv.memoryBytes-min(memory, inv.memoryBytes) {
		return nil, fmt.Errorf("%w: requests %d memory bytes, %d reserved of %d", backend.ErrResourcesUnavailable, t.MemoryBytes, memory, inv.memoryBytes)
	}
	return r, nil
}

// reserveResources holds Backend.mu. It records a reservation for id against
// inv, a fresh probe taken by the caller before acquiring the lock.
func (b *Backend) reserveResources(id string, t backend.Task, inv hostInventory) (*resourceReservation, error) {
	held, err := b.allResourceReservationsLocked()
	if err != nil {
		return nil, err
	}
	if t.CPUMillis == 0 && t.MemoryBytes == 0 && t.AcceleratorCount == 0 {
		return nil, nil
	}
	if _, ok := held[id]; ok {
		return nil, errors.New("job already started")
	}
	r, err := admit(t, inv, held)
	if err != nil {
		return nil, err
	}
	r.id = id
	if b.resources.reserved == nil {
		b.resources.reserved = make(map[string]*resourceReservation)
	}
	b.resources.reserved[id] = r
	return r, nil
}

// releaseResources holds Backend.mu. Callers must hold positive closure of
// every process started under r; status or cancellation alone is not enough.
func (b *Backend) releaseResources(r *resourceReservation) {
	if r != nil && b.resources.reserved[r.id] == r {
		delete(b.resources.reserved, r.id)
	}
}

// installResources holds Backend.mu after an ordinary or retry start for id.
// It attaches r to the installed execution and reports whether it did. A
// start that failed preflight, before any process existed, is not attached.
func (b *Backend) installResources(id string, r *resourceReservation, preflight bool) bool {
	e := b.jobs[id]
	if r == nil || e == nil || preflight && e.cmd.Process == nil {
		return false
	}
	e.resources = r
	return true
}

// visibleDevices replaces any ambient or task-supplied device selection with
// the reserved GPUs. An execution without GPUs sees none. This only steers
// cooperating CUDA and NVIDIA container tooling; it is not device isolation.
func visibleDevices(env []string, r *resourceReservation) []string {
	out := make([]string, 0, len(env)+2)
	for _, kv := range env {
		if strings.HasPrefix(kv, "CUDA_VISIBLE_DEVICES=") || strings.HasPrefix(kv, "NVIDIA_VISIBLE_DEVICES=") {
			continue
		}
		out = append(out, kv)
	}
	var ids []string
	if r != nil {
		for _, g := range r.gpus {
			ids = append(ids, g.uuid)
		}
	}
	nvidia := "void"
	if len(ids) != 0 {
		nvidia = strings.Join(ids, ",")
	}
	return append(out, "CUDA_VISIBLE_DEVICES="+strings.Join(ids, ","), "NVIDIA_VISIBLE_DEVICES="+nvidia)
}

// gpuMatches reports whether a device name satisfies a requested accelerator
// name: equal ignoring case, or equal after a leading "NVIDIA " vendor prefix.
func gpuMatches(name, want string) bool {
	return strings.EqualFold(name, want) || strings.EqualFold(gpuAlias(name), want)
}

// queryGPUs runs argv with a finite deadline and bounded output and parses
// its "uuid, name" CSV lines.
func queryGPUs(ctx context.Context, argv []string) ([]hostGPU, error) {
	ctx, cancel := context.WithTimeout(ctx, gpuQueryTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	stdout := &boundedBuffer{max: gpuQueryMaxOutput}
	stderr := &boundedBuffer{max: 4 << 10}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.WaitDelay = time.Second
	if err := cmd.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("%s: %w", argv[0], ctxErr)
		}
		if stdout.overflow {
			return nil, fmt.Errorf("%s: output exceeds %d bytes", argv[0], gpuQueryMaxOutput)
		}
		if msg := strings.TrimSpace(retryReason(stderr.buf.String())); msg != "" {
			return nil, fmt.Errorf("%s: %w: %s", argv[0], err, msg)
		}
		return nil, fmt.Errorf("%s: %w", argv[0], err)
	}
	return parseGPUs(stdout.buf.Bytes())
}

var gpuUUID = regexp.MustCompile(`^GPU-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// parseGPUs parses nvidia-smi CSV output strictly: every nonempty line must
// hold a distinct GPU UUID and a printable name. UUID hex is lowercased before
// duplicate detection, so case aliases of one device are refused. The result
// is sorted by UUID.
func parseGPUs(data []byte) ([]hostGPU, error) {
	if !utf8.Valid(data) {
		return nil, errors.New("gpu inventory is not UTF-8")
	}
	var gpus []hostGPU
	seen := make(map[string]bool)
	for line := range strings.Lines(string(data)) {
		line = strings.TrimRight(line, "\r\n")
		if strings.TrimSpace(line) == "" {
			continue
		}
		uuid, name, ok := strings.Cut(line, ",")
		uuid, name = strings.TrimSpace(uuid), strings.TrimSpace(name)
		if !ok || !gpuUUID.MatchString(uuid) {
			return nil, fmt.Errorf("invalid gpu inventory line %q", retryReason(line))
		}
		uuid = "GPU-" + strings.ToLower(uuid[len("GPU-"):])
		if name == "" || len(name) > 256 || strings.ContainsFunc(name, func(r rune) bool { return r < 0x20 || r == 0x7f || r == ',' }) {
			return nil, fmt.Errorf("invalid gpu name %q", retryReason(name))
		}
		if seen[uuid] {
			return nil, fmt.Errorf("duplicate gpu %s", uuid)
		}
		seen[uuid] = true
		if len(gpus) == maxHostGPUs {
			return nil, fmt.Errorf("gpu inventory exceeds %d devices", maxHostGPUs)
		}
		gpus = append(gpus, hostGPU{uuid: uuid, name: name})
	}
	slices.SortFunc(gpus, func(a, b hostGPU) int { return strings.Compare(a.uuid, b.uuid) })
	return gpus, nil
}

// boundedBuffer keeps at most max bytes and fails writes beyond that.
type boundedBuffer struct {
	buf      bytes.Buffer
	max      int
	overflow bool
}

func (w *boundedBuffer) Write(p []byte) (int, error) {
	if w.buf.Len()+len(p) > w.max {
		w.overflow = true
		return 0, errors.New("output limit exceeded")
	}
	return w.buf.Write(p)
}
