package local

import (
	"errors"
	"fmt"
	"strings"

	"github.com/tmc/skygo/internal/backend"
)

// allResourceReservationsLocked reconstructs the admission view while b.mu is
// held. Ordinary executions keep their original reservations. Durable jobs own
// their recorded allocation until that same job has a validated terminal result;
// they do not also enter the ordinary map. No probe or filesystem I/O runs here.
func (b *Backend) allResourceReservationsLocked() (map[string]*resourceReservation, error) {
	held := make(map[string]*resourceReservation, len(b.resources.reserved))
	for id, r := range b.resources.reserved {
		if r == nil || r.id != id || !validID.MatchString(id) {
			return nil, fmt.Errorf("%w: invalid ordinary resource reservation", ErrInventory)
		}
		held[id] = r
	}
	for _, d := range b.durableReservationsLocked() {
		if !d.Valid || !validID.MatchString(d.ID) {
			return nil, fmt.Errorf("%w: durable allocation is unknown", ErrInventory)
		}
		if err := validateDurableResourceLaunch(d.Task, d.Launch); err != nil {
			return nil, err
		}
		if _, ok := held[d.ID]; ok {
			return nil, fmt.Errorf("%w: duplicate resource owner", ErrInventory)
		}
		r := &resourceReservation{id: d.ID, cpuMillis: d.Launch.CPUMillis, memoryBytes: d.Launch.MemoryBytes}
		for _, uuid := range d.Launch.GPUs {
			r.gpus = append(r.gpus, hostGPU{uuid: uuid})
		}
		held[d.ID] = r
	}
	var cpu, memory int64
	used := make(map[string]bool)
	for _, r := range held {
		if r.cpuMillis < 0 || r.memoryBytes < 0 || len(r.gpus) > maxHostGPUs ||
			r.cpuMillis > (1<<63-1)-cpu || r.memoryBytes > (1<<63-1)-memory {
			return nil, fmt.Errorf("%w: invalid resource accounting", ErrInventory)
		}
		cpu += r.cpuMillis
		memory += r.memoryBytes
		for _, g := range r.gpus {
			if !canonicalResourceGPU(g.uuid) || used[g.uuid] {
				return nil, fmt.Errorf("%w: GPU allocation is not distinct", ErrInventory)
			}
			used[g.uuid] = true
		}
	}
	return held, nil
}

// durableLaunchFor selects once from the caller's fresh inventory, with b.mu
// held. startDurable installs the result in d.jobs before unlocking; that is the
// reservation. A failed pre-launch directory creation publishes no reservation.
// The supervisor consumes this fixed placement and never probes or selects again.
func (b *Backend) durableLaunchFor(id string, t backend.Task, inv hostInventory) (durableLaunch, error) {
	if !validID.MatchString(id) {
		return durableLaunch{}, errors.New("invalid job ID")
	}
	if err := validateResources(t); err != nil {
		return durableLaunch{}, err
	}
	held, err := b.allResourceReservationsLocked()
	if err != nil {
		return durableLaunch{}, err
	}
	if _, ok := held[id]; ok {
		return durableLaunch{}, errors.New("job already started")
	}
	r, err := admit(t, inv, held)
	if err != nil {
		return durableLaunch{}, err
	}
	launch := durableLaunch{CPUMillis: r.cpuMillis, MemoryBytes: r.memoryBytes}
	for _, g := range r.gpus {
		launch.GPUs = append(launch.GPUs, g.uuid)
	}
	launch.Env = durableResourceEnvironment(launch.GPUs)
	if err := validateDurableResourceLaunch(t, launch); err != nil {
		return durableLaunch{}, err
	}
	return launch, nil
}

// validateDurableResourceLaunch checks persisted placement without consulting
// current hardware. Invalid or unknown metadata never means an empty allocation.
func validateDurableResourceLaunch(t backend.Task, launch durableLaunch) error {
	if err := validateResources(t); err != nil {
		return fmt.Errorf("%w: invalid durable resource request: %v", ErrInventory, err)
	}
	if launch.CPUMillis != t.CPUMillis || launch.MemoryBytes != t.MemoryBytes ||
		len(launch.GPUs) != int(t.AcceleratorCount) {
		return fmt.Errorf("%w: durable allocation differs from accepted task", ErrInventory)
	}
	seen := make(map[string]bool, len(launch.GPUs))
	for _, uuid := range launch.GPUs {
		if !canonicalResourceGPU(uuid) || seen[uuid] {
			return fmt.Errorf("%w: invalid durable GPU allocation", ErrInventory)
		}
		seen[uuid] = true
	}
	want := durableResourceEnvironment(launch.GPUs)
	cuda, cudaOK := launch.Env["CUDA_VISIBLE_DEVICES"]
	nvidia, nvidiaOK := launch.Env["NVIDIA_VISIBLE_DEVICES"]
	if len(launch.Env) != len(want) || !cudaOK || !nvidiaOK ||
		cuda != want["CUDA_VISIBLE_DEVICES"] || nvidia != want["NVIDIA_VISIBLE_DEVICES"] {
		return fmt.Errorf("%w: durable GPU visibility differs from allocation", ErrInventory)
	}
	return nil
}

func canonicalResourceGPU(uuid string) bool {
	return gpuUUID.MatchString(uuid) && uuid == "GPU-"+strings.ToLower(uuid[len("GPU-"):])
}

func durableResourceEnvironment(gpus []string) map[string]string {
	cuda := strings.Join(gpus, ",")
	nvidia := cuda
	if len(gpus) == 0 {
		nvidia = "void"
	}
	return map[string]string{"CUDA_VISIBLE_DEVICES": cuda, "NVIDIA_VISIBLE_DEVICES": nvidia}
}
