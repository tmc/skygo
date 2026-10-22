package workeradmission

import (
	"errors"
	"fmt"
	"math/bits"
)

var (
	// ErrInvalid reports malformed input: an empty or repeated group ID, or
	// a demand with no ranks.
	ErrInvalid = errors.New("invalid admission input")
	// ErrOverflow reports a whole demand or reservation sum that does not
	// fit in 64 bits.
	ErrOverflow = errors.New("admission demand overflows")
	// ErrExceedsBudget reports a demand that can never fit the budget.
	ErrExceedsBudget = errors.New("demand exceeds budget")
)

// Demand is one group's requirement on one target. Every field but Ranks is
// per rank; the whole demand is Ranks times each.
type Demand struct {
	Ranks        uint32
	CPUMillis    uint64
	MemoryBytes  uint64
	Accelerators uint32
}

// Budget is an operator-declared per-target gang budget. A zero field is zero
// capacity, not unlimited.
type Budget struct {
	Ranks        uint64
	CPUMillis    uint64
	MemoryBytes  uint64
	Accelerators uint64
}

// Held is an admitted group's reservation. It stays charged while any member
// may be active, including held or Unknown members.
type Held struct {
	ID     string
	Demand Demand
}

// Queued is a waiting group. A queue is in acceptance order.
type Queued struct {
	ID     string
	Demand Demand
}

// Fits reports whether d can ever be admitted under b, for refusal before
// acceptance. It returns an error wrapping ErrInvalid, ErrOverflow or
// ErrExceedsBudget.
func Fits(b Budget, d Demand) error {
	need, err := whole(d)
	if err != nil {
		return err
	}
	return within(need, b)
}

// Admit returns the IDs of the groups admitted from the head of queue, in
// order. A group is admitted only if its whole demand fits b less the held
// reservations and the groups admitted before it; Admit stops at the first
// group that does not fit. On any error it admits nothing.
func Admit(b Budget, held []Held, queue []Queued) ([]string, error) {
	seen := make(map[string]bool)
	var used Budget
	for _, h := range held {
		if err := checkID(seen, h.ID); err != nil {
			return nil, err
		}
		need, err := whole(h.Demand)
		if err != nil {
			return nil, fmt.Errorf("held %q: %w", h.ID, err)
		}
		if used, err = add(used, need); err != nil {
			return nil, fmt.Errorf("held %q: %w", h.ID, err)
		}
	}
	needs := make([]Budget, len(queue))
	for i, q := range queue {
		if err := checkID(seen, q.ID); err != nil {
			return nil, err
		}
		need, err := whole(q.Demand)
		if err != nil {
			return nil, fmt.Errorf("queued %q: %w", q.ID, err)
		}
		needs[i] = need
	}
	var admitted []string
	for i, q := range queue {
		next, err := add(used, needs[i])
		if err != nil || within(next, b) != nil {
			break
		}
		used = next
		admitted = append(admitted, q.ID)
	}
	return admitted, nil
}

func checkID(seen map[string]bool, id string) error {
	if id == "" {
		return fmt.Errorf("%w: empty group id", ErrInvalid)
	}
	if seen[id] {
		return fmt.Errorf("%w: repeated group id %q", ErrInvalid, id)
	}
	seen[id] = true
	return nil
}

// whole returns d's whole demand: Ranks times each per-rank field.
func whole(d Demand) (Budget, error) {
	if d.Ranks == 0 {
		return Budget{}, fmt.Errorf("%w: demand has no ranks", ErrInvalid)
	}
	n := uint64(d.Ranks)
	cpu, err := mul(n, d.CPUMillis)
	if err != nil {
		return Budget{}, err
	}
	memory, err := mul(n, d.MemoryBytes)
	if err != nil {
		return Budget{}, err
	}
	accelerators, err := mul(n, uint64(d.Accelerators))
	if err != nil {
		return Budget{}, err
	}
	return Budget{Ranks: n, CPUMillis: cpu, MemoryBytes: memory, Accelerators: accelerators}, nil
}

func mul(a, b uint64) (uint64, error) {
	hi, lo := bits.Mul64(a, b)
	if hi != 0 {
		return 0, ErrOverflow
	}
	return lo, nil
}

func add(a, b Budget) (Budget, error) {
	var carry, c uint64
	a.Ranks, c = bits.Add64(a.Ranks, b.Ranks, 0)
	carry |= c
	a.CPUMillis, c = bits.Add64(a.CPUMillis, b.CPUMillis, 0)
	carry |= c
	a.MemoryBytes, c = bits.Add64(a.MemoryBytes, b.MemoryBytes, 0)
	carry |= c
	a.Accelerators, c = bits.Add64(a.Accelerators, b.Accelerators, 0)
	carry |= c
	if carry != 0 {
		return Budget{}, ErrOverflow
	}
	return a, nil
}

// within reports whether need fits b in every dimension.
func within(need, b Budget) error {
	switch {
	case need.Ranks > b.Ranks:
		return fmt.Errorf("%w: ranks %d of %d", ErrExceedsBudget, need.Ranks, b.Ranks)
	case need.CPUMillis > b.CPUMillis:
		return fmt.Errorf("%w: cpu %dm of %dm", ErrExceedsBudget, need.CPUMillis, b.CPUMillis)
	case need.MemoryBytes > b.MemoryBytes:
		return fmt.Errorf("%w: memory %d of %d bytes", ErrExceedsBudget, need.MemoryBytes, b.MemoryBytes)
	case need.Accelerators > b.Accelerators:
		return fmt.Errorf("%w: accelerators %d of %d", ErrExceedsBudget, need.Accelerators, b.Accelerators)
	}
	return nil
}
