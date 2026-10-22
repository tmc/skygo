package workeradmission

import (
	"errors"
	"math"
	"slices"
	"testing"
)

// four is a budget of four ranks of one CPU, 1 GiB and one accelerator each.
var four = Budget{Ranks: 4, CPUMillis: 4000, MemoryBytes: 4 << 30, Accelerators: 4}

// ranks returns n ranks of four's per-rank shape.
func ranks(n uint32) Demand {
	return Demand{Ranks: n, CPUMillis: 1000, MemoryBytes: 1 << 30, Accelerators: 1}
}

func TestFits(t *testing.T) {
	for _, tt := range []struct {
		name string
		b    Budget
		d    Demand
		want error
	}{
		{"exact", four, ranks(4), nil},
		{"one rank over", four, ranks(5), ErrExceedsBudget},
		{"cpu over", four, Demand{Ranks: 2, CPUMillis: 2001}, ErrExceedsBudget},
		{"memory over", four, Demand{Ranks: 2, MemoryBytes: 2<<30 + 1}, ErrExceedsBudget},
		{"accelerator over", four, Demand{Ranks: 2, Accelerators: 3}, ErrExceedsBudget},
		{"zero budget zero demand", Budget{Ranks: 1}, Demand{Ranks: 1}, nil},
		{"zero budget nonzero demand", Budget{Ranks: 1}, Demand{Ranks: 1, CPUMillis: 1}, ErrExceedsBudget},
		{"zero budget accelerators", Budget{Ranks: 1, CPUMillis: 1000}, Demand{Ranks: 1, CPUMillis: 1000, Accelerators: 1}, ErrExceedsBudget},
		{"zero budget ranks", Budget{CPUMillis: 1000}, Demand{Ranks: 1}, ErrExceedsBudget},
		{"no ranks", four, Demand{CPUMillis: 1}, ErrInvalid},
		{"overflow", Budget{Ranks: math.MaxUint64, CPUMillis: math.MaxUint64}, Demand{Ranks: 2, CPUMillis: math.MaxUint64}, ErrOverflow},
		{"largest", Budget{Ranks: math.MaxUint32, CPUMillis: math.MaxUint64}, Demand{Ranks: math.MaxUint32, CPUMillis: math.MaxUint64 / math.MaxUint32}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := Fits(tt.b, tt.d); !errors.Is(err, tt.want) || (err == nil) != (tt.want == nil) {
				t.Fatalf("Fits = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestAdmit(t *testing.T) {
	big := Demand{Ranks: 1, CPUMillis: math.MaxUint64}
	for _, tt := range []struct {
		name  string
		b     Budget
		held  []Held
		queue []Queued
		want  []string
		err   error
	}{
		{"empty queue", four, nil, nil, nil, nil},
		{"exact fit", four, nil, []Queued{{"a", ranks(4)}}, []string{"a"}, nil},
		{"several in order", four, nil, []Queued{{"a", ranks(1)}, {"b", ranks(2)}, {"c", ranks(1)}}, []string{"a", "b", "c"}, nil},
		{"stops at first misfit", four, nil, []Queued{{"a", ranks(3)}, {"b", ranks(3)}}, []string{"a"}, nil},
		// Whole demand, never a fitting prefix of a group's ranks.
		{"partial fit", Budget{Ranks: 4, CPUMillis: 2000}, nil, []Queued{{"a", Demand{Ranks: 3, CPUMillis: 1000}}}, nil, nil},
		// Strict FIFO: a later smaller group that fits waits behind the head.
		{"head blocks later", four, []Held{{"h", ranks(2)}}, []Queued{{"a", ranks(3)}, {"b", ranks(1)}}, nil, nil},
		{"held charged", four, []Held{{"h", ranks(3)}}, []Queued{{"a", ranks(1)}, {"b", ranks(1)}}, []string{"a"}, nil},
		// A reservation whose members are held or Unknown is still a reservation.
		{"held unknown members", Budget{Ranks: 4, CPUMillis: 3000}, []Held{{"unknown", Demand{Ranks: 1, CPUMillis: 2000}}}, []Queued{{"a", Demand{Ranks: 2, CPUMillis: 1000}}}, nil, nil},
		{"held over shrunk budget", Budget{Ranks: 1}, []Held{{"h", Demand{Ranks: 2}}}, []Queued{{"a", Demand{Ranks: 1}}}, nil, nil},
		{"accelerators charged", four, []Held{{"h", Demand{Ranks: 1, Accelerators: 4}}}, []Queued{{"a", ranks(1)}}, nil, nil},
		{"zero budget dimension", Budget{Ranks: 4}, nil, []Queued{{"a", Demand{Ranks: 1}}, {"b", Demand{Ranks: 1, MemoryBytes: 1}}}, []string{"a"}, nil},
		{"zero budget ranks", Budget{CPUMillis: 4000}, nil, []Queued{{"a", Demand{Ranks: 1}}}, nil, nil},
		{"queued sum overflow is a misfit", Budget{Ranks: 4, CPUMillis: math.MaxUint64}, nil, []Queued{{"a", big}, {"b", big}}, []string{"a"}, nil},
		{"empty id", four, nil, []Queued{{"", ranks(1)}}, nil, ErrInvalid},
		{"repeated queued id", four, nil, []Queued{{"a", ranks(1)}, {"a", ranks(1)}}, nil, ErrInvalid},
		{"queued id held", four, []Held{{"a", ranks(1)}}, []Queued{{"a", ranks(1)}}, nil, ErrInvalid},
		{"queued no ranks", four, nil, []Queued{{"a", ranks(1)}, {"b", Demand{}}}, nil, ErrInvalid},
		{"held no ranks", four, []Held{{"h", Demand{}}}, []Queued{{"a", ranks(1)}}, nil, ErrInvalid},
		{"queued overflow", four, nil, []Queued{{"a", ranks(1)}, {"b", Demand{Ranks: 2, MemoryBytes: math.MaxUint64}}}, nil, ErrOverflow},
		{"held overflow", four, []Held{{"h", Demand{Ranks: 3, CPUMillis: math.MaxUint64 / 2}}}, []Queued{{"a", ranks(1)}}, nil, ErrOverflow},
		{"held sum overflow", four, []Held{{"g", big}, {"h", big}}, []Queued{{"a", ranks(1)}}, nil, ErrOverflow},
		// Every group is validated before any decision, even behind a blocked head.
		{"blocked head then no ranks", Budget{Ranks: 1}, nil, []Queued{{"a", Demand{Ranks: 2}}, {"b", Demand{}}}, nil, ErrInvalid},
		{"blocked head then repeated id", Budget{Ranks: 1}, nil, []Queued{{"a", Demand{Ranks: 2}}, {"a", Demand{Ranks: 1}}}, nil, ErrInvalid},
		{"blocked head then overflow", Budget{Ranks: 1}, nil, []Queued{{"a", Demand{Ranks: 2}}, {"b", Demand{Ranks: 2, CPUMillis: math.MaxUint64}}}, nil, ErrOverflow},
	} {
		t.Run(tt.name, func(t *testing.T) {
			held, queue := slices.Clone(tt.held), slices.Clone(tt.queue)
			got, err := Admit(tt.b, tt.held, tt.queue)
			if !errors.Is(err, tt.err) || (err == nil) != (tt.err == nil) {
				t.Fatalf("Admit error = %v, want %v", err, tt.err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("Admit = %q, want %q", got, tt.want)
			}
			if !slices.Equal(held, tt.held) || !slices.Equal(queue, tt.queue) {
				t.Fatal("Admit changed its input")
			}
		})
	}
}
