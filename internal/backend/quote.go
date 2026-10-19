package backend

import (
	"errors"
	"strings"
	"time"
)

// Money holds a fixed-point amount; nil AmountMicros means unspecified.
type Money struct {
	AmountMicros *int64
	Currency     string
}

// RateUnit identifies the denominator of a configured rate.
type RateUnit uint8

const (
	RateUnitUnspecified RateUnit = iota
	TargetHour
	CPUHour
	AcceleratorHour
)

// FactOrigin identifies how a fact was obtained.
type FactOrigin uint8

const (
	FactOriginUnspecified FactOrigin = iota
	Configured
)

// Quote is an operator-configured rate valid during [ObservedAt, ValidUntil).
type Quote struct {
	Rate                   Money
	Unit                   RateUnit
	Origin                 FactOrigin
	Source                 string
	ObservedAt, ValidUntil time.Time
}

// CapacityState is advisory and never reserves execution capacity.
type CapacityState uint8

const (
	CapacityUnspecified CapacityState = iota
	CapacityAvailable
	CapacityUnavailable
	CapacityError
)

// CapacitySnapshot is advisory capacity with an optional known slot count.
type CapacitySnapshot struct {
	State                  CapacityState
	AvailableSlots         *uint32
	Origin                 FactOrigin
	Source                 string
	ObservedAt, ValidUntil time.Time
}

// TargetAdvice contains optional configured rate and capacity facts.
type TargetAdvice struct {
	Quote    *Quote
	Capacity *CapacitySnapshot
}

// ValidQuoteCurrency reports whether s is a three-letter uppercase currency.
func ValidQuoteCurrency(s string) bool {
	if len(s) != 3 {
		return false
	}
	for _, c := range []byte(s) {
		if c < 'A' || c > 'Z' {
			return false
		}
	}
	return true
}

// ValidRateUnit reports whether unit is a supported rate denominator.
func ValidRateUnit(unit RateUnit) bool {
	return unit == TargetHour || unit == CPUHour || unit == AcceleratorHour
}

const maxConfiguredRate = int64(1_000_000_000_000)

func validQuoteSource(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for i, c := range []byte(s) {
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !alnum && (i == 0 || !strings.ContainsRune("._:-", rune(c))) {
			return false
		}
	}
	return true
}

func validQuoteTime(t time.Time) bool {
	_, offset := t.Zone()
	return !t.IsZero() && offset == 0 && t.Year() >= 1 && t.Year() <= 9999
}

func validQuoteWindow(observed, until time.Time) bool {
	return validQuoteTime(observed) && validQuoteTime(until) && observed.Before(until) && until.Sub(observed) <= 24*time.Hour
}

// ValidateAdvice checks static configured facts; freshness is checked per request.
func ValidateAdvice(a *TargetAdvice) error {
	if a == nil {
		return nil
	}
	if a.Quote == nil && a.Capacity == nil {
		return errors.New("empty target facts")
	}
	if p := a.Quote; p != nil {
		if p.Origin != Configured {
			return errors.New("invalid quote origin")
		}
		if p.Rate.AmountMicros == nil || *p.Rate.AmountMicros < 0 || *p.Rate.AmountMicros > maxConfiguredRate {
			return errors.New("invalid quote amount")
		}
		if !ValidQuoteCurrency(p.Rate.Currency) || !ValidRateUnit(p.Unit) || !validQuoteSource(p.Source) {
			return errors.New("invalid quote metadata")
		}
		if !validQuoteTime(p.ObservedAt) || !validQuoteTime(p.ValidUntil) {
			return errors.New("invalid quote timestamp")
		}
		if !validQuoteWindow(p.ObservedAt, p.ValidUntil) {
			return errors.New("invalid target facts validity window")
		}
	}
	if c := a.Capacity; c != nil {
		if c.Origin != Configured {
			return errors.New("invalid capacity origin")
		}
		if !validQuoteSource(c.Source) || !validQuoteWindow(c.ObservedAt, c.ValidUntil) {
			return errors.New("invalid capacity metadata")
		}
		if c.AvailableSlots != nil && *c.AvailableSlots > 1_000_000 {
			return errors.New("invalid capacity count")
		}
		switch c.State {
		case CapacityAvailable:
			if c.AvailableSlots != nil && *c.AvailableSlots == 0 {
				return errors.New("invalid capacity count")
			}
		case CapacityUnavailable:
			if c.AvailableSlots != nil && *c.AvailableSlots != 0 {
				return errors.New("invalid capacity count")
			}
		case CapacityError:
			if c.AvailableSlots != nil {
				return errors.New("invalid capacity count")
			}
		default:
			return errors.New("invalid capacity state")
		}
	}
	return nil
}

// CloneAdvice returns an independent copy, including optional scalar values.
func CloneAdvice(a *TargetAdvice) *TargetAdvice {
	if a == nil {
		return nil
	}
	out := *a
	if a.Quote != nil {
		p := *a.Quote
		if p.Rate.AmountMicros != nil {
			n := *p.Rate.AmountMicros
			p.Rate.AmountMicros = &n
		}
		out.Quote = &p
	}
	if a.Capacity != nil {
		c := *a.Capacity
		if c.AvailableSlots != nil {
			n := *c.AvailableSlots
			c.AvailableSlots = &n
		}
		out.Capacity = &c
	}
	return &out
}
