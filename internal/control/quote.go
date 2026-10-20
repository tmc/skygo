package control

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
)

// PriceStatus describes whether a rate is comparable at the captured instant.
type PriceStatus string

const (
	PriceMissing          PriceStatus = "missing"
	PriceFuture           PriceStatus = "future"
	PriceStale            PriceStatus = "stale"
	PriceCurrencyMismatch PriceStatus = "currency_mismatch"
	PriceUnitMismatch     PriceStatus = "unit_mismatch"
	PriceComparable       PriceStatus = "comparable"
)

// FactFreshness describes the captured freshness of an advisory snapshot.
type FactFreshness string

const (
	FreshnessUnknown FactFreshness = "unknown"
	FreshnessFuture  FactFreshness = "future"
	FreshnessStale   FactFreshness = "stale"
	FreshnessFresh   FactFreshness = "fresh"
)

// EstimateStatus explains whether a compute cost could be estimated.
type EstimateStatus string

const (
	EstimateQuoteNotComparable  EstimateStatus = "quote_not_comparable"
	EstimateDurationUnspecified EstimateStatus = "duration_unspecified"
	EstimateQuantityUnspecified EstimateStatus = "quantity_unspecified"
	EstimateAvailable           EstimateStatus = "available"
)

// QuoteOutcome describes whether a recommendation was selected.
type QuoteOutcome string

const (
	QuoteSelected          QuoteOutcome = "selected"
	QuoteNoEligibleTarget  QuoteOutcome = "no_eligible_target"
	QuoteNoComparableQuote QuoteOutcome = "no_comparable_quote"
)

// QuoteCandidate reports one target without excluding ineligible alternatives.
type QuoteCandidate struct {
	Target               string
	Eligible             bool
	Rejection            string
	Quote                *backend.Quote
	Capacity             *backend.CapacitySnapshot
	PriceStatus          PriceStatus
	CapacityFreshness    FactFreshness
	CapacityWarning      bool
	EstimatedComputeCost *backend.Money
	EstimateStatus       EstimateStatus
}

// QuotePlan contains advisory rate comparison, never execution placement.
type QuotePlan struct {
	AsOf                    time.Time
	Currency                string
	Unit                    backend.RateUnit
	BillableDurationSeconds *int64
	Outcome                 QuoteOutcome
	SelectedTarget          string
	ComparisonCount         uint32
	UnpricedEligibleCount   uint32
	Candidates              []QuoteCandidate
}

// ErrQuoteMetadata rejects identities that cannot be represented by QuoteTask.
var ErrQuoteMetadata = errors.New("quote target metadata is invalid")

const maxQuoteRate = int64(1_000_000_000_000)
const maxQuoteDuration = int64(604800)

// checkTarget applies the same hard constraints to ordinary and advisory plans.
func checkTarget(t backend.Task, target backend.Target, requireOutputStorage bool) error {
	return checkWorkspaceTarget(t, target, requireOutputStorage, nil)
}

func checkWorkspaceTarget(t backend.Task, target backend.Target, requireOutputStorage bool, workspace *workspaceTarget) error {
	switch {
	case t.Workdir != nil && !workspace.matches(target):
		return errors.New("local workspace service is unsupported")
	case t.Target != "" && t.Target != target.Name:
		return errors.New("target constraint differs")
	case requireOutputStorage && target.OutputStorage == backend.UnspecifiedOutput:
		return errors.New("output storage is unspecified")
	case !matchesLabels(target.Labels, t.TargetLabels):
		return errors.New("target labels differ")
	case t.Retry != nil && !target.SupportsRetry:
		return errors.New("finite job retries are unsupported")
	case (t.CPULimitMillis != 0 || t.MemoryLimitBytes != 0) && !target.SupportsResourceLimits:
		return errors.New("resource limits are unsupported")
	case len(t.NodeSelector) != 0 && !target.SupportsNodeSelector:
		return errors.New("node selectors are unsupported")
	default:
		return target.Backend.Check(cloneTask(t))
	}
}

func quoteDiagnostic(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if s == "" || !utf8.ValidString(s) {
		return "target check rejected task"
	}
	if len(s) <= 256 {
		return s
	}
	n := 256
	for !utf8.ValidString(s[:n]) {
		n--
	}
	return s[:n]
}

func quotePriceState(p *backend.Quote, currency string, unit backend.RateUnit, now time.Time) PriceStatus {
	switch {
	case p == nil:
		return PriceMissing
	case now.Before(p.ObservedAt):
		return PriceFuture
	case !now.Before(p.ValidUntil):
		return PriceStale
	case p.Rate.Currency != currency:
		return PriceCurrencyMismatch
	case p.Unit != unit:
		return PriceUnitMismatch
	default:
		return PriceComparable
	}
}

func quoteCapacityFresh(c *backend.CapacitySnapshot, now time.Time) FactFreshness {
	switch {
	case c == nil:
		return FreshnessUnknown
	case now.Before(c.ObservedAt):
		return FreshnessFuture
	case !now.Before(c.ValidUntil):
		return FreshnessStale
	default:
		return FreshnessFresh
	}
}

func quoteComputeEstimate(rate, seconds int64) (int64, error) {
	if rate < 0 || rate > maxQuoteRate || seconds <= 0 || seconds > maxQuoteDuration || rate > math.MaxInt64/seconds {
		return 0, errors.New("invalid compute estimate input")
	}
	product := rate * seconds
	amount := product / 3600
	if product%3600 != 0 {
		amount++
	}
	return amount, nil
}

// quoteAt consumes validated configured facts and a caller-owned clock value.
// Control.QuoteTask supplies fresh auth and time.Now().UTC().
// It has no store, filesystem, provider, submission or scheduling side effect.
func quoteAt(task backend.Task, currency string, unit backend.RateUnit, seconds *int64, targets []backend.Target, now time.Time, requireOutputStorage bool) (QuotePlan, error) {
	return quoteWorkspaceAt(task, currency, unit, seconds, targets, now, requireOutputStorage, nil)
}

func quoteWorkspaceAt(task backend.Task, currency string, unit backend.RateUnit, seconds *int64, targets []backend.Target, now time.Time, requireOutputStorage bool, workspace *workspaceTarget) (QuotePlan, error) {
	var result QuotePlan
	if !backend.ValidQuoteCurrency(currency) || !backend.ValidRateUnit(unit) || seconds != nil && (*seconds <= 0 || *seconds > maxQuoteDuration) || !validQuoteNow(now) {
		return result, fmt.Errorf("%w: invalid quote request", ErrInvalid)
	}
	if len(targets) == 0 || len(targets) > 32 {
		return result, fmt.Errorf("%w: invalid target configuration", ErrInvalid)
	}
	var err error
	task, err = normalizeTask(task)
	if err != nil {
		return result, err
	}
	if err := Validate(task); err != nil {
		return result, err
	}
	targets = slices.Clone(targets)
	slices.SortFunc(targets, func(a, b backend.Target) int { return strings.Compare(a.Name, b.Name) })
	for i, target := range targets {
		name := target.Name
		if name == "" || len(name) > 256 || !utf8.ValidString(name) {
			return result, ErrQuoteMetadata
		}
		if target.Backend == nil || i > 0 && targets[i-1].Name == name {
			return result, fmt.Errorf("%w: invalid target configuration", ErrInvalid)
		}
		if err := backend.ValidateAdvice(target.Advice); err != nil {
			return result, fmt.Errorf("%w: invalid target facts", ErrInvalid)
		}
	}
	result = QuotePlan{AsOf: now, Currency: currency, Unit: unit, Outcome: QuoteNoEligibleTarget}
	if seconds != nil {
		n := *seconds
		result.BillableDurationSeconds = &n
	}
	eligible := 0
	var lowest int64
	for _, target := range targets {
		c := QuoteCandidate{Target: target.Name, EstimateStatus: EstimateQuoteNotComparable}
		advice := backend.CloneAdvice(target.Advice)
		checkErr := checkWorkspaceTarget(task, target, requireOutputStorage, workspace)
		c.Rejection = quoteDiagnostic(checkErr)
		c.Eligible = checkErr == nil
		var price *backend.Quote
		var capacity *backend.CapacitySnapshot
		if advice != nil {
			price, capacity = advice.Quote, advice.Capacity
		}
		c.Quote, c.Capacity = price, capacity
		c.PriceStatus = quotePriceState(price, currency, unit, now)
		c.CapacityFreshness = quoteCapacityFresh(capacity, now)
		c.CapacityWarning = capacity == nil || c.CapacityFreshness != FreshnessFresh || capacity.State != backend.CapacityAvailable
		if c.Eligible {
			eligible++
			if c.PriceStatus == PriceComparable {
				result.ComparisonCount++
				amount := *price.Rate.AmountMicros
				if result.SelectedTarget == "" || amount < lowest {
					result.SelectedTarget, lowest = c.Target, amount
				}
				switch {
				case seconds == nil:
					c.EstimateStatus = EstimateDurationUnspecified
				case unit != backend.TargetHour:
					c.EstimateStatus = EstimateQuantityUnspecified
				default:
					n, err := quoteComputeEstimate(amount, *seconds)
					if err != nil {
						return QuotePlan{}, err
					}
					c.EstimatedComputeCost = &backend.Money{AmountMicros: &n, Currency: currency}
					c.EstimateStatus = EstimateAvailable
				}
			} else {
				result.UnpricedEligibleCount++
			}
		}
		result.Candidates = append(result.Candidates, c)
	}
	if result.SelectedTarget != "" {
		result.Outcome = QuoteSelected
	} else if eligible > 0 {
		result.Outcome = QuoteNoComparableQuote
	}
	return result, nil
}

func validQuoteNow(t time.Time) bool {
	_, offset := t.Zone()
	return !t.IsZero() && offset == 0 && t.Year() >= 1 && t.Year() <= 9999
}

// QuoteTask recommends the lowest comparable configured rate. It never submits
// work, reserves capacity, or changes the task target.
func (c *Control) QuoteTask(actor auth.Actor, task backend.Task, currency string, unit backend.RateUnit, billableSeconds *int64) (QuotePlan, error) {
	if _, err := c.engine.Check(actor, auth.Read); err != nil {
		return QuotePlan{}, err
	}
	return quoteWorkspaceAt(task, currency, unit, billableSeconds, c.targets, time.Now().UTC(), c.retention.enabled, c.workspace)
}
