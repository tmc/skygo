package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"

	"github.com/spf13/cobra"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/backend"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type quoteFlags struct {
	objective, currency, unit string
	duration                  time.Duration
}

func (q *quoteFlags) flags(c *cobra.Command) {
	c.Flags().StringVar(&q.objective, "objective", "first-fit", "Planning objective: first-fit or lowest-rate (advisory only)")
	c.Flags().StringVar(&q.currency, "currency", "", "Currency for lowest-rate comparison (for example USD)")
	c.Flags().StringVar(&q.unit, "unit", "target-hour", "Rate denominator: target-hour, cpu-hour or accelerator-hour")
	c.Flags().DurationVar(&q.duration, "billable-duration", 0, "User-declared total billable duration; whole seconds, at most 168h")
}

func (q *quoteFlags) validate(c *cobra.Command) error {
	switch q.objective {
	case "first-fit":
		for _, name := range []string{"currency", "unit", "billable-duration"} {
			if c.Flags().Changed(name) {
				return fmt.Errorf("--%s requires --objective lowest-rate", name)
			}
		}
		return nil
	case "lowest-rate":
		if !backend.ValidQuoteCurrency(q.currency) {
			return errors.New("lowest-rate requires a three-letter uppercase currency")
		}
		if quoteUnit(q.unit) == v1.RateUnit_RATE_UNIT_UNSPECIFIED {
			return errors.New("invalid quote rate unit")
		}
		if c.Flags().Changed("billable-duration") && (q.duration <= 0 || q.duration > 168*time.Hour || q.duration%time.Second != 0) {
			return errors.New("billable duration must be whole seconds in 1s..168h")
		}
		return nil
	default:
		return errors.New("objective must be first-fit or lowest-rate")
	}
}

func quoteUnit(s string) v1.RateUnit {
	switch s {
	case "target-hour":
		return v1.RateUnit_RATE_UNIT_TARGET_HOUR
	case "cpu-hour":
		return v1.RateUnit_RATE_UNIT_CPU_HOUR
	case "accelerator-hour":
		return v1.RateUnit_RATE_UNIT_ACCELERATOR_HOUR
	}
	return v1.RateUnit_RATE_UNIT_UNSPECIFIED
}

func (o *options) quoteTask(ctx context.Context, cmd *cobra.Command, task *v1.Task, q quoteFlags) error {
	in := &v1.QuoteTaskRequest{Task: task, Currency: q.currency, Unit: quoteUnit(q.unit)}
	if cmd.Flags().Changed("billable-duration") {
		n := int64(q.duration / time.Second)
		in.BillableDurationSeconds = &n
	}
	result, err := o.client().QuoteTask(ctx, request(o, in))
	if err != nil {
		return err
	}
	if o.json {
		err = writeQuoteJSON(cmd.OutOrStdout(), result.Msg)
	} else {
		err = writeQuotePlan(cmd.OutOrStdout(), result.Msg)
	}
	if err != nil {
		return err
	}
	switch result.Msg.Outcome {
	case v1.QuoteOutcome_QUOTE_OUTCOME_SELECTED:
		return nil
	case v1.QuoteOutcome_QUOTE_OUTCOME_NO_ELIGIBLE_TARGET:
		return errors.New("no eligible target")
	case v1.QuoteOutcome_QUOTE_OUTCOME_NO_COMPARABLE_QUOTE:
		return errors.New("no comparable configured quote")
	default:
		return errors.New("server returned an invalid quote outcome")
	}
}

func writeQuoteJSON(w io.Writer, m *v1.QuoteTaskResponse) error {
	b, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(m)
	if err != nil {
		return err
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, b); err != nil {
		return err
	}
	if proto.Size(m) > 64<<10 || compact.Len() > 64<<10 {
		return errors.New("quote response exceeds 64 KiB")
	}
	_, err = fmt.Fprintf(w, "%s\n", compact.Bytes())
	return err
}

func quoteText(s string) string {
	if strings.IndexFunc(s, unicode.IsControl) >= 0 {
		return strconv.QuoteToGraphic(s)
	}
	return s
}

func moneyText(m *v1.Money) string {
	if m == nil || m.AmountMicros == nil {
		return "unknown"
	}
	return fmt.Sprintf("%d.%06d %s", *m.AmountMicros/1_000_000, *m.AmountMicros%1_000_000, m.Currency)
}

func rateText(q *v1.Quote) string {
	if q == nil {
		return "unknown"
	}
	unit := strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(q.Unit.String(), "RATE_UNIT_")), "_", "-")
	return moneyText(q.Rate) + "/" + unit
}

func writeQuotePlan(w io.Writer, p *v1.QuoteTaskResponse) error {
	var output bytes.Buffer
	if p.Outcome == v1.QuoteOutcome_QUOTE_OUTCOME_SELECTED {
		fmt.Fprintf(&output, "advisory target: %s (lowest comparable configured rate)\n", quoteText(p.SelectedTarget))
		for _, c := range p.Candidates {
			if c.Target == p.SelectedTarget {
				fmt.Fprintf(&output, "rate: %s\n", rateText(c.Quote))
				if c.EstimateStatus == v1.EstimateStatus_ESTIMATE_STATUS_AVAILABLE {
					fmt.Fprintf(&output, "estimated compute cost: %s\n", moneyText(c.EstimatedComputeCost))
				} else {
					fmt.Fprintf(&output, "estimated compute cost: unknown (%s)\n", strings.ToLower(strings.TrimPrefix(c.EstimateStatus.String(), "ESTIMATE_STATUS_")))
				}
				if c.CapacityWarning {
					fmt.Fprintln(&output, "capacity warning: configured capacity is unknown, stale or unavailable")
				}
			}
		}
	}
	table := tabwriter.NewWriter(&output, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "TARGET\tELIGIBLE\tPRICE\tRATE\tCAPACITY\tREJECTION")
	for _, c := range p.Candidates {
		fmt.Fprintf(table, "%s\t%t\t%s\t%s\t%s\t%s\n", quoteText(c.Target), c.Eligible, strings.ToLower(strings.TrimPrefix(c.PriceStatus.String(), "PRICE_STATUS_")), rateText(c.Quote), strings.ToLower(strings.TrimPrefix(c.CapacityFreshness.String(), "FACT_FRESHNESS_")), quoteText(c.Rejection))
	}
	if err := table.Flush(); err != nil {
		return err
	}
	fmt.Fprintln(&output, "excluded charges: storage, transfer, tax, licenses, provider adjustments")
	fmt.Fprintln(&output, "advisory only; run uses task.target or ordinary placement")
	_, err := w.Write(output.Bytes())
	return err
}
