package cli

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/spf13/cobra"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func validPageInput(size int32, token string) bool {
	return size >= 0 && size <= 100 && len(token) <= 683 && utf8.ValidString(token)
}

func (o *options) retentionInfoCommand() *cobra.Command {
	var size int32
	var token string
	cmd := &cobra.Command{Use: "retention", Short: "Show retention domains, limits and one admin obligation page", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if !validPageInput(size, token) {
			return errors.New("invalid retention page size or token")
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		response, err := o.client().GetRetention(ctx, request(o, &v1.GetRetentionRequest{PageSize: size, PageToken: token}))
		if err != nil {
			return retentionDiagnostic(err)
		}
		if response == nil || response.Msg == nil {
			return errors.New("server returned no retention information")
		}
		if o.json {
			return writeJSON(cmd.OutOrStdout(), response.Msg)
		}
		return writeRetentionInfo(cmd, response.Msg)
	}}
	cmd.Flags().Int32Var(&size, "page-size", 0, "Admin obligations per page (0 selects 50; at most 100)")
	cmd.Flags().StringVar(&token, "page-token", "", "Continuation token from the previous admin page")
	return cmd
}

func writeRetentionInfo(cmd *cobra.Command, info *v1.GetRetentionResponse) error {
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "enabled: %t\n", info.Enabled); err != nil {
		return err
	}
	if info.Enabled {
		domain, err := domainText(info.Domain)
		if err != nil {
			return err
		}
		for _, value := range []*timestamppb.Timestamp{info.AcceptUntil, info.RetryUntil, info.EpochZeroCutoff} {
			if value == nil || value.CheckValid() != nil {
				return errors.New("server returned invalid retention deadlines")
			}
		}
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "request-domain: %s\naccept-until: %s\nretry-until: %s\nepoch-zero-cutoff: %s\nlegacy-replay-open: %t\n", domain, info.AcceptUntil.AsTime().Format(time.RFC3339Nano), info.RetryUntil.AsTime().Format(time.RFC3339Nano), info.EpochZeroCutoff.AsTime().Format(time.RFC3339Nano), info.LegacyReplayOpen); err != nil {
			return err
		}
	}
	for _, target := range info.Targets {
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "target: %s storage=%s output-cleanup=%t execution-cleanup=%t output-reservation-bytes=%d\n", target.Target, target.OutputStorage, target.OutputCleanup, target.ExecutionCleanup, target.OutputReservationBytes); err != nil {
			return err
		}
	}
	if info.Admin == nil {
		return nil
	}
	for _, usage := range info.Admin.Usage {
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "usage: %s used=%d limit=%d\n", usage.Domain, usage.Used, usage.Limit); err != nil {
			return err
		}
	}
	for _, obligation := range info.Admin.Obligations {
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "obligation: %s job=%s needs-manual=%t output-pending=%t execution-pending=%t reason=%s\n", obligation.Id, obligation.JobId, obligation.NeedsManual, obligation.OutputPending, obligation.ExecutionPending, obligation.Reason); err != nil {
			return err
		}
	}
	if info.Admin.NextPageToken != "" {
		if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "next-page-token: %s\n", info.Admin.NextPageToken); err != nil {
			return err
		}
	}
	observed := "unavailable"
	if info.Admin.ObservedAt != nil {
		if info.Admin.ObservedAt.CheckValid() != nil {
			return errors.New("server returned invalid observation time")
		}
		observed = info.Admin.ObservedAt.AsTime().Format(time.RFC3339Nano)
	} else if info.Admin.DatabaseFileBytes != nil || info.Admin.DatabaseAllocatedBytes != nil || info.Admin.FilesystemFreeBytes != nil || info.Admin.OutputAllocatedBytes != nil || info.Admin.OutputFileCount != nil {
		return errors.New("server returned physical samples without observation time")
	}
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "observed-at: %s\nreusable-database-bytes: %d\npending-cleanup-records: %d\n", observed, info.Admin.ReusableDatabaseBytes, info.Admin.PendingCleanupRecords); err != nil {
		return err
	}
	for _, sample := range []struct {
		name  string
		value *uint64
	}{
		{"database-file-bytes", info.Admin.DatabaseFileBytes},
		{"database-allocated-bytes", info.Admin.DatabaseAllocatedBytes},
		{"filesystem-free-bytes", info.Admin.FilesystemFreeBytes},
		{"output-allocated-bytes", info.Admin.OutputAllocatedBytes},
		{"output-file-count", info.Admin.OutputFileCount},
	} {
		if sample.value == nil {
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s: unavailable\n", sample.name); err != nil {
				return err
			}
		} else if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s: %d\n", sample.name, *sample.value); err != nil {
			return err
		}
	}
	for _, condition := range info.Admin.Conditions {
		if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "retention-condition: %s\n", condition); err != nil {
			return err
		}
	}
	return nil
}

func (o *options) pruneCommand() *cobra.Command {
	var before, token, obligation string
	var metadataOnly, logsOnly, acknowledge, yes bool
	var size int32
	cmd := &cobra.Command{Use: "prune --before RFC3339", Short: "Inspect one eligible retention page; --yes marks it for cleanup", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if !validPageInput(size, token) {
			return errors.New("invalid prune page size or token")
		}
		if metadataOnly && logsOnly {
			return errors.New("choose only one of --metadata-only and --logs-only")
		}
		if acknowledge != metadataOnly {
			return errors.New("--metadata-only requires --ack-unverified-terminal, and the acknowledgement is valid only with --metadata-only")
		}
		mode := v1.PruneMode_PRUNE_MODE_WHOLE
		if metadataOnly {
			mode = v1.PruneMode_PRUNE_MODE_METADATA_ONLY
		}
		if logsOnly {
			mode = v1.PruneMode_PRUNE_MODE_OUTPUT_ONLY
		}
		selection := &v1.PruneJobsRequest{Mode: mode, Execute: yes, AcknowledgeUnverifiedTerminal: acknowledge, PageSize: size, PageToken: token, ObligationId: obligation}
		if obligation != "" {
			if !validHexID(obligation) || !logsOnly || cmd.Flags().Changed("before") || cmd.Flags().Changed("page-token") || cmd.Flags().Changed("page-size") {
				return errors.New("--obligation requires --logs-only and excludes --before, --page-token and --page-size")
			}
		} else {
			parsed, err := time.Parse(time.RFC3339Nano, before)
			if err != nil || parsed.IsZero() {
				return errors.New("prune requires --before RFC3339")
			}
			selection.Before = timestamppb.New(parsed.UTC())
			if selection.Before.CheckValid() != nil {
				return errors.New("invalid prune cutoff")
			}
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		response, err := o.client().PruneJobs(ctx, request(o, selection))
		if err != nil {
			return retentionDiagnostic(err)
		}
		if response == nil || response.Msg == nil {
			return errors.New("server returned no prune summary")
		}
		page := response.Msg
		if o.json {
			data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(page)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s\n", data)
			return err
		}
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "executed: %t\ninspected: %d\nselected: %d\nmarked: %d\nskipped: %d\n", page.Executed, page.Inspected, page.Selected, page.Marked, page.Skipped); err != nil {
			return err
		}
		for _, skip := range page.Skips {
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "skip: %s count=%d\n", skip.Reason, skip.Count); err != nil {
				return err
			}
		}
		for _, transfer := range page.Transfers {
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "charge: %s added=%d released=%d\n", transfer.Domain, transfer.Added, transfer.Released); err != nil {
				return err
			}
		}
		if page.NextPageToken != "" {
			_, err = fmt.Fprintf(cmd.ErrOrStderr(), "next-page-token: %s\n", page.NextPageToken)
		}
		return err
	}}
	cmd.Flags().StringVar(&before, "before", "", "Select results older than this RFC3339 timestamp")
	cmd.Flags().BoolVar(&metadataOnly, "metadata-only", false, "Expire terminal metadata without asserting execution cleanup")
	cmd.Flags().BoolVar(&logsOnly, "logs-only", false, "Expire eligible closed output without removing history or native execution")
	cmd.Flags().BoolVar(&acknowledge, "ack-unverified-terminal", false, "Acknowledge that metadata expiry does not prove execution stopped")
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "Execute this page instead of a read-only dry-run")
	cmd.Flags().StringVar(&obligation, "obligation", "", "One stored manual obligation ID (requires --logs-only)")
	cmd.Flags().Int32Var(&size, "page-size", 0, "Maximum selected subjects (0 selects 100; at most 100)")
	cmd.Flags().StringVar(&token, "page-token", "", "Continuation token from the previous prune page")
	return cmd
}
