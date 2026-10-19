package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type retentionLimit struct {
	name    string
	value   *uint64
	maximum uint64
}

func retentionLimits(policy *store.RetentionPolicy) []retentionLimit {
	maximum := store.DefaultRetentionPolicy()
	return []retentionLimit{
		{"jobs", &policy.Jobs, maximum.Jobs},
		{"task-bytes", &policy.TaskBytes, maximum.TaskBytes},
		{"retry-bytes", &policy.RetryBytes, maximum.RetryBytes},
		{"metadata-bytes", &policy.MetadataBytes, maximum.MetadataBytes},
		{"request-records", &policy.RequestRecords, maximum.RequestRecords},
		{"request-bytes", &policy.RequestBytes, maximum.RequestBytes},
		{"cleanup-records", &policy.CleanupRecords, maximum.CleanupRecords},
		{"cleanup-bytes", &policy.CleanupBytes, maximum.CleanupBytes},
		{"output-bytes", &policy.OutputBytes, maximum.OutputBytes},
		{"output-domains", &policy.OutputDomains, maximum.OutputDomains},
		{"output-files", &policy.OutputFiles, maximum.OutputFiles},
		{"principal-scopes", &policy.PrincipalScopes, maximum.PrincipalScopes},
	}
}

func (o *options) retentionStateCommand() *cobra.Command {
	root := &cobra.Command{Use: "retention", Short: "Manage bounded retention on stopped state"}
	var dir string
	var disableLocalOutput bool
	policy := store.DefaultRetentionPolicy()
	limits := retentionLimits(&policy)
	enable := &cobra.Command{Use: "enable", Short: "Enable bounded request epochs and retention limits", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if policy.Window < time.Hour || policy.Window > 90*24*time.Hour {
			return errors.New("request window must be between 1h and 2160h")
		}
		for _, limit := range limits {
			if *limit.value == 0 || *limit.value > limit.maximum {
				return fmt.Errorf("--max-%s must be between 1 and %d", limit.name, limit.maximum)
			}
		}
		policy.LocalOutputEnabled = !disableLocalOutput
		warnPhysicalState(cmd, dir)
		info, err := store.EnableRetention(dir, policy)
		if err != nil {
			return err
		}
		if err := o.writeOfflineRetention(cmd, dir, info, true); err != nil {
			return fmt.Errorf("retention enabled; write activation readback: %w", err)
		}
		return nil
	}}
	enable.Flags().StringVar(&dir, "data-dir", "skygo-data", "Durable state directory (server must be stopped)")
	enable.Flags().BoolVar(&disableLocalOutput, "disable-local-output", false, "Enable remote-only retention; requires an absent or empty owned log directory")
	enable.Flags().DurationVar(&policy.Window, "request-window", policy.Window, "Minimum request deduplication window (1h..2160h)")
	for _, limit := range limits {
		unit := "records"
		if strings.HasSuffix(limit.name, "bytes") {
			unit = "bytes"
		}
		enable.Flags().Uint64Var(limit.value, "max-"+limit.name, *limit.value, fmt.Sprintf("Retention limit in %s (at most %d)", unit, limit.maximum))
	}
	var revalidateDir string
	var writersStopped bool
	revalidate := &cobra.Command{Use: "revalidate", Short: "Reconcile local output accounting on stopped state", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if !writersStopped {
			return errors.New("revalidation requires --ack-local-writers-stopped")
		}
		info, err := store.RevalidateLocalOutput(revalidateDir)
		if err != nil {
			return err
		}
		if err := o.writeOfflineRetention(cmd, revalidateDir, info, true); err != nil {
			return fmt.Errorf("local output revalidated; write readback: %w", err)
		}
		return nil
	}}
	revalidate.Flags().StringVar(&revalidateDir, "data-dir", "skygo-data", "Durable state directory (server must be stopped)")
	revalidate.Flags().BoolVar(&writersStopped, "ack-local-writers-stopped", false, "Acknowledge that the server and all local output writers are stopped")
	root.AddCommand(enable, revalidate, physicalInspectionCommand(o))
	return root
}

func (o *options) compactStateCommand() *cobra.Command {
	var dir, output string
	cmd := &cobra.Command{Use: "compact --output FILE", Short: "Compact stopped state into a new file", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if err := store.Compact(dir, output); err != nil {
			return err
		}
		if o.json {
			return json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
				Path string `json:"path"`
			}{output})
		}
		_, err := fmt.Fprintln(cmd.OutOrStdout(), output)
		return err
	}}
	cmd.Flags().StringVar(&dir, "data-dir", "skygo-data", "Durable state directory (server must be stopped)")
	cmd.Flags().StringVar(&output, "output", "", "Exclusive destination database file; original is preserved")
	_ = cmd.MarkFlagRequired("output")
	return cmd
}

func (o *options) writeOfflineRetention(cmd *cobra.Command, path string, info store.RetentionInfo, restart bool) error {
	domain, err := domainText(&v1.RequestDomain{Lineage: info.Domain.Lineage, Epoch: info.Domain.Epoch})
	if err != nil {
		return err
	}
	for _, value := range []time.Time{info.AcceptUntil, info.RetryUntil, info.LegacyCutoff} {
		if value.IsZero() || timestamppb.New(value).CheckValid() != nil {
			return errors.New("state returned invalid retention deadlines")
		}
	}
	limits := make(map[string]uint64)
	for _, limit := range retentionLimits(&info.Policy) {
		limits[strings.ReplaceAll(limit.name, "-", "_")] = *limit.value
	}
	out := struct {
		Path               string            `json:"path"`
		Enabled            bool              `json:"enabled"`
		RequestDomain      string            `json:"request_domain"`
		AcceptUntil        time.Time         `json:"accept_until"`
		RetryUntil         time.Time         `json:"retry_until"`
		EpochZeroCutoff    time.Time         `json:"epoch_zero_cutoff"`
		LegacyReplayOpen   bool              `json:"legacy_replay_open"`
		RequestWindow      string            `json:"request_window"`
		Limits             map[string]uint64 `json:"limits"`
		RestartRequired    bool              `json:"restart_required"`
		LocalOutputEnabled bool              `json:"local_output_enabled"`
	}{Path: path, Enabled: info.Enabled, RequestDomain: domain, AcceptUntil: info.AcceptUntil.UTC(), RetryUntil: info.RetryUntil.UTC(), EpochZeroCutoff: info.LegacyCutoff.UTC(), LegacyReplayOpen: info.LegacyReplayOpen, RequestWindow: info.Policy.Window.String(), Limits: limits, RestartRequired: restart, LocalOutputEnabled: info.Policy.LocalOutputEnabled}
	if o.json {
		return json.NewEncoder(cmd.OutOrStdout()).Encode(out)
	}
	_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s\nrequest-domain: %s\naccept-until: %s\nretry-until: %s\nepoch-zero-cutoff: %s\nlegacy-replay-open: %t\nrequest-window: %s\nlocal-output-enabled: %t\n", path, domain, out.AcceptUntil.Format(time.RFC3339Nano), out.RetryUntil.Format(time.RFC3339Nano), out.EpochZeroCutoff.Format(time.RFC3339Nano), out.LegacyReplayOpen, out.RequestWindow, out.LocalOutputEnabled)
	if err != nil {
		return err
	}
	for _, limit := range retentionLimits(&info.Policy) {
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "max-%s: %d\n", limit.name, *limit.value); err != nil {
			return err
		}
	}
	if restart {
		_, err = fmt.Fprintln(cmd.OutOrStdout(), "restart-required: true")
	}
	return err
}
