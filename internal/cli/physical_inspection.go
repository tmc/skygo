package cli

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tmc/skygo/internal/store"
)

func physicalInspectionCommand(o *options) *cobra.Command {
	var dir string
	cmd := &cobra.Command{Use: "inspect", Short: "Inspect stopped state without migration or writes", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		info, inspectErr := store.InspectPhysicalState(cmd.Context(), dir)
		if o.json {
			if err := json.NewEncoder(cmd.OutOrStdout()).Encode(info); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "geometry: %s\nvalidation: schema and binding geometry only; not a migration safety certificate\n", info.GeometryStatus); err != nil {
				return err
			}
			for _, v := range []struct {
				name  string
				value *uint64
			}{{"database bytes", info.FileBytes}, {"allocated bytes", info.AllocatedBytes}, {"free bytes", info.FilesystemFreeBytes}, {"jobs", info.Jobs}, {"raw request key bytes", info.RawRequestKeyBytes}, {"added legacy identity body lower bound", info.AddedLegacyIDPayloadLowerBound}, {"migrated live payload lower bound", info.MigratedLivePayloadLowerBound}} {
				if v.value != nil {
					if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s: %d\n", v.name, *v.value); err != nil {
						return err
					}
				}
			}
			if info.Reason != "" {
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "inspection reason: %s\n", info.Reason); err != nil {
					return err
				}
			}
		}
		return inspectErr
	}}
	cmd.Flags().StringVar(&dir, "data-dir", "skygo-data", "Existing state directory (server must be stopped)")
	return cmd
}

// Advisory only: unknown, locked, canceled, missing or deliberately unscanned
// geometry is never an invalid-state warning or a reason to alter startup.
func warnPhysicalState(cmd *cobra.Command, dir string) {
	info, err := store.ProbePhysicalState(cmd.Context(), dir)
	if err != nil {
		return
	}
	if warning := physicalWarning(info); warning != "" {
		fmt.Fprintln(cmd.ErrOrStderr(), warning)
	}
}

func physicalWarning(info store.PhysicalInspection) string {
	if !info.ProbeComplete || info.FileBytes == nil || info.FilesystemFreeBytes == nil {
		return ""
	}
	if *info.FileBytes < 256<<20 && *info.FilesystemFreeBytes >= 2<<30 {
		return ""
	}
	return "observed database size/free space may constrain retention; inspection is advisory; use state retention inspect before planned migration"
}
