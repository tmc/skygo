package cli

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tmc/skygo/internal/store"
)

func (o *options) stateCommand() *cobra.Command {
	state := &cobra.Command{Use: "state", Short: "Manage stopped server state"}
	for _, action := range []string{"backup", "restore"} {
		var dir, file string
		name, short, flag := action, "Back up stopped server state", "output"
		if action == "restore" {
			short, flag = "Restore state into a new directory", "input"
		}
		cmd := &cobra.Command{Use: name, Short: short, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			if name == "backup" {
				err = store.Backup(dir, file)
			} else {
				err = store.Restore(file, dir)
			}
			if err != nil {
				return err
			}
			if name == "restore" {
				info, err := store.InspectRetention(dir)
				if err != nil {
					return fmt.Errorf("state restored; inspect retention readback: %w", err)
				}
				if info.Enabled {
					return o.writeOfflineRetention(cmd, dir, info, true)
				}
			}
			path := file
			if name == "restore" {
				path = dir
			}
			if o.json {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
					Path string `json:"path"`
				}{path})
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), path)
			return err
		}}
		cmd.Flags().StringVar(&dir, "data-dir", "skygo-data", "Durable state directory (server must be stopped)")
		cmd.Flags().StringVar(&file, flag, "", "Snapshot database file")
		_ = cmd.MarkFlagRequired(flag)
		state.AddCommand(cmd)
	}
	state.AddCommand(o.retentionStateCommand(), o.compactStateCommand())
	return state
}
