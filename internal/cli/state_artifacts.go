package cli

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
)

func (o *options) artifactsStateCommand() *cobra.Command {
	root := &cobra.Command{Use: "artifacts", Short: "Manage artifacts on stopped state"}
	var directory, artifacts string
	var stopped bool
	initialize := &cobra.Command{Use: "init", Short: "Initialize an empty private artifact directory", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if !stopped {
			return errors.New("artifact initialization requires --ack-writers-stopped")
		}
		if err := server.InitializeArtifacts(directory, artifacts); err != nil {
			return err
		}
		if o.json {
			err := json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
				Enabled         bool   `json:"enabled"`
				Policy          string `json:"policy"`
				RestartRequired bool   `json:"restart_required"`
			}{true, "default-v1", true})
			if err != nil {
				return fmt.Errorf("artifacts initialized; write readback: %w", err)
			}
			return nil
		}
		_, err := fmt.Fprintln(cmd.OutOrStdout(), "enabled: true\npolicy: default-v1\nrestart_required: true")
		if err != nil {
			return fmt.Errorf("artifacts initialized; write readback: %w", err)
		}
		return nil
	}}
	initialize.Flags().StringVar(&directory, "data-dir", "skygo-data", "Durable state directory (server must be stopped)")
	initialize.Flags().StringVar(&artifacts, "artifact-dir", "", "Existing empty private directory owned by the operator")
	initialize.Flags().BoolVar(&stopped, "ack-writers-stopped", false, "Acknowledge that the server and all artifact writers are stopped")
	_ = initialize.MarkFlagRequired("artifact-dir")
	var backupData, backupRoot, output string
	var backupStopped bool
	backup := &cobra.Command{Use: "backup", Short: "Back up stopped state and its artifact files", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if !backupStopped {
			return errors.New("companion backup requires --ack-writers-stopped")
		}
		if backupData == "" || backupRoot == "" || output == "" {
			return errors.New("companion backup requires nonempty --data-dir, --artifact-dir and --output")
		}
		if err := cmd.Context().Err(); err != nil {
			return err
		}
		if err := store.BackupCompanion(cmd.Context(), backupData, backupRoot, output); err != nil {
			return err
		}
		return o.writeCompanionResult(cmd, "backup")
	}}
	backup.Flags().StringVar(&backupData, "data-dir", "", "Existing stopped state directory")
	backup.Flags().StringVar(&backupRoot, "artifact-dir", "", "Existing artifact directory matching this state")
	backup.Flags().StringVar(&output, "output", "", "New companion backup directory")
	backup.Flags().BoolVar(&backupStopped, "ack-writers-stopped", false, "Acknowledge that the original server, backends and artifact writers are stopped")
	_ = backup.MarkFlagRequired("data-dir")
	_ = backup.MarkFlagRequired("artifact-dir")
	_ = backup.MarkFlagRequired("output")

	var input, restoreData string
	var restoreStopped bool
	restore := &cobra.Command{Use: "restore", Short: "Restore a companion backup into new stopped state", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if !restoreStopped {
			return errors.New("companion restore requires --ack-writers-stopped")
		}
		if input == "" || restoreData == "" {
			return errors.New("companion restore requires nonempty --input and --data-dir")
		}
		if err := cmd.Context().Err(); err != nil {
			return err
		}
		if err := store.RestoreCompanion(cmd.Context(), input, restoreData); err != nil {
			return err
		}
		return o.writeCompanionResult(cmd, "restore")
	}}
	restore.Flags().StringVar(&input, "input", "", "Existing companion backup directory")
	restore.Flags().StringVar(&restoreData, "data-dir", "", "New state directory; artifacts are restored under artifacts/")
	restore.Flags().BoolVar(&restoreStopped, "ack-writers-stopped", false, "Acknowledge that the original server, backends and artifact writers are stopped")
	_ = restore.MarkFlagRequired("input")
	_ = restore.MarkFlagRequired("data-dir")
	root.AddCommand(initialize, backup, restore)
	return root
}

// Readback reports completion without exposing source paths or object identities.
// A write failure does not undo the completed operation or authorize a retry.
func (o *options) writeCompanionResult(cmd *cobra.Command, action string) error {
	var err error
	if o.json {
		err = json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
			Complete bool `json:"complete"`
		}{true})
	} else {
		_, err = fmt.Fprintln(cmd.OutOrStdout(), action+" complete")
	}
	if err != nil {
		return fmt.Errorf("companion %s completed; write readback: %w", action, err)
	}
	return nil
}
