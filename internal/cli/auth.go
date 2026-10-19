package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/store"
)

func localCommand(cmd *cobra.Command) bool {
	for p := cmd; p != nil; p = p.Parent() {
		if p.Name() == "auth" || p.Name() == "state" {
			return true
		}
	}
	return false
}

func (o *options) resolveCredentials(cmd *cobra.Command) error {
	literal := cmd.Flags().Changed("token")
	file := cmd.Flags().Changed("token-file")
	env := os.Getenv("SKYGO_TOKEN")
	sources := 0
	for _, supplied := range []bool{literal, file, env != ""} {
		if supplied {
			sources++
		}
	}
	if sources > 1 {
		return errors.New("choose only one credential source: --token-file, --token or SKYGO_TOKEN")
	}
	if cmd.Name() == "server" {
		path, _ := cmd.Flags().GetString("auth-file")
		if path != "" && sources != 0 {
			return errors.New("--auth-file conflicts with legacy token credentials")
		}
	}
	if file {
		token, err := readTokenFile(o.tokenFile)
		if err != nil {
			return err
		}
		o.token = token
	} else if !literal {
		o.token = env
	}
	if !validToken(o.token) {
		return errors.New("invalid bearer credential")
	}
	return nil
}

func validToken(token string) bool {
	if len(token) > 4096 {
		return false
	}
	for _, b := range []byte(token) {
		if b < 0x21 || b > 0x7e {
			return false
		}
	}
	return true
}

func readTokenFile(path string) (string, error) {
	bad := errors.New("token file must be a private regular file containing one bounded token")
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm()&0077 != 0 {
		return "", bad
	}
	f, err := os.Open(path)
	if err != nil {
		return "", bad
	}
	defer f.Close()
	after, err := f.Stat()
	if err != nil || !after.Mode().IsRegular() || after.Mode().Perm()&0077 != 0 || !os.SameFile(before, after) {
		return "", bad
	}
	data, err := io.ReadAll(io.LimitReader(f, 4098))
	if err != nil || len(data) > 4097 {
		return "", bad
	}
	token := strings.TrimSuffix(string(data), "\n")
	if token == "" || !validToken(token) {
		return "", bad
	}
	return token, nil
}

func (o *options) authCommand() *cobra.Command {
	var path string
	root := &cobra.Command{Use: "auth", Short: "Manage a private scoped authentication file"}
	root.PersistentFlags().StringVar(&path, "auth-file", "", "Private scoped authentication file")
	_ = root.MarkPersistentFlagRequired("auth-file")
	var dir, tokenOutput string
	initialize := &cobra.Command{Use: "init", Short: "Activate scoped authentication on stopped server state", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if err := initializeAuth(dir, path, tokenOutput); err != nil {
			return err
		}
		return o.authReceipt(cmd, "initialized")
	}}
	initialize.Flags().StringVar(&dir, "data-dir", "skygo-data", "Durable state directory (server must be stopped)")
	initialize.Flags().StringVar(&tokenOutput, "token-output", "", "New private file for the initial admin token")
	_ = initialize.MarkFlagRequired("token-output")
	principals := &cobra.Command{Use: "principal", Short: "Manage principal roles"}
	for _, operation := range []string{"add", "set-role"} {
		var role string
		cmd := &cobra.Command{Use: operation + " ID", Short: "Set a principal's authentication role", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if operation == "add" {
				err = auth.AddPrincipal(path, args[0], auth.Role(role))
			} else {
				err = auth.SetRole(path, args[0], auth.Role(role))
			}
			if err != nil {
				return fmt.Errorf("update principal: %w", err)
			}
			return o.authReceipt(cmd, "principal updated")
		}}
		cmd.Flags().StringVar(&role, "role", "", "reader, operator or admin")
		_ = cmd.MarkFlagRequired("role")
		principals.AddCommand(cmd)
	}
	var output string
	var lifetime time.Duration
	tokens := &cobra.Command{Use: "token", Short: "Issue, list and revoke credentials"}
	issue := &cobra.Command{Use: "issue PRINCIPAL", Short: "Write a new token once to a private file", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		info, err := auth.Issue(path, args[0], output, lifetime)
		if err != nil {
			return fmt.Errorf("issue credential: %w", err)
		}
		if o.json {
			return json.NewEncoder(cmd.OutOrStdout()).Encode(info)
		}
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\n", info.ID, info.Principal, info.ExpiresAt.Format(time.RFC3339))
		return err
	}}
	issue.Flags().StringVar(&output, "output", "", "New private token file")
	issue.Flags().DurationVar(&lifetime, "expires-in", 30*24*time.Hour, "Token lifetime (maximum 90 days)")
	_ = issue.MarkFlagRequired("output")
	revoke := &cobra.Command{Use: "revoke ID", Short: "Revoke a token by its public ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if err := auth.Revoke(path, args[0]); err != nil {
			return fmt.Errorf("revoke credential: %w", err)
		}
		return o.authReceipt(cmd, "credential revoked")
	}}
	list := &cobra.Command{Use: "list", Short: "List public credential metadata", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		infos, err := auth.Tokens(path)
		if err != nil {
			return fmt.Errorf("list credentials: %w", err)
		}
		if o.json {
			return json.NewEncoder(cmd.OutOrStdout()).Encode(infos)
		}
		w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tPRINCIPAL\tEXPIRES")
		for _, info := range infos {
			fmt.Fprintf(w, "%s\t%s\t%s\n", info.ID, info.Principal, info.ExpiresAt.Format(time.RFC3339))
		}
		return w.Flush()
	}}
	tokens.AddCommand(issue, revoke, list)
	root.AddCommand(initialize, principals, tokens)
	return root
}

func (o *options) authReceipt(cmd *cobra.Command, operation string) error {
	if o.json {
		return json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
			Operation string `json:"operation"`
		}{operation})
	}
	_, err := fmt.Fprintln(cmd.OutOrStdout(), operation)
	return err
}

func initializeAuth(dir, path, tokenOutput string) (result error) {
	s, err := store.Open(dir)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, s.Close()) }()
	if s.Scoped() {
		return errors.New("scoped authentication is already active")
	}
	cleanup, err := auth.Initialize(path, tokenOutput, s.Owner())
	if err != nil {
		return fmt.Errorf("prepare authentication: %w", err)
	}
	activated := false
	defer func() {
		if !activated {
			result = errors.Join(result, cleanup())
		}
	}()
	if _, err := auth.NewFile(path, s.Owner()); err != nil {
		return fmt.Errorf("validate authentication: %w", err)
	}
	if err := s.ActivateScoped(); err != nil {
		return fmt.Errorf("activate scoped authentication: %w", err)
	}
	activated = true
	return nil
}
