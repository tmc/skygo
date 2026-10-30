package main

import (
	"github.com/spf13/cobra"
	"github.com/tmc/skygo/integrations/ssh/internal/agentssh"
	"github.com/tmc/skygo/integrations/ssh/sshagent"
	"github.com/tmc/skygo/internal/cli"
	"net/http"
)

// These existing protocol/cleanup fixtures retain their prior assertions;
// production runClient now uses the tracked sshagent.RunCommand path.
// agentCommand returns the skygo command for args, reaching the agent on
// host through hc with the credential in tokenFile.
func agentCommand(hc *http.Client, host, target, tokenFile string, args []string) (*cobra.Command, error) {
	root := cli.NewWithClient(nil, hc, cli.ClientOptions{Infra: "ssh/" + host, Target: target})
	root.SetArgs(args)
	if err := setEndpoint(root, agentssh.Target{AgentName: agentName}.Endpoint(), tokenFile); err != nil {
		return nil, err
	}
	return root, nil
}

// checkArgs refuses skygo arguments that would bypass the agent, and
// reports whether the command needs it. Help needs no agent.
func checkArgs(_ *cobra.Command, args []string) (bool, error) { return sshagent.CheckCommand(args) }

// setEndpoint points root at the agent with its private token file.
func setEndpoint(root *cobra.Command, endpoint, tokenFile string) error {
	if err := root.PersistentFlags().Set("endpoint", endpoint); err != nil {
		return err
	}
	return root.PersistentFlags().Set("token-file", tokenFile)
}
