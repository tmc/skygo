package sshagent_test

import (
	"context"
	"fmt"

	"github.com/tmc/skygo/integrations/ssh/sshagent"
)

// Open checks the whole Config before it reads, builds or connects to
// anything. This Config pins an SSH agent key but leaves out the
// fingerprint, so Open refuses it at once; no host is needed.
func ExampleOpen() {
	cfg := sshagent.Config{
		Address:        "dgx.example:22",
		User:           "skygo",
		KnownHostsFile: "/home/u/.config/skygo-ssh/dgx.known_hosts",
		AgentSocket:    "/home/u/.ssh/agent.sock",
		Source:         "/home/u/src/skygo",
		IdentityDir:    "/home/u/.config/skygo-ssh/agents/dgx",
		CacheDir:       "/home/u/.cache/skygo-ssh/build",
		Target:         "dgx",
	}
	s, err := sshagent.Open(context.Background(), cfg)
	if err != nil {
		// An *OpenError holds connections whose state is unknown; a
		// caller that gets one keeps it and opens no further session to
		// the host.
		fmt.Println(err)
		return
	}
	fmt.Println(s.Binding().AgentDir)
	if err := s.Close(); err != nil {
		// The SSH agent connection's state is unknown; s still holds it.
		fmt.Println(err)
	}
	// Output:
	// sshagent: an agent socket needs a SHA256 key fingerprint
}

// The zero Session owns nothing, so closing it does nothing.
func ExampleSession_Close() {
	var s sshagent.Session
	fmt.Println(s.Close())
	// Output:
	// <nil>
}
