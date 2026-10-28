package sshconfig_test

import (
	"context"
	"fmt"
	"strings"

	"github.com/tmc/skygo/integrations/ssh/internal/sshconfig"
)

// sshG is a literal excerpt of `ssh -G gpu1` output.
const sshG = `host gpu1
user ubuntu
hostname 10.0.0.7
port 2222
identityfile ~/.ssh/id_ed25519
identityagent SSH_AUTH_SOCK
forwardagent no
proxycommand none
`

func Example() {
	h, err := sshconfig.Parse("gpu1", "/home/alice", []byte(sshG))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(h.Address, h.User, h.IdentityFiles, h.IdentityAgent == "")

	// A jump host would be silently bypassed by a direct connection.
	_, err = sshconfig.Parse("gpu1", "/home/alice", []byte(sshG+"proxyjump bastion\n"))
	fmt.Println(err)
	// Output:
	// 10.0.0.7:2222 ubuntu [/home/alice/.ssh/id_ed25519] true
	// sshconfig: gpu1: proxyjump is not supported
}

func ExampleResolve() {
	// MODELED: this Runner returns fixed output instead of running ssh.
	run := func(ctx context.Context, name string, args ...string) ([]byte, error) {
		fmt.Println("run:", name, strings.Join(args, " "))
		return []byte(sshG), nil
	}
	h, err := sshconfig.Resolve(context.Background(), "gpu1", "/home/alice", run)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(h.Alias, h.Address)

	// An alias that ssh would read as an option is refused before running.
	_, err = sshconfig.Resolve(context.Background(), "-oProxyCommand=sh", "/home/alice", run)
	fmt.Println(err)
	// Output:
	// run: ssh -G -- gpu1
	// gpu1 10.0.0.7:2222
	// sshconfig: invalid host alias "-oProxyCommand=sh"
}
