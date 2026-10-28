package bootstrap_test

import (
	"context"
	"fmt"
	"io"

	"github.com/tmc/skygo/integrations/ssh/internal/bootstrap"
)

// A modeledRunner is a MODELED Runner: it returns fixed status protocol
// output and runs nothing, locally or over SSH.
type modeledRunner string

func (out modeledRunner) Run(ctx context.Context, command string, stdin io.Reader) ([]byte, []byte, error) {
	return []byte(out), nil, nil
}

// This example models the state of a fresh agent directory. The Runner is
// modeled, so it shows Status parsing the protocol, not a remote host.
func Example() {
	a := bootstrap.Agent{
		Arch:   bootstrap.Arch,
		Commit: "0123456789abcdef0123456789abcdef01234567",
		Digest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		Port:   8443,
		Target: "local",
		// Status validates that each file is named; it does not read them.
		Local: map[string]string{
			"skygo-ssh": "/build/skygo-ssh-linux-arm64",
			"cert.pem":  "/state/cert.pem",
			"key.pem":   "/state/key.pem",
			"token":     "/state/token",
		},
	}
	r := modeledRunner("arch aarch64\n" +
		"file skygo-ssh missing\nfile cert.pem missing\nfile key.pem missing\nfile token missing\n" +
		"agent none\n")

	st, err := bootstrap.Status(context.Background(), r, a)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(a.Dir())
	fmt.Println(st.Arch == a.Arch, st.Agent, st.PID)
	for _, name := range bootstrap.Files {
		fmt.Println(name, st.Files[name])
	}
	// Output:
	// ~/skygo-test/0123456789abcdef0123456789abcdef01234567/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
	// true none 0
	// skygo-ssh missing
	// cert.pem missing
	// key.pem missing
	// token missing
}
