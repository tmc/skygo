package ec2ctl_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/tmc/skygo/integrations/ec2/internal/ec2ctl"
)

const config = `{
	"version": 1,
	"profile": {
		"account": "123456789012",
		"region": "us-west-2",
		"subnet_id": "subnet-0123456789abcdef0",
		"security_group_ids": ["sg-0aaaaaaaaaaaaaaaa"],
		"image_id": "ami-0123456789abcdef0",
		"instance_type": "t3.micro",
		"root_device": "/dev/xvda",
		"root_volume_gib": 16,
		"root_volume_type": "gp3",
		"encrypt_root": true
	},
	"identity": {"owner": "install-7f3a", "environment": "env-0123456789abcdef", "generation": 1},
	"user_data": ""
}`

func ExampleParseConfig() {
	cfg, err := ec2ctl.ParseConfig(strings.NewReader(config))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(cfg.Profile.Region, cfg.Identity.Generation)
	// Output: us-west-2 1
}

func ExampleLoadConfig() {
	tmp, err := os.MkdirTemp("", "ec2ctl-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	path := filepath.Join(tmp, "config.json")
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		log.Fatal(err)
	}
	cfg, err := ec2ctl.LoadConfig(path)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(cfg.Profile.Region, cfg.Identity.Generation)
	// Output: us-west-2 1
}

// Example creates the record of a new VM and takes it down before
// anything was sent, which needs no call to EC2.
func Example() {
	tmp, err := os.MkdirTemp("", "ec2ctl-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	cfg, err := ec2ctl.ParseConfig(strings.NewReader(config))
	if err != nil {
		log.Fatal(err)
	}
	m, err := ec2ctl.Create(filepath.Join(tmp, "vm"), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close()
	fmt.Println(m.Status().Next)

	if err := m.SetGoal(ec2ctl.GoalAbsent); err != nil {
		log.Fatal(err)
	}
	st, err := m.Drive(context.Background(), nil, ec2ctl.Limits{Steps: 1})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(st.Record.Desired, st.Reached())
	fmt.Println(m.SetGoal(ec2ctl.GoalRunning))
	// Output:
	// create
	// absent true
	// the VM wants absent; that is latched
}
