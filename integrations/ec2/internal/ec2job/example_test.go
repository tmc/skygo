package ec2job_test

import (
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2job"
)

const exampleConfig = `{
	"version": 1,
	"vm": {
		"instance_id": "i-0123456789abcdef0",
		"image_id": "ami-0123456789abcdef0",
		"instance_type": "t4g.small",
		"architecture": "arm64",
		"address": "private",
		"ip": "10.0.0.5",
		"cpu_millis": 2000,
		"memory_bytes": 2147483648,
		"safety_deadline": "2099-01-01T00:00:00Z",
		"cleanup_reserve_seconds": 600
	},
	"ssh": {
		"port": 22,
		"user": "ec2-user",
		"known_hosts_file": "/home/op/ec2/known_hosts",
		"key_file": "/home/op/ec2/id_ed25519"
	},
	"agent": {
		"source": "/home/op/src/skygo",
		"identity_dir": "/home/op/ec2/identity",
		"cache_dir": "/home/op/ec2/cache",
		"target": "ec2vm"
	},
	"installation": {
		"data_dir": "/home/op/ec2/installation/data",
		"auth_file": "/home/op/ec2/installation/auth.json",
		"artifact_dir": "/home/op/ec2/installation/artifacts",
		"scoped_token_file": "/home/op/ec2/bob.token"
	},
	"task": {"shell": "/bin/sh", "infra": "aws/us-west-2", "max_timeout_seconds": 3600}
}`

func ExampleParseConfig() {
	c, err := ec2job.ParseConfig(strings.NewReader(exampleConfig))
	if err != nil {
		fmt.Println(err)
		return
	}
	s := c.Session(c.IP, nil)
	fmt.Println(c.InstanceID, c.Public, c.IP)
	fmt.Println(s.Address, s.User, s.Target)
	// Output:
	// i-0123456789abcdef0 false 10.0.0.5
	// 10.0.0.5:22 ec2-user ec2vm
}

func ExampleConfig_Task() {
	c, err := ec2job.ParseConfig(strings.NewReader(exampleConfig))
	if err != nil {
		fmt.Println(err)
		return
	}
	digest, size := strings.Repeat("a", 64), int64(10240)
	now := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)

	t, err := c.Task(ec2job.TaskInput{JSON: []byte(`{"argv":["/bin/sh","-c","echo hi"],"timeoutSeconds":"60"}`)}, digest, size, now)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(t.Target, t.TimeoutSeconds, t.Workdir == nil)

	_, err = c.Task(ec2job.TaskInput{JSON: []byte(`{"argv":["nvidia-smi"],"accelerator":"H100","acceleratorCount":1,"timeoutSeconds":"60"}`)}, digest, size, now)
	fmt.Println(err)
	// Output:
	// ec2vm 60 true
	// task: accelerators are not supported on this VM
}

func ExampleRemoteError() {
	err := error(&ec2job.RemoteError{Method: ec2job.Method, Code: connect.CodeFailedPrecondition, Reason: "request epoch expired"})
	fmt.Println(err)
	// Output:
	// ec2job: SubmitWorkspaceJob: failed_precondition: request epoch expired
}
