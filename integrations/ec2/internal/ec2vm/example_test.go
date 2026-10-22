package ec2vm_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2vm"
)

func profile() *ec2api.Profile {
	return &ec2api.Profile{
		Account:          "123456789012",
		Region:           "us-west-2",
		SubnetID:         "subnet-0123456789abcdef0",
		SecurityGroupIDs: []string{"sg-0aaaaaaaaaaaaaaaa"},
		ImageID:          "ami-0123456789abcdef0",
		InstanceType:     "t3.micro",
		RootDevice:       "/dev/xvda",
		RootVolumeGiB:    16,
		RootVolumeType:   "gp3",
		EncryptRoot:      true,
	}
}

var identity = ec2api.Identity{Owner: "install-7f3a", Environment: "env-0123456789abcdef", Generation: 1}

func ExampleNewRecord() {
	r, err := ec2vm.NewRecord(profile(), identity, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(r.Desired, r.Phase, r.Token == identity.ClientToken())
	// Output: present creating true
}

func ExampleNext() {
	r, err := ec2vm.NewRecord(profile(), identity, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(ec2vm.Next(r, ec2vm.Options{}))
	// A create may have been sent and no instance is known: with the
	// default reissue window, search by ClientToken.
	r.CreateSent, r.CreateSentAt = true, time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	fmt.Println(ec2vm.Next(r, ec2vm.Options{}))
	// Output:
	// create
	// recover
}

func ExampleRecord_WithDesired() {
	r, err := ec2vm.NewRecord(profile(), identity, nil)
	if err != nil {
		log.Fatal(err)
	}
	r, err = r.WithDesired(ec2vm.DesiredAbsent)
	if err != nil {
		log.Fatal(err)
	}
	_, err = r.WithDesired(ec2vm.DesiredPresent)
	fmt.Println(err)
	// Output: record already wants absent
}

func ExampleFromFields() {
	r, err := ec2vm.NewRecord(profile(), identity, nil)
	if err != nil {
		log.Fatal(err)
	}
	r.Identity.Generation = 2 // a field that disagrees with the stored request
	_, err = ec2vm.FromFields(profile(), r)
	fmt.Println(err)
	// Output: record token does not match its request and identity
}

// cloud answers like EC2 for one instance that launches and runs.
type cloud struct{ inst ec2api.Instance }

func (c *cloud) RunInstances(_ context.Context, req *ec2api.CreateRequest) (*ec2api.Reservation, error) {
	c.inst = ec2api.Instance{ID: "i-0123456789abcdef0", State: "pending", ClientToken: req.ClientToken()}
	return &ec2api.Reservation{ID: "r-0123456789abcdef0", OwnerID: "123456789012", Instances: []ec2api.Instance{c.inst}}, nil
}

func (c *cloud) FindOwned(context.Context, *ec2api.CreateRequest) (*ec2api.Instance, error) {
	return &c.inst, nil
}

func (c *cloud) DescribeOwned(context.Context, *ec2api.CreateRequest, string) (*ec2api.Instance, error) {
	c.inst.State = "running"
	return &c.inst, nil
}

func (c *cloud) CheckCPUCredits(context.Context, *ec2api.CreateRequest, string) (string, error) {
	return "standard", nil
}

func (c *cloud) Stop(_ context.Context, _ *ec2api.CreateRequest, id string) (*ec2api.StateChange, error) {
	return &ec2api.StateChange{InstanceID: id, Previous: "running", Current: "stopping"}, nil
}

func (c *cloud) Start(_ context.Context, _ *ec2api.CreateRequest, id string) (*ec2api.StateChange, error) {
	return &ec2api.StateChange{InstanceID: id, Previous: "stopped", Current: "pending"}, nil
}

func (c *cloud) Terminate(_ context.Context, _ *ec2api.CreateRequest, id string) (*ec2api.StateChange, error) {
	return &ec2api.StateChange{InstanceID: id, Previous: "running", Current: "shutting-down"}, nil
}

func ExampleStep() {
	var stored ec2vm.Record // stands in for a durable store
	opt := ec2vm.Options{Commit: func(_ context.Context, r ec2vm.Record) error {
		stored = r
		return nil
	}}
	r, err := ec2vm.NewRecord(profile(), identity, nil)
	if err != nil {
		log.Fatal(err)
	}
	c := new(cloud) // an *ec2api.Client in production
	for a := ec2vm.Next(r, opt); a != ec2vm.ActionNone; a = ec2vm.Next(r, opt) {
		r, err = ec2vm.Step(context.Background(), c, r, opt)
		if err != nil {
			log.Fatal(err)
		}
		stored = r
		fmt.Println(a, "->", r.Phase)
	}
	fmt.Println(stored.InstanceID)
	// Output:
	// create -> creating
	// verify credits -> creating
	// observe -> running
	// i-0123456789abcdef0
}

func Example() {
	r, err := ec2vm.NewRecord(profile(), identity, nil)
	if err != nil {
		log.Fatal(err)
	}
	// Persist r before calling Step; Next itself performs no I/O.
	fmt.Println(r.Desired, ec2vm.Next(r, ec2vm.Options{}))
	// Output: present create
}

func ExampleDesired_String() {
	fmt.Println(ec2vm.DesiredAbsent.String())
	// Output: absent
}

func ExamplePhase_String() {
	fmt.Println(ec2vm.PhaseUnknown.String())
	// Output: unknown
}

func ExampleHold_String() {
	fmt.Println(ec2vm.HoldConflict.String())
	// Output: conflict
}

func ExampleAction_String() {
	fmt.Println(ec2vm.ActionRecover.String())
	// Output: recover
}
