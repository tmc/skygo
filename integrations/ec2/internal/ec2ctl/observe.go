package ec2ctl

import (
	"context"
	"errors"
	"time"

	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2vm"
)

// An Observation is a fresh, owned description of a VM. It is not stored
// and does not change the VM's record.
type Observation struct {
	Call       string    // what was asked: "DescribeInstances by ID" or "DescribeInstances by client token"
	InstanceID string    // the owned instance described
	State      string    // its EC2 state, such as "running"
	At         time.Time // when the description was received
}

// Observe describes the instance record r names, refusing any instance
// [ec2api.VerifyOwner] does not accept: by its ID if r has one, otherwise
// by r's ClientToken if a create may have been sent. It only reads. An
// error, an empty search or an InvalidInstanceID.NotFound is not evidence
// that no instance exists.
//
// A Drive cannot serve as a status refresh: for a record that is running
// and wants present, ec2vm.Next is ActionNone and no call is made.
func Observe(ctx context.Context, c *ec2api.Client, r ec2vm.Record) (*Observation, error) {
	req, err := ec2api.ParseCreateRequest(r.Request)
	if err != nil {
		return nil, err
	}
	var (
		inst *ec2api.Instance
		call string
	)
	switch {
	case r.InstanceID != "":
		call = "DescribeInstances by ID"
		inst, err = c.DescribeOwned(ctx, req, r.InstanceID)
	case r.CreateSent:
		call = "DescribeInstances by client token"
		inst, err = c.FindOwned(ctx, req)
	default:
		return nil, errors.New("ec2ctl: no create was sent; there is nothing to observe")
	}
	if err != nil {
		return nil, err
	}
	return &Observation{Call: call, InstanceID: inst.ID, State: inst.State, At: time.Now().UTC()}, nil
}
