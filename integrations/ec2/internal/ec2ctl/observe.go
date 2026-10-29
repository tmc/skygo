package ec2ctl

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
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

	// PublicIP and PrivateIP are the addresses the same description
	// reported, or the zero Addr where it reported none. Each is a valid
	// unicast address without a zone, as checked by Observe; an address
	// is a fact of this description only and grants no SSH host authority.
	PublicIP  netip.Addr
	PrivateIP netip.Addr
}

// Observe describes the instance record r names, refusing any instance
// [ec2api.VerifyOwner] does not accept: by its ID if r has one, otherwise
// by r's ClientToken if a create may have been sent. It only reads. An
// error, an empty search or an InvalidInstanceID.NotFound is not evidence
// that no instance exists.
//
// Observe refuses a description whose public or private address is not
// the canonical text of a unicast IP address. The model of what EC2 may
// report is checked here, before anything can connect to it.
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
	public, err := parseAddress("public", inst.PublicIP)
	if err != nil {
		return nil, err
	}
	private, err := parseAddress("private", inst.PrivateIP)
	if err != nil {
		return nil, err
	}
	return &Observation{
		Call:       call,
		InstanceID: inst.ID,
		State:      inst.State,
		At:         time.Now().UTC(),
		PublicIP:   public,
		PrivateIP:  private,
	}, nil
}

// Address returns the public address of o if public is set, and its
// private address otherwise. It refuses a public address that is in a
// private range and an address the description did not report: an
// operator selects one kind explicitly, and the other is never used in
// its place.
func (o *Observation) Address(public bool) (netip.Addr, error) {
	if o == nil {
		return netip.Addr{}, errors.New("ec2ctl: no observation")
	}
	kind, a := "private", o.PrivateIP
	if public {
		kind, a = "public", o.PublicIP
	}
	switch {
	case !a.IsValid():
		return netip.Addr{}, fmt.Errorf("ec2ctl: instance %s has no %s address", o.InstanceID, kind)
	case public && a.IsPrivate():
		return netip.Addr{}, fmt.Errorf("ec2ctl: instance %s public address is in a private range", o.InstanceID)
	}
	return a, nil
}

// parseAddress parses the address text EC2 reported. Empty text is the
// zero Addr. Any other text must be the canonical form of a global
// unicast IPv4 or IPv6 address, which includes private ranges but not
// loopback, link-local, multicast or unspecified addresses, with no zone
// and not IPv4-mapped.
func parseAddress(kind, s string) (netip.Addr, error) {
	if s == "" {
		return netip.Addr{}, nil
	}
	a, err := netip.ParseAddr(s)
	switch {
	case err != nil, a.String() != s, a.Zone() != "", a.Is4In6():
		return netip.Addr{}, fmt.Errorf("ec2ctl: described %s address is not a canonical IP address", kind)
	case !a.IsGlobalUnicast():
		return netip.Addr{}, fmt.Errorf("ec2ctl: described %s address is not a unicast address", kind)
	}
	return a, nil
}
