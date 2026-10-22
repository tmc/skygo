package ec2api

import (
	"fmt"
	"slices"
	"strings"
)

// VerifyOwner returns instance id if reservations contain exactly that one
// instance and it is the instance req launched into profile p: the
// reservation belongs to p's account, the instance is in p's region, and its
// ClientToken, its complete tag set, image, type, subnet, key pair and
// security groups all equal the request's.
//
// An instance whose tags were changed after launch, including by an added
// tag, is not owned. VerifyOwner reads an observation; it is not an atomic
// precondition on a later mutation, so whoever can change tags or the
// instance must be trusted not to do so concurrently.
//
// If reservations contain no instance, VerifyOwner returns an error
// wrapping ErrNotObserved. Otherwise a refusal wraps ErrNotOwned.
func VerifyOwner(p *Profile, req *CreateRequest, reservations []Reservation, id string) (*Instance, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if err := req.matches(p); err != nil {
		return nil, err
	}
	if !instanceIDRE.MatchString(id) {
		return nil, fmt.Errorf("instance ID %q is invalid", id)
	}
	var (
		found *Instance
		owner string
		n     int
	)
	for i := range reservations {
		for j := range reservations[i].Instances {
			n++
			if inst := &reservations[i].Instances[j]; inst.ID == id {
				found, owner = inst, reservations[i].OwnerID
			}
		}
	}
	switch {
	case n == 0:
		return nil, fmt.Errorf("instance %s: %w", id, ErrNotObserved)
	case n > 1:
		return nil, fmt.Errorf("%w: observed %d instances, want exactly %s", ErrNotOwned, n, id)
	case found == nil:
		return nil, fmt.Errorf("%w: observed a different instance, want %s", ErrNotOwned, id)
	}
	mismatch := func(what string) error {
		return fmt.Errorf("%w: instance %s %s does not match", ErrNotOwned, id, what)
	}
	l := &req.spec.launch
	zone := found.AvailabilityZone
	switch {
	case owner != p.Account:
		return nil, mismatch("account")
	case len(zone) != len(p.Region)+1 || !strings.HasPrefix(zone, p.Region) || zone[len(zone)-1] < 'a' || zone[len(zone)-1] > 'z':
		return nil, mismatch("region")
	case found.ClientToken != req.spec.token:
		return nil, mismatch("client token")
	case !slices.Equal(found.Tags, req.spec.tags):
		return nil, mismatch("tags")
	case found.ImageID != l.ImageID:
		return nil, mismatch("image")
	case found.InstanceType != l.InstanceType:
		return nil, mismatch("instance type")
	case found.SubnetID != l.SubnetID:
		return nil, mismatch("subnet")
	case found.KeyName != l.KeyName:
		return nil, mismatch("key pair")
	case !slices.Equal(found.SecurityGroupIDs, l.SecurityGroupIDs):
		return nil, mismatch("security groups")
	}
	inst := *found
	inst.SecurityGroupIDs = slices.Clone(found.SecurityGroupIDs)
	inst.Tags = slices.Clone(found.Tags)
	return &inst, nil
}
