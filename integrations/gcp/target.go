package gce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"golang.org/x/oauth2"
)

// A Target pins the original resources of an execution target. Address is
// a canonical RFC 1918 IPv4 address, without a port. These operator-supplied
// pins must agree with the machine's successful create witnesses.
type Target struct {
	ComputeProjectID, Zone      string
	InstanceID, DiskID, ImageID string
	Address                     string
}

// VerifyTarget freshly verifies that target is the machine's admitted,
// originally created VM, running on its original disk and image at the
// exact private address. The machine must already have reached Running or
// Restarted, with no unresolved request. ctx must have a deadline; tok is
// an explicit finite bearer token under the same rules as Drive.
//
// VerifyTarget sends only GET requests and changes no durable or observed
// lifecycle state. It never admits, provisions, starts, retries a mutation
// or connects over SSH. An interrupted original or failed first body Close
// stays owned by m under the same custody rules as Drive. Successful
// verification is a point-in-time observation, not an atomic binding to a
// later SSH connection; callers must also verify the enrolled host key.
func (m *Machine) VerifyTarget(ctx context.Context, tok oauth2.Token, target Target) error {
	if err := m.usable(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("gce: verify target needs a context deadline")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("gce: verify target: %w", err)
	}
	if err := checkToken(&tok); err != nil {
		return fmt.Errorf("gce: verify target: %w", err)
	}
	ip, err := netip.ParseAddr(target.Address)
	if err != nil || !ip.Is4() || !ip.IsPrivate() || ip.String() != target.Address {
		return errors.New("gce: target address must be a canonical private IPv4 address")
	}
	r := m.rec
	if m.pending != nil || m.held != nil || r.Op != nil || r.Held != "" || !r.Reached ||
		r.Intent == 0 || r.CompletedIntent != r.Intent || r.Phase != phaseCreated ||
		(r.Desired != Running && (r.Desired != Restarted || r.Restart != "done")) {
		return errors.New("gce: target needs a completed running goal with no unresolved request")
	}
	if r.Admission == nil || r.DiskCreate == nil || r.InstanceCreate == nil || r.check() != nil {
		return errors.New("gce: target needs admission and original successful create witnesses")
	}
	if target.ComputeProjectID != r.Config.ComputeProjectID || target.Zone != r.Config.Zone ||
		target.InstanceID != r.InstanceID || target.DiskID != r.DiskID || target.ImageID != r.ImageID {
		return errors.New("gce: target pins differ from the original machine")
	}
	var p projectWire
	if err := m.get(ctx, &tok, "projects.get", "projects/"+r.Config.Project, "", &p); err != nil {
		return causes("gce: verify target project failed", m.pending, err)
	}
	if p.Name != r.Config.Project || !sameID(p.ID, target.ComputeProjectID) {
		return errors.New("gce: target project identity differs")
	}
	if v, n := p.CommonInstanceMetadata.value("enable-oslogin"); n > 1 || n == 1 && !strings.EqualFold(v, "false") {
		return errors.New("gce: target project enables OS Login")
	}
	var reply targetInstanceWire
	if err := m.get(ctx, &tok, "instances.get", m.instancePath(), "", &reply); err != nil {
		return causes("gce: verify target instance failed", m.pending, err)
	}
	// The response-only projection leaves all lifecycle request types and
	// their exact mutation bodies unchanged.
	vm := reply.instanceWire
	for _, nic := range reply.NetworkInterfaces {
		vm.NetworkInterfaces = append(vm.NetworkInterfaces, nic.networkWire)
	}
	if m.checkInstance(&vm) != nil || vm.Status != "RUNNING" ||
		len(reply.NetworkInterfaces) != 1 || reply.NetworkInterfaces[0].Address != target.Address ||
		len(reply.NetworkInterfaces[0].IPv6AccessConfigs) != 0 || reply.NetworkInterfaces[0].IPv6AccessType != "" ||
		reply.NetworkInterfaces[0].IPv6Address != "" ||
		(reply.NetworkInterfaces[0].StackType != "" && reply.NetworkInterfaces[0].StackType != "IPV4_ONLY") {
		return errors.New("gce: target instance identity, state or address differs")
	}
	var disk diskWire
	if err := m.get(ctx, &tok, "disks.get", m.diskPath(), "", &disk); err != nil {
		return causes("gce: verify target disk failed", m.pending, err)
	}
	if m.checkDisk(&disk) != nil || disk.Status != "READY" || len(disk.Users) != 1 || disk.SourceImageID != target.ImageID {
		return errors.New("gce: target disk identity, state or image differs")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("gce: verify target: %w", err)
	}
	return nil
}

// targetInstanceWire adds observed addresses without changing the wire
// types shared with lifecycle mutation bodies.
type targetInstanceWire struct {
	instanceWire
	NetworkInterfaces []targetNetworkWire `json:"networkInterfaces"`
}

type targetNetworkWire struct {
	networkWire
	Address           string            `json:"networkIP"`
	IPv6AccessConfigs []json.RawMessage `json:"ipv6AccessConfigs"`
	IPv6AccessType    string            `json:"ipv6AccessType"`
	IPv6Address       string            `json:"ipv6Address"`
	StackType         string            `json:"stackType"`
}
