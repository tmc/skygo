package ec2api

import (
	"errors"
	"slices"
	"testing"
)

const ownedID = "i-0123456789abcdef0"

// ownedReservation returns the reservation EC2 would report for req.
func ownedReservation(req *CreateRequest) Reservation {
	l := req.spec.launch
	return Reservation{
		ID:      "r-0123456789abcdef0",
		OwnerID: "123456789012",
		Instances: []Instance{{
			ID:               ownedID,
			State:            "running",
			ImageID:          l.ImageID,
			InstanceType:     l.InstanceType,
			SubnetID:         l.SubnetID,
			VPCID:            "vpc-0123456789abcdef0",
			AvailabilityZone: "us-west-2b",
			ClientToken:      req.ClientToken(),
			SecurityGroupIDs: slices.Clone(l.SecurityGroupIDs),
			Tags:             slices.Clone(req.spec.tags),
		}},
	}
}

func TestVerifyOwner(t *testing.T) {
	req := testRequest(t)
	r := ownedReservation(req)
	inst, err := VerifyOwner(testProfile(), req, []Reservation{r}, ownedID)
	if err != nil {
		t.Fatal(err)
	}
	inst.Tags[0].Value = "changed"
	if r.Instances[0].Tags[0].Value == "changed" {
		t.Error("VerifyOwner returned an alias of its input")
	}
}

func TestVerifyOwnerRefuses(t *testing.T) {
	req := testRequest(t)
	tests := []struct {
		name   string
		change func(*Reservation)
	}{
		{"account", func(r *Reservation) { r.OwnerID = "210987654321" }},
		{"other region", func(r *Reservation) { r.Instances[0].AvailabilityZone = "us-east-1a" }},
		{"region prefix", func(r *Reservation) { r.Instances[0].AvailabilityZone = "us-west-22a" }},
		{"no zone", func(r *Reservation) { r.Instances[0].AvailabilityZone = "" }},
		{"token", func(r *Reservation) { r.Instances[0].ClientToken = "skygo-other" }},
		{"no token", func(r *Reservation) { r.Instances[0].ClientToken = "" }},
		{"owner tag value", func(r *Reservation) { r.Instances[0].Tags[3].Value = "install-other" }},
		{"tag subset", func(r *Reservation) { r.Instances[0].Tags = r.Instances[0].Tags[:4] }},
		{"extra tag", func(r *Reservation) { r.Instances[0].Tags = append(r.Instances[0].Tags, Tag{"z", "z"}) }},
		{"no tags", func(r *Reservation) { r.Instances[0].Tags = nil }},
		{"image", func(r *Reservation) { r.Instances[0].ImageID = "ami-0fedcba9876543210" }},
		{"type", func(r *Reservation) { r.Instances[0].InstanceType = "t3.large" }},
		{"subnet", func(r *Reservation) { r.Instances[0].SubnetID = "subnet-0fedcba9876543210" }},
		{"key", func(r *Reservation) { r.Instances[0].KeyName = "other" }},
		{"groups", func(r *Reservation) { r.Instances[0].SecurityGroupIDs = r.Instances[0].SecurityGroupIDs[:1] }},
		{"other ID", func(r *Reservation) { r.Instances[0].ID = "i-0fedcba9876543210" }},
		{"two instances", func(r *Reservation) {
			other := r.Instances[0]
			other.ID = "i-0fedcba9876543210"
			r.Instances = append(r.Instances, other)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := ownedReservation(req)
			tt.change(&r)
			_, err := VerifyOwner(testProfile(), req, []Reservation{r}, ownedID)
			if !errors.Is(err, ErrNotOwned) {
				t.Fatalf("error = %v, want ErrNotOwned", err)
			}
		})
	}
}

func TestVerifyOwnerTwoReservations(t *testing.T) {
	req := testRequest(t)
	a, b := ownedReservation(req), ownedReservation(req)
	b.Instances[0].ID = "i-0fedcba9876543210"
	if _, err := VerifyOwner(testProfile(), req, []Reservation{a, b}, ownedID); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("error = %v, want ErrNotOwned", err)
	}
}

func TestVerifyOwnerNotObserved(t *testing.T) {
	req := testRequest(t)
	for _, rs := range [][]Reservation{nil, {{ID: "r-0123456789abcdef0", OwnerID: "123456789012"}}} {
		_, err := VerifyOwner(testProfile(), req, rs, ownedID)
		if !errors.Is(err, ErrNotObserved) || errors.Is(err, ErrNotOwned) {
			t.Fatalf("error = %v, want ErrNotObserved", err)
		}
	}
}

func TestVerifyOwnerProfile(t *testing.T) {
	req := testRequest(t)
	r := ownedReservation(req)
	p := testProfile()
	p.Region = "us-east-1"
	if _, err := VerifyOwner(p, req, []Reservation{r}, ownedID); err == nil {
		t.Fatal("request for another profile accepted")
	}
	if _, err := VerifyOwner(testProfile(), req, []Reservation{r}, "i-bad"); err == nil {
		t.Fatal("invalid ID accepted")
	}
}
