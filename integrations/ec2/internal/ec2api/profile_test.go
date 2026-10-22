package ec2api

import (
	"strings"
	"testing"
)

func TestProfileValidate(t *testing.T) {
	if err := testProfile().Validate(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		change func(*Profile)
	}{
		{"short account", func(p *Profile) { p.Account = "12345678901" }},
		{"region", func(p *Profile) { p.Region = "us_west_2" }},
		{"china region", func(p *Profile) { p.Region = "cn-north-1" }},
		{"iso region", func(p *Profile) { p.Region = "us-iso-east-1" }},
		{"subnet", func(p *Profile) { p.SubnetID = "subnet-0123" }},
		{"image", func(p *Profile) { p.ImageID = "ami-0123456789ABCDEF0" }},
		{"instance type", func(p *Profile) { p.InstanceType = "t3" }},
		{"key name", func(p *Profile) { p.KeyName = "my key" }},
		{"root device", func(p *Profile) { p.RootDevice = "xvda" }},
		{"no volume", func(p *Profile) { p.RootVolumeGiB = 0 }},
		{"huge volume", func(p *Profile) { p.RootVolumeGiB = 16385 }},
		{"volume type", func(p *Profile) { p.RootVolumeType = "st1" }},
		{"no groups", func(p *Profile) { p.SecurityGroupIDs = nil }},
		{"six groups", func(p *Profile) {
			p.SecurityGroupIDs = []string{"sg-00000001", "sg-00000002", "sg-00000003", "sg-00000004", "sg-00000005", "sg-00000006"}
		}},
		{"bad group", func(p *Profile) { p.SecurityGroupIDs = []string{"sg-x"} }},
		{"repeated group", func(p *Profile) { p.SecurityGroupIDs = []string{"sg-00000001", "sg-00000001"} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := testProfile()
			tt.change(p)
			if err := p.Validate(); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestProfileHash(t *testing.T) {
	p := testProfile()
	if got, want := p.Hash(), "b85c987d09a77a0bacd2bed09255a284b8c768fc6e454da9e67a17c8210f3e4d"; got != want {
		t.Fatalf("Hash = %s, want %s", got, want)
	}
	q := testProfile()
	q.SecurityGroupIDs = []string{q.SecurityGroupIDs[1], q.SecurityGroupIDs[0]}
	if q.Hash() != p.Hash() {
		t.Error("security group order changed the hash")
	}
	if strings.Join(p.SecurityGroupIDs, ",") != "sg-0bbbbbbbbbbbbbbbb,sg-0aaaaaaaaaaaaaaaa" {
		t.Error("Hash reordered the caller's security groups")
	}
	q.EncryptRoot = false
	if q.Hash() == p.Hash() {
		t.Error("encryption did not change the hash")
	}
}

func TestClientToken(t *testing.T) {
	base := testIdentity()
	if got, want := base.ClientToken(), "skygo-55e39499ed37635201145febc23225f7968c94e1b96a477577cc0dedc0"; got != want {
		t.Fatalf("ClientToken = %s, want %s", got, want)
	}
	if base.ClientToken() != testIdentity().ClientToken() {
		t.Fatal("equal identities produced different tokens")
	}
	tests := []struct {
		name string
		id   Identity
	}{
		{"generation", Identity{Owner: base.Owner, Environment: base.Environment, Generation: 2}},
		{"owner", Identity{Owner: "install-7f3b", Environment: base.Environment, Generation: 1}},
		{"environment", Identity{Owner: base.Owner, Environment: "env-0123456789abcdee", Generation: 1}},
		// Raw concatenation would make these equal.
		{"boundary", Identity{Owner: base.Owner + "e", Environment: strings.TrimPrefix(base.Environment, "e"), Generation: 1}},
	}
	for _, tt := range tests {
		if tt.id.ClientToken() == base.ClientToken() {
			t.Errorf("%s: token did not change", tt.name)
		}
	}
	long := Identity{Owner: strings.Repeat("o", 128), Environment: strings.Repeat("e", 128), Generation: 1<<64 - 1}
	for _, id := range []Identity{base, long} {
		tok := id.ClientToken()
		if len(tok) != 64 || !strings.HasPrefix(tok, "skygo-") {
			t.Errorf("token %q is not 64 characters with the skygo- prefix", tok)
		}
		for _, c := range tok[len("skygo-"):] {
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				t.Errorf("token %q has non-hex character %q", tok, c)
			}
		}
	}
}

func TestIdentityValidate(t *testing.T) {
	tests := []struct {
		id Identity
		ok bool
	}{
		{testIdentity(), true},
		{Identity{Owner: "o", Environment: "e", Generation: 1, Lease: "lease-1"}, true},
		{Identity{Owner: "", Environment: "e", Generation: 1}, false},
		{Identity{Owner: "o", Environment: "", Generation: 1}, false},
		{Identity{Owner: "o", Environment: "e", Generation: 0}, false},
		{Identity{Owner: "o/x", Environment: "e", Generation: 1}, false},
		{Identity{Owner: strings.Repeat("o", 129), Environment: "e", Generation: 1}, false},
		{Identity{Owner: "o", Environment: "é", Generation: 1}, false},
		{Identity{Owner: "o", Environment: "e", Generation: 1, Lease: "Lease"}, false},
	}
	for _, tt := range tests {
		if err := tt.id.Validate(); (err == nil) != tt.ok {
			t.Errorf("Validate(%+v) = %v, want ok %v", tt.id, err, tt.ok)
		}
	}
}
