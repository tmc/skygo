package ec2api

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// A Profile is the fixed placement and image of one EC2 target.
type Profile struct {
	Account          string   // 12-digit account the credentials must belong to
	Region           string   // such as "us-east-1"
	SubnetID         string   // existing subnet
	SecurityGroupIDs []string // 1 to 5 existing groups; order is not significant
	ImageID          string   // AMI containing the skygo agent
	InstanceType     string   // such as "t3.micro"
	CPUCredits       string   // "standard" or "unlimited" for burstable types, or empty to send none
	KeyName          string   // existing key pair, or empty for none
	RootDevice       string   // root device name of ImageID, such as "/dev/xvda"
	RootVolumeGiB    int      // 1 to 16384
	RootVolumeType   string   // "gp2" or "gp3"
	EncryptRoot      bool
}

const maxSecurityGroups = 5

var (
	accountRE      = regexp.MustCompile(`^[0-9]{12}$`)
	regionRE       = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-[0-9]{1,2}$`)
	subnetRE       = regexp.MustCompile(`^subnet-([0-9a-f]{8}|[0-9a-f]{17})$`)
	groupRE        = regexp.MustCompile(`^sg-([0-9a-f]{8}|[0-9a-f]{17})$`)
	imageRE        = regexp.MustCompile(`^ami-([0-9a-f]{8}|[0-9a-f]{17})$`)
	instanceIDRE   = regexp.MustCompile(`^i-([0-9a-f]{8}|[0-9a-f]{17})$`)
	instanceTypeRE = regexp.MustCompile(`^[a-z0-9-]{1,16}\.[a-z0-9-]{1,16}$`)
	keyNameRE      = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)
	deviceRE       = regexp.MustCompile(`^/dev/[a-z0-9]{2,16}$`)
	identityRE     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	leaseRE        = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
)

// Validate reports whether p is complete and well formed.
// It does not check that the named resources exist.
func (p *Profile) Validate() error {
	switch {
	case !accountRE.MatchString(p.Account):
		return errors.New("profile account must be 12 digits")
	case !regionRE.MatchString(p.Region) || strings.HasPrefix(p.Region, "cn-") || strings.HasPrefix(p.Region, "us-iso"):
		return fmt.Errorf("profile region %q is not a supported region name", p.Region)
	}
	return p.validateLaunch()
}

// validateLaunch validates every field but Account and Region.
func (p *Profile) validateLaunch() error {
	switch {
	case !subnetRE.MatchString(p.SubnetID):
		return fmt.Errorf("profile subnet %q is not a subnet ID", p.SubnetID)
	case !imageRE.MatchString(p.ImageID):
		return fmt.Errorf("profile image %q is not an AMI ID", p.ImageID)
	case !instanceTypeRE.MatchString(p.InstanceType):
		return fmt.Errorf("profile instance type %q is invalid", p.InstanceType)
	case p.CPUCredits != "" && p.CPUCredits != "standard" && p.CPUCredits != "unlimited":
		return fmt.Errorf("profile CPU credits %q is not standard or unlimited", p.CPUCredits)
	case p.KeyName != "" && !keyNameRE.MatchString(p.KeyName):
		return fmt.Errorf("profile key name %q is invalid", p.KeyName)
	case !deviceRE.MatchString(p.RootDevice):
		return fmt.Errorf("profile root device %q is invalid", p.RootDevice)
	case p.RootVolumeGiB < 1 || p.RootVolumeGiB > 16384:
		return fmt.Errorf("profile root volume size %d GiB is out of range", p.RootVolumeGiB)
	case p.RootVolumeType != "gp2" && p.RootVolumeType != "gp3":
		return fmt.Errorf("profile root volume type %q is not gp2 or gp3", p.RootVolumeType)
	case len(p.SecurityGroupIDs) == 0 || len(p.SecurityGroupIDs) > maxSecurityGroups:
		return fmt.Errorf("profile needs 1 to %d security groups", maxSecurityGroups)
	}
	for _, g := range p.SecurityGroupIDs {
		if !groupRE.MatchString(g) {
			return fmt.Errorf("profile security group %q is invalid", g)
		}
	}
	if g := sortedGroups(p.SecurityGroupIDs); len(slices.Compact(g)) != len(p.SecurityGroupIDs) {
		return errors.New("profile lists a security group twice")
	}
	return nil
}

// Hash returns the lowercase hexadecimal SHA-256 of p's canonical encoding.
// Security group order does not affect it.
func (p *Profile) Hash() string {
	var e fieldEncoder
	e.string("skygo-ec2-profile-v1")
	e.string(p.Account)
	e.string(p.Region)
	e.string(p.SubnetID)
	groups := sortedGroups(p.SecurityGroupIDs)
	e.uint(uint64(len(groups)))
	for _, g := range groups {
		e.string(g)
	}
	e.string(p.ImageID)
	e.string(p.InstanceType)
	e.string(p.KeyName)
	e.string(p.RootDevice)
	e.uint(uint64(p.RootVolumeGiB))
	e.string(p.RootVolumeType)
	if p.EncryptRoot {
		e.uint(1)
	} else {
		e.uint(0)
	}
	e.string(p.CPUCredits)
	sum := sha256.Sum256(e.buf)
	return hex.EncodeToString(sum[:])
}

func sortedGroups(groups []string) []string {
	g := slices.Clone(groups)
	slices.Sort(g)
	return g
}

// An Identity names one generation of one skygo environment.
type Identity struct {
	Owner       string // installation owner
	Environment string // environment ID
	Generation  uint64 // positive; a new generation is a new instance
	Lease       string // native lease ID, or empty
}

// Validate reports whether id is complete and well formed.
func (id Identity) Validate() error {
	switch {
	case !identityRE.MatchString(id.Owner):
		return errors.New("identity owner must be 1 to 128 letters, digits or ._:-")
	case !identityRE.MatchString(id.Environment):
		return errors.New("identity environment must be 1 to 128 letters, digits or ._:-")
	case id.Generation == 0:
		return errors.New("identity generation must be positive")
	case id.Lease != "" && !leaseRE.MatchString(id.Lease):
		return errors.New("identity lease must be 1 to 32 lowercase letters, digits or hyphens")
	}
	return nil
}

const (
	tokenPrefix = "skygo-"
	maxTokenLen = 64 // EC2 ClientToken limit in ASCII characters
)

// ClientToken returns the RunInstances ClientToken for id: "skygo-"
// followed by the first 58 lowercase hexadecimal digits of the SHA-256 of a
// length-prefixed encoding of owner, environment and generation.
// The lease does not affect it.
func (id Identity) ClientToken() string {
	var e fieldEncoder
	e.string("skygo-ec2-token-v1")
	e.string(id.Owner)
	e.string(id.Environment)
	e.uint(id.Generation)
	sum := sha256.Sum256(e.buf)
	return tokenPrefix + hex.EncodeToString(sum[:])[:maxTokenLen-len(tokenPrefix)]
}

// A fieldEncoder writes an unambiguous sequence of length-prefixed fields.
type fieldEncoder struct {
	buf []byte
}

func (e *fieldEncoder) string(s string) {
	e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(len(s)))
	e.buf = append(e.buf, s...)
}

func (e *fieldEncoder) uint(v uint64) {
	e.buf = binary.BigEndian.AppendUint64(e.buf, v)
}
