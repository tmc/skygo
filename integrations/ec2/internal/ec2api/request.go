package ec2api

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	ec2Version = "2016-11-15"
	stsVersion = "2011-06-15"

	// MaxUserData is the EC2 limit on raw user data bytes.
	MaxUserData = 16 << 10

	// MaxCreateBody bounds the bytes of a RunInstances request.
	MaxCreateBody = 32 << 10
)

// Tag keys applied to the instance, its root volume and its network
// interface at creation.
const (
	TagOwner       = "skygo.tmc.dev/owner"
	TagEnvironment = "skygo.tmc.dev/environment"
	TagGeneration  = "skygo.tmc.dev/generation"
	TagLease       = "skygo.tmc.dev/lease"
	TagProfile     = "skygo.tmc.dev/profile"
	TagName        = "Name"
)

// taggedResources are the resource types RunInstances tags at creation.
var taggedResources = []string{"instance", "volume", "network-interface"}

var hashRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

// A Tag is an EC2 resource tag.
type Tag struct {
	Key, Value string
}

// A CreateRequest is an immutable RunInstances request for one instance.
// Reissuing it sends the same ClientToken and the same body bytes.
type CreateRequest struct {
	spec createSpec
	body []byte
}

// createSpec is the decoded content of a CreateRequest.
type createSpec struct {
	token    string
	launch   Profile // Account and Region are empty; groups are sorted
	tags     []Tag   // sorted by key
	userData []byte
}

// NewCreateRequest returns the RunInstances request that launches the
// instance for id from profile p. The request sets MinCount and MaxCount to
// 1, InstanceInitiatedShutdownBehavior to terminate, IMDSv2 with hop limit 1,
// a root volume deleted on termination with the volume type's baseline
// performance, the profile's CPU credit option if any, no instance profile,
// and identical owner tags on the instance, volume and network interface.
// userData is optional and at most MaxUserData bytes.
func NewCreateRequest(p *Profile, id Identity, userData []byte) (*CreateRequest, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if err := id.Validate(); err != nil {
		return nil, err
	}
	if len(userData) > MaxUserData {
		return nil, fmt.Errorf("user data is %d bytes, more than %d", len(userData), MaxUserData)
	}
	launch := *p
	launch.Account, launch.Region = "", ""
	launch.SecurityGroupIDs = sortedGroups(p.SecurityGroupIDs)
	s := createSpec{
		token:    id.ClientToken(),
		launch:   launch,
		tags:     ownerTags(id, p.Hash()),
		userData: bytes.Clone(userData),
	}
	body := s.encode()
	if len(body) > MaxCreateBody {
		return nil, fmt.Errorf("create request is %d bytes, more than %d", len(body), MaxCreateBody)
	}
	return &CreateRequest{spec: s, body: body}, nil
}

// ParseCreateRequest parses body, which must be exactly the bytes of a
// request NewCreateRequest returned.
func ParseCreateRequest(body []byte) (*CreateRequest, error) {
	if len(body) > MaxCreateBody {
		return nil, fmt.Errorf("create request is %d bytes, more than %d", len(body), MaxCreateBody)
	}
	v, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, fmt.Errorf("parse create request: %w", err)
	}
	get := func(k string) string {
		if len(v[k]) != 1 {
			return ""
		}
		return v[k][0]
	}
	const bdm = "BlockDeviceMapping.1."
	s := createSpec{
		token: get("ClientToken"),
		launch: Profile{
			SubnetID:       get("SubnetId"),
			ImageID:        get("ImageId"),
			InstanceType:   get("InstanceType"),
			CPUCredits:     get("CreditSpecification.CpuCredits"),
			KeyName:        get("KeyName"),
			RootDevice:     get(bdm + "DeviceName"),
			RootVolumeType: get(bdm + "Ebs.VolumeType"),
			EncryptRoot:    get(bdm+"Ebs.Encrypted") == "true",
		},
	}
	s.launch.RootVolumeGiB, _ = strconv.Atoi(get(bdm + "Ebs.VolumeSize"))
	for i := 1; i <= maxSecurityGroups+1; i++ {
		g := get("SecurityGroupId." + strconv.Itoa(i))
		if g == "" {
			break
		}
		s.launch.SecurityGroupIDs = append(s.launch.SecurityGroupIDs, g)
	}
	for i := 1; i <= maxTags+1; i++ {
		prefix := "TagSpecification.1.Tag." + strconv.Itoa(i) + "."
		k := get(prefix + "Key")
		if k == "" {
			break
		}
		s.tags = append(s.tags, Tag{k, get(prefix + "Value")})
	}
	if ud := get("UserData"); ud != "" {
		s.userData, err = base64.StdEncoding.Strict().DecodeString(ud)
		if err != nil {
			return nil, errors.New("create request user data is not base64")
		}
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	if !bytes.Equal(s.encode(), body) {
		return nil, errors.New("create request is not in canonical form")
	}
	return &CreateRequest{spec: s, body: bytes.Clone(body)}, nil
}

// ClientToken returns the request's ClientToken.
func (r *CreateRequest) ClientToken() string { return r.spec.token }

// Body returns a copy of the request's form-encoded body.
func (r *CreateRequest) Body() []byte { return bytes.Clone(r.body) }

// Identity returns the identity recorded in the request's tags.
func (r *CreateRequest) Identity() Identity {
	id, _ := identityFromTags(r.spec.tags)
	return id
}

// ProfileHash returns the profile hash recorded in the request's tags.
func (r *CreateRequest) ProfileHash() string { return tagValue(r.spec.tags, TagProfile) }

// matches reports whether r launches into profile p.
func (r *CreateRequest) matches(p *Profile) error {
	l := r.spec.launch
	switch {
	case r.ProfileHash() != p.Hash():
		return errors.New("create request was built for a different profile")
	case l.SubnetID != p.SubnetID || l.ImageID != p.ImageID || l.InstanceType != p.InstanceType ||
		l.KeyName != p.KeyName || l.CPUCredits != p.CPUCredits || l.RootDevice != p.RootDevice || l.RootVolumeGiB != p.RootVolumeGiB ||
		l.RootVolumeType != p.RootVolumeType || l.EncryptRoot != p.EncryptRoot ||
		!slices.Equal(l.SecurityGroupIDs, sortedGroups(p.SecurityGroupIDs)):
		return errors.New("create request does not match the profile")
	}
	return nil
}

const maxTags = 6

// ownerTags returns the sorted tags that identify id's instance.
func ownerTags(id Identity, profileHash string) []Tag {
	name := id.Environment
	if len(name) > 8 {
		name = name[:8]
	}
	if id.Lease != "" {
		name = id.Lease + "-" + name
	}
	tags := []Tag{
		{TagName, "skygo-" + name},
		{TagEnvironment, id.Environment},
		{TagGeneration, strconv.FormatUint(id.Generation, 10)},
		{TagOwner, id.Owner},
		{TagProfile, profileHash},
	}
	if id.Lease != "" {
		tags = append(tags, Tag{TagLease, id.Lease})
	}
	slices.SortFunc(tags, func(a, b Tag) int { return strings.Compare(a.Key, b.Key) })
	return tags
}

func tagValue(tags []Tag, key string) string {
	for _, t := range tags {
		if t.Key == key {
			return t.Value
		}
	}
	return ""
}

func identityFromTags(tags []Tag) (Identity, error) {
	gen, err := strconv.ParseUint(tagValue(tags, TagGeneration), 10, 64)
	if err != nil {
		return Identity{}, errors.New("create request generation tag is invalid")
	}
	id := Identity{
		Owner:       tagValue(tags, TagOwner),
		Environment: tagValue(tags, TagEnvironment),
		Generation:  gen,
		Lease:       tagValue(tags, TagLease),
	}
	return id, id.Validate()
}

func (s *createSpec) validate() error {
	if err := s.launch.validateLaunch(); err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if !slices.IsSorted(s.launch.SecurityGroupIDs) {
		return errors.New("create request security groups are not sorted")
	}
	if len(s.userData) > MaxUserData {
		return errors.New("create request user data is too large")
	}
	id, err := identityFromTags(s.tags)
	if err != nil {
		return err
	}
	profile := tagValue(s.tags, TagProfile)
	if !hashRE.MatchString(profile) {
		return errors.New("create request profile tag is invalid")
	}
	if !slices.Equal(s.tags, ownerTags(id, profile)) {
		return errors.New("create request tags are not the owner tags")
	}
	if s.token != id.ClientToken() {
		return errors.New("create request client token does not match its identity")
	}
	return nil
}

// encode returns the canonical form-encoded RunInstances body.
func (s *createSpec) encode() []byte {
	l := s.launch
	v := url.Values{}
	v.Set("Action", "RunInstances")
	v.Set("Version", ec2Version)
	v.Set("ClientToken", s.token)
	v.Set("ImageId", l.ImageID)
	v.Set("InstanceType", l.InstanceType)
	if l.CPUCredits != "" {
		v.Set("CreditSpecification.CpuCredits", l.CPUCredits)
	}
	v.Set("SubnetId", l.SubnetID)
	for i, g := range l.SecurityGroupIDs {
		v.Set("SecurityGroupId."+strconv.Itoa(i+1), g)
	}
	if l.KeyName != "" {
		v.Set("KeyName", l.KeyName)
	}
	v.Set("MinCount", "1")
	v.Set("MaxCount", "1")
	v.Set("InstanceInitiatedShutdownBehavior", "terminate")
	v.Set("MetadataOptions.HttpEndpoint", "enabled")
	v.Set("MetadataOptions.HttpTokens", "required")
	v.Set("MetadataOptions.HttpPutResponseHopLimit", "1")
	const bdm = "BlockDeviceMapping.1."
	v.Set(bdm+"DeviceName", l.RootDevice)
	v.Set(bdm+"Ebs.DeleteOnTermination", "true")
	v.Set(bdm+"Ebs.Encrypted", strconv.FormatBool(l.EncryptRoot))
	v.Set(bdm+"Ebs.VolumeSize", strconv.Itoa(l.RootVolumeGiB))
	v.Set(bdm+"Ebs.VolumeType", l.RootVolumeType)
	for i, rt := range taggedResources {
		prefix := "TagSpecification." + strconv.Itoa(i+1) + "."
		v.Set(prefix+"ResourceType", rt)
		for j, t := range s.tags {
			tp := prefix + "Tag." + strconv.Itoa(j+1) + "."
			v.Set(tp+"Key", t.Key)
			v.Set(tp+"Value", t.Value)
		}
	}
	if len(s.userData) > 0 {
		v.Set("UserData", base64.StdEncoding.EncodeToString(s.userData))
	}
	return []byte(v.Encode())
}

// queryBody returns the form-encoded body of a Query action with the given
// alternating parameter names and values.
func queryBody(action, version string, params ...string) []byte {
	v := url.Values{}
	v.Set("Action", action)
	v.Set("Version", version)
	for i := 0; i+1 < len(params); i += 2 {
		v.Set(params[i], params[i+1])
	}
	return []byte(v.Encode())
}

// describeByIDBody returns a DescribeInstances body for exactly one ID.
func describeByIDBody(id, nextToken string) []byte {
	params := []string{"InstanceId.1", id}
	if nextToken != "" {
		params = append(params, "NextToken", nextToken)
	}
	return queryBody("DescribeInstances", ec2Version, params...)
}

// describeByTokenBody returns a DescribeInstances body that filters by
// r's ClientToken and owner, environment and generation tags.
func describeByTokenBody(r *CreateRequest, nextToken string) []byte {
	params := []string{
		"Filter.1.Name", "client-token",
		"Filter.1.Value.1", r.spec.token,
		"MaxResults", strconv.Itoa(describePageSize),
	}
	for i, k := range []string{TagOwner, TagEnvironment, TagGeneration} {
		n := strconv.Itoa(i + 2)
		params = append(params, "Filter."+n+".Name", "tag:"+k, "Filter."+n+".Value.1", tagValue(r.spec.tags, k))
	}
	if nextToken != "" {
		params = append(params, "NextToken", nextToken)
	}
	return queryBody("DescribeInstances", ec2Version, params...)
}

const describePageSize = 50
