package ec2ctl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
)

// A Config is the operator's fixed description of one VM: the profile it
// launches into, the identity it is created for and its user data. None of
// it may change for the life of a state directory.
type Config struct {
	Profile  ec2api.Profile
	Identity ec2api.Identity
	UserData []byte
}

const (
	configVersion  = 1
	maxConfig      = 64 << 10
	maxCredentials = 8 << 10
)

type wireProfile struct {
	Account          string   `json:"account"`
	Region           string   `json:"region"`
	SubnetID         string   `json:"subnet_id"`
	SecurityGroupIDs []string `json:"security_group_ids"`
	ImageID          string   `json:"image_id"`
	InstanceType     string   `json:"instance_type"`
	CPUCredits       string   `json:"cpu_credits"`
	KeyName          string   `json:"key_name"`
	RootDevice       string   `json:"root_device"`
	RootVolumeGiB    int      `json:"root_volume_gib"`
	RootVolumeType   string   `json:"root_volume_type"`
	EncryptRoot      bool     `json:"encrypt_root"`
}

func toWireProfile(p *ec2api.Profile) wireProfile {
	return wireProfile{
		Account:          p.Account,
		Region:           p.Region,
		SubnetID:         p.SubnetID,
		SecurityGroupIDs: p.SecurityGroupIDs,
		ImageID:          p.ImageID,
		InstanceType:     p.InstanceType,
		CPUCredits:       p.CPUCredits,
		KeyName:          p.KeyName,
		RootDevice:       p.RootDevice,
		RootVolumeGiB:    p.RootVolumeGiB,
		RootVolumeType:   p.RootVolumeType,
		EncryptRoot:      p.EncryptRoot,
	}
}

func (w wireProfile) profile() ec2api.Profile {
	return ec2api.Profile{
		Account:          w.Account,
		Region:           w.Region,
		SubnetID:         w.SubnetID,
		SecurityGroupIDs: w.SecurityGroupIDs,
		ImageID:          w.ImageID,
		InstanceType:     w.InstanceType,
		CPUCredits:       w.CPUCredits,
		KeyName:          w.KeyName,
		RootDevice:       w.RootDevice,
		RootVolumeGiB:    w.RootVolumeGiB,
		RootVolumeType:   w.RootVolumeType,
		EncryptRoot:      w.EncryptRoot,
	}
}

type wireIdentity struct {
	Owner       string `json:"owner"`
	Environment string `json:"environment"`
	Generation  uint64 `json:"generation"`
	Lease       string `json:"lease"`
}

type wireConfig struct {
	Version  int          `json:"version"`
	Profile  wireProfile  `json:"profile"`
	Identity wireIdentity `json:"identity"`
	UserData string       `json:"user_data"`
}

// ParseConfig parses a version 1 JSON configuration of at most 64 KiB:
//
//	{
//		"version": 1,
//		"profile": {"account": ..., "region": ..., "subnet_id": ...,
//			"security_group_ids": [...], "image_id": ..., "instance_type": ...,
//			"cpu_credits": ..., "key_name": ..., "root_device": ...,
//			"root_volume_gib": ..., "root_volume_type": ..., "encrypt_root": ...},
//		"identity": {"owner": ..., "environment": ..., "generation": ..., "lease": ...},
//		"user_data": "..."
//	}
//
// It refuses unknown fields, trailing data and a configuration from which
// [ec2api.NewCreateRequest] cannot build a request. ParseConfig reads r
// until EOF or the size bound and does not bound how long that takes; to
// read a file, use [LoadConfig].
func ParseConfig(r io.Reader) (*Config, error) {
	data, err := readAtMost(r, maxConfig)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	var w wireConfig
	if err := decodeStrict(data, &w); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if w.Version != configVersion {
		return nil, fmt.Errorf("config version %d is not supported", w.Version)
	}
	c := &Config{
		Profile: w.Profile.profile(),
		Identity: ec2api.Identity{
			Owner:       w.Identity.Owner,
			Environment: w.Identity.Environment,
			Generation:  w.Identity.Generation,
			Lease:       w.Identity.Lease,
		},
		UserData: []byte(w.UserData),
	}
	if _, err := ec2api.NewCreateRequest(&c.Profile, c.Identity, c.UserData); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return c, nil
}

// LoadConfig reads and parses, as [ParseConfig] does, the configuration in
// path, a private regular file (see the package documentation).
func LoadConfig(path string) (*Config, error) {
	data, err := readPrivate(path, maxConfig)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return ParseConfig(bytes.NewReader(data))
}

// checkConfig reports whether cfg is a configuration from which
// [ec2api.NewCreateRequest] builds a request. It neither changes nor keeps
// cfg.
func checkConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("ec2ctl: no configuration")
	}
	if _, err := ec2api.NewCreateRequest(&cfg.Profile, cfg.Identity, cfg.UserData); err != nil {
		return fmt.Errorf("ec2ctl: invalid configuration: %w", err)
	}
	return nil
}

type wireCredentials struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token"`
}

var accessKeyRE = regexp.MustCompile(`^[A-Z0-9]{16,128}$`)

// LoadCredentials reads static AWS credentials from path, a private
// regular file (see the package documentation) of at most 8 KiB holding
//
//	{"access_key_id": ..., "secret_access_key": ..., "session_token": ...}
//
// where session_token may be omitted. The returned provider returns those
// credentials and reads nothing else: no environment, shared
// configuration or instance metadata. Errors never quote the file.
func LoadCredentials(path string) (aws.CredentialsProvider, error) {
	data, err := readPrivate(path, maxCredentials)
	if err != nil {
		return nil, fmt.Errorf("credentials: %w", err)
	}
	var w wireCredentials
	if err := decodeStrict(data, &w); err != nil {
		return nil, errors.New("credentials: file is not a JSON object of access_key_id, secret_access_key and optional session_token")
	}
	switch {
	case !accessKeyRE.MatchString(w.AccessKeyID):
		return nil, errors.New("credentials: access key ID is malformed")
	case !printable(w.SecretAccessKey, 256):
		return nil, errors.New("credentials: secret access key is malformed")
	case w.SessionToken != "" && !printable(w.SessionToken, 4096):
		return nil, errors.New("credentials: session token is malformed")
	}
	creds := aws.Credentials{
		AccessKeyID:     w.AccessKeyID,
		SecretAccessKey: w.SecretAccessKey,
		SessionToken:    w.SessionToken,
		Source:          "skygo-ec2 credentials file",
	}
	return aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return creds, nil
	}), nil
}

// printable reports whether s is 1 to max printable ASCII bytes other
// than space.
func printable(s string, max int) bool {
	if s == "" || len(s) > max {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] <= ' ' || s[i] > '~' {
			return false
		}
	}
	return true
}

// readAtMost reads all of r, refusing more than max bytes.
func readAtMost(r io.Reader, max int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > max {
		return nil, fmt.Errorf("input exceeds %d bytes", max)
	}
	return data, nil
}

// decodeStrict decodes exactly one JSON value from data into v, refusing
// unknown fields and trailing data.
func decodeStrict(data []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing data after the JSON value")
	}
	return nil
}
