package gce

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// An Image is one exact, already published Compute Engine image version.
// Name is never an image family; ID is the decimal numeric image id that
// images.get must report for Project and Name.
type Image struct {
	Project string `json:"project"`
	Name    string `json:"name"`
	ID      string `json:"id"`
}

// A Config is the fixed operator configuration of one machine. A state
// directory records the exact Config it was created with and refuses any
// other.
//
// Project is the project name used in REST paths. ComputeProjectID is the
// numeric id that Compute Engine projects.get reports for Project; it is
// not assumed to equal a Resource Manager project number. MachineType,
// Subnet and DiskType are canonical partial resource paths:
//
//	zones/ZONE/machineTypes/TYPE
//	projects/PROJECT/regions/REGION/subnetworks/NAME
//	zones/ZONE/diskTypes/TYPE
//
// DiskGiB is the boot disk size, 10 to 65536. SSHUser and AuthorizedKey
// form the single instance-level ssh-keys entry; AuthorizedKey is one
// OpenSSH ssh-ed25519 public key with no options. No other key type is
// supported.
type Config struct {
	Project          string `json:"project"`
	ComputeProjectID string `json:"compute_project_id"`
	Zone             string `json:"zone"`
	MachineType      string `json:"machine_type"`
	Subnet           string `json:"subnet"`
	Image            Image  `json:"image"`
	DiskType         string `json:"disk_type"`
	DiskGiB          int64  `json:"disk_gib"`
	SSHUser          string `json:"ssh_user"`
	AuthorizedKey    string `json:"authorized_key"`
}

const (
	maxConfigBytes = 16 << 10
	minDiskGiB     = 10
	maxDiskGiB     = 65536
	maxKeyBytes    = 4096
)

var (
	projectRE  = regexp.MustCompile(`^[a-z][-a-z0-9]{4,28}[a-z0-9]$`)
	zoneRE     = regexp.MustCompile(`^([a-z]+-[a-z]+[0-9]+)-[a-z]$`)
	nameRE     = regexp.MustCompile(`^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$`)
	userRE     = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,31}$`)
	errTrailer = errors.New("trailing data after JSON value")
)

// ParseConfig reads one JSON Config from r and validates it. It refuses
// unknown or duplicate fields, trailing data and inputs over 16 KiB.
func ParseConfig(r io.Reader) (Config, error) {
	var cfg Config
	data, err := readBounded(r, maxConfigBytes)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	if err := decodeStrict(data, &cfg, true); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

// validate reports whether every field of c is present and canonical.
func (c *Config) validate() error {
	if !projectRE.MatchString(c.Project) {
		return fmt.Errorf("project %q is not a project name", c.Project)
	}
	if err := checkID("compute_project_id", c.ComputeProjectID); err != nil {
		return err
	}
	m := zoneRE.FindStringSubmatch(c.Zone)
	if m == nil {
		return fmt.Errorf("zone %q is not a zone name", c.Zone)
	}
	region := m[1]
	if err := checkPath("machine_type", c.MachineType, "zones", c.Zone, "machineTypes"); err != nil {
		return err
	}
	if err := checkPath("disk_type", c.DiskType, "zones", c.Zone, "diskTypes"); err != nil {
		return err
	}
	f := strings.Split(c.Subnet, "/")
	if len(f) != 6 || f[0] != "projects" || !projectRE.MatchString(f[1]) || f[2] != "regions" || f[3] != region || f[4] != "subnetworks" || !nameRE.MatchString(f[5]) {
		return fmt.Errorf("subnet %q is not projects/PROJECT/regions/%s/subnetworks/NAME", c.Subnet, region)
	}
	if !projectRE.MatchString(c.Image.Project) {
		return fmt.Errorf("image project %q is not a project name", c.Image.Project)
	}
	if !nameRE.MatchString(c.Image.Name) {
		return fmt.Errorf("image name %q is not an image name", c.Image.Name)
	}
	if err := checkID("image id", c.Image.ID); err != nil {
		return err
	}
	if c.DiskGiB < minDiskGiB || c.DiskGiB > maxDiskGiB {
		return fmt.Errorf("disk_gib %d is not %d to %d", c.DiskGiB, minDiskGiB, maxDiskGiB)
	}
	if !userRE.MatchString(c.SSHUser) {
		return fmt.Errorf("ssh_user %q is not a user name", c.SSHUser)
	}
	return checkKey(c.AuthorizedKey)
}

// checkPath reports whether p is exactly kind/scope/collection/NAME.
func checkPath(field, p, kind, scope, collection string) error {
	f := strings.Split(p, "/")
	if len(f) != 4 || f[0] != kind || f[1] != scope || f[2] != collection || !nameRE.MatchString(f[3]) {
		return fmt.Errorf("%s %q is not %s/%s/%s/NAME", field, p, kind, scope, collection)
	}
	return nil
}

// checkID reports whether s is a canonical nonzero decimal uint64.
func checkID(field, s string) error {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n == 0 || strconv.FormatUint(n, 10) != s {
		return fmt.Errorf("%s %q is not a nonzero decimal id", field, s)
	}
	return nil
}

// ed25519Wire is the length of a complete ssh-ed25519 public key blob:
// string "ssh-ed25519", then string of the 32 public key bytes
// (RFC 8709, section 4).
const ed25519Wire = 4 + len("ssh-ed25519") + 4 + 32

// checkKey reports whether k is one OpenSSH ssh-ed25519 public key line:
// the type, the base64 of exactly the complete public key wire and an
// optional plain comment.
func checkKey(k string) error {
	if len(k) == 0 || len(k) > maxKeyBytes || strings.ContainsAny(k, "\r\n\x00") {
		return errors.New("authorized_key must be one line of at most 4096 bytes")
	}
	f := strings.Fields(k)
	if len(f) < 2 || len(f) > 3 || strings.Join(f, " ") != k {
		return errors.New("authorized_key must be TYPE BASE64 [COMMENT] with single spaces")
	}
	if f[0] != "ssh-ed25519" {
		return fmt.Errorf("authorized_key type %q is not supported; use ssh-ed25519", f[0])
	}
	blob, err := base64.StdEncoding.Strict().DecodeString(f[1])
	if err != nil {
		return errors.New("authorized_key blob is not base64")
	}
	if len(blob) != ed25519Wire {
		return errors.New("authorized_key blob is not one complete ssh-ed25519 public key")
	}
	name, rest := sshString(blob)
	key, rest := sshString(rest)
	if name != "ssh-ed25519" || len(key) != 32 || rest == nil || len(rest) != 0 {
		return errors.New("authorized_key blob is not one complete ssh-ed25519 public key")
	}
	if len(f) == 3 && strings.ContainsAny(f[2], ":\"") {
		return errors.New("authorized_key comment must not contain ':' or '\"'")
	}
	return nil
}

// sshString splits the SSH wire string at the start of b from the rest of
// b. rest is nil if b does not start with a complete string.
func sshString(b []byte) (s string, rest []byte) {
	if len(b) < 4 {
		return "", nil
	}
	n := uint64(b[0])<<24 | uint64(b[1])<<16 | uint64(b[2])<<8 | uint64(b[3])
	if n > uint64(len(b)-4) {
		return "", nil
	}
	return string(b[4 : 4+n]), b[4+n:]
}

// hash returns the hex SHA-256 of c's canonical JSON form.
func (c *Config) hash() string {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err) // Config has only strings and an integer
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// readBounded reads all of r, failing if it holds more than max bytes.
func readBounded(r io.Reader, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("input exceeds %d bytes", max)
	}
	return b, nil
}

// decodeStrict decodes the single JSON value in data into v. It refuses
// duplicate object keys at any depth, trailing data and, if exact is set,
// unknown fields. Keys are compared after JSON unescaping and with the
// case folding encoding/json uses to match fields, so an escaped or
// case-varied alias of a key is a duplicate.
func decodeStrict(data []byte, v any, exact bool) error {
	if err := checkDuplicates(json.NewDecoder(bytes.NewReader(data))); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	if exact {
		dec.DisallowUnknownFields()
	}
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errTrailer
	}
	return nil
}

// checkDuplicates walks one JSON value and reports a duplicate object key.
func checkDuplicates(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}
	d, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch d {
	case '{':
		seen := map[string]bool{}
		for dec.More() {
			k, err := dec.Token()
			if err != nil {
				return err
			}
			key := foldKey(k.(string))
			if seen[key] {
				return fmt.Errorf("duplicate key %q", k)
			}
			seen[key] = true
			if err := checkDuplicates(dec); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := checkDuplicates(dec); err != nil {
				return err
			}
		}
	}
	_, err = dec.Token() // closing delimiter
	return err
}

// foldKey returns the case-folded form of a decoded object key. Two keys
// that encoding/json would match to the same field fold equal; the
// round trip maps the Kelvin sign and long s to k and s as its matching
// does.
func foldKey(k string) string {
	return strings.ToLower(strings.ToUpper(k))
}
