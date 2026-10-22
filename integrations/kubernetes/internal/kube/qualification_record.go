package kube

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
)

const qualificationRecordVersion = "skygo-kubernetes-data-qualification-v1"
const qualificationComposition = "disjoint-mounts-v1"
const maxQualificationRecord = 256 << 10

// A record is trusted operator configuration, not proof against its author.
// This codec checks shape and identity only. Native evidence and current
// deployment/profile matching are required before any Backend can use it.
type qualificationRecord struct {
	Version        string                      `json:"version"`
	Implementation qualificationImplementation `json:"implementation"`
	TargetSHA256   string                      `json:"target_sha256"`
	Deployment     qualificationDeployment     `json:"deployment"`
	Images         []qualificationImage        `json:"images"`
	Composition    string                      `json:"composition"`
	Evidence       qualificationEvidence       `json:"evidence"`
}

type qualificationImplementation struct {
	ExecutableSHA256     string `json:"executable_sha256"`
	BuildReceiptSHA256   string `json:"build_receipt_sha256"`
	HelperImage          string `json:"helper_image"`
	HelperSHA256         string `json:"helper_sha256"`
	HelperPlatformDigest string `json:"helper_platform_digest"`
	HelperOS             string `json:"helper_os"`
	HelperArchitecture   string `json:"helper_architecture"`
	OS                   string `json:"os"`
	Architecture         string `json:"architecture"`
}

type qualificationDeployment struct {
	APIServer         string                 `json:"api_server"`
	CASHA256          string                 `json:"ca_sha256"`
	Namespace         string                 `json:"namespace"`
	NamespaceUID      string                 `json:"namespace_uid"`
	KubernetesVersion string                 `json:"kubernetes_version"`
	Pool              qualificationPool      `json:"pool"`
	PolicyRevision    string                 `json:"policy_revision"`
	Nodes             []qualificationNode    `json:"nodes"`
	Storage           []qualificationStorage `json:"storage"`
}

type qualificationPool struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type qualificationNode struct {
	Name         string `json:"name"`
	UID          string `json:"uid"`
	Kubelet      string `json:"kubelet"`
	Runtime      string `json:"runtime"`
	Kernel       string `json:"kernel"`
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
}

type qualificationStorage struct {
	Reference      string `json:"reference"`
	PVCUID         string `json:"pvc_uid"`
	StorageClass   string `json:"storage_class"`
	Driver         string `json:"driver"`
	PolicyRevision string `json:"policy_revision"`
}

type qualificationImage struct {
	Image            string               `json:"image"`
	PlatformDigest   string               `json:"platform_digest"`
	OS               string               `json:"os"`
	Architecture     string               `json:"architecture"`
	Profiles         []string             `json:"profiles"`
	FixedPathsSHA256 string               `json:"fixed_paths_sha256"`
	DefaultDirectory bool                 `json:"default_directory"`
	Directories      []string             `json:"directories"`
	Mounts           []qualificationMount `json:"mounts"`
}

type qualificationMount struct {
	Reference   string `json:"reference"`
	Destination string `json:"destination"`
	ReadOnly    bool   `json:"read_only"`
}

type qualificationEvidence struct {
	RunID         string              `json:"run_id"`
	CompletedAt   time.Time           `json:"completed_at"`
	ReceiptSHA256 string              `json:"receipt_sha256"`
	Cases         []qualificationCase `json:"cases"`
}

type qualificationCase struct {
	Name          string `json:"name"`
	ReceiptSHA256 string `json:"receipt_sha256"`
}

var errQualificationRecord = errors.New("invalid kubernetes qualification record")

func encodeQualificationRecord(r *qualificationRecord) ([]byte, error) {
	if !validQualificationRecord(r) {
		return nil, errQualificationRecord
	}
	b, err := json.Marshal(r)
	if err != nil || len(b) > maxQualificationRecord {
		return nil, errQualificationRecord
	}
	return b, nil
}

func decodeQualificationRecord(b []byte) (*qualificationRecord, error) {
	if len(b) == 0 || len(b) > maxQualificationRecord {
		return nil, errQualificationRecord
	}
	var r qualificationRecord
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return nil, errQualificationRecord
	}
	canonical, err := encodeQualificationRecord(&r)
	// Exact reencoding also rejects duplicate keys, trailing values and alternate
	// escaping/whitespace. It is not merely a hash of unchecked input bytes.
	if err != nil || !bytes.Equal(b, canonical) {
		return nil, errQualificationRecord
	}
	return &r, nil
}

func qualificationRecordID(r *qualificationRecord) (string, error) {
	b, err := encodeQualificationRecord(r)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func cloneQualificationRecord(r *qualificationRecord) *qualificationRecord {
	if r == nil {
		return nil
	}
	c := *r
	c.Deployment.Nodes = slices.Clone(r.Deployment.Nodes)
	c.Deployment.Storage = slices.Clone(r.Deployment.Storage)
	c.Images = slices.Clone(r.Images)
	for i := range c.Images {
		c.Images[i].Profiles = slices.Clone(r.Images[i].Profiles)
		c.Images[i].Directories = slices.Clone(r.Images[i].Directories)
		c.Images[i].Mounts = slices.Clone(r.Images[i].Mounts)
	}
	c.Evidence.Cases = slices.Clone(r.Evidence.Cases)
	return &c
}

func qualificationText(s string, max int) bool {
	if len(s) == 0 || len(s) > max {
		return false
	}
	for _, c := range []byte(s) {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}

func qualificationPath(s string) bool {
	return len(s) <= 240 && path.IsAbs(s) && path.Clean(s) == s && qualificationText(s, 240)
}

func validQualificationRecord(r *qualificationRecord) bool {
	if r == nil || r.Version != qualificationRecordVersion || r.Composition != qualificationComposition || !configHex(r.TargetSHA256) {
		return false
	}
	i := r.Implementation
	if !configHex(i.ExecutableSHA256) || !configHex(i.BuildReceiptSHA256) || !configImage(i.HelperImage) || !configHex(i.HelperSHA256) || !configHex(i.HelperPlatformDigest) || !qualificationText(i.HelperOS, 32) || !qualificationText(i.HelperArchitecture, 32) || !qualificationText(i.OS, 32) || !qualificationText(i.Architecture, 32) {
		return false
	}
	d := r.Deployment
	server, err := canonicalServer(d.APIServer)
	if err != nil || server != d.APIServer || !strings.HasPrefix(server, "https://") || !configHex(d.CASHA256) || len(validation.IsDNS1123Label(d.Namespace)) != 0 || !qualificationText(d.NamespaceUID, 128) || !qualificationText(d.KubernetesVersion, 128) || !qualificationText(d.PolicyRevision, 128) {
		return false
	}
	if len(validation.IsQualifiedName(d.Pool.Key)) != 0 || len(d.Pool.Key) > 256 || d.Pool.Value == "" || len(validation.IsValidLabelValue(d.Pool.Value)) != 0 || len(d.Nodes) == 0 || len(d.Nodes) > 32 || len(d.Storage) > 16 {
		return false
	}
	for n, node := range d.Nodes {
		if len(validation.IsDNS1123Subdomain(node.Name)) != 0 || !qualificationText(node.UID, 128) || !qualificationText(node.Kubelet, 128) || !qualificationText(node.Runtime, 128) || !qualificationText(node.Kernel, 256) || !qualificationText(node.OS, 32) || !qualificationText(node.Architecture, 32) || n > 0 && d.Nodes[n-1].Name >= node.Name {
			return false
		}
		for _, previous := range d.Nodes[:n] {
			if previous.UID == node.UID {
				return false
			}
		}
	}
	for n, s := range d.Storage {
		if len(validation.IsDNS1123Label(s.Reference)) != 0 || !qualificationText(s.PVCUID, 128) || !qualificationText(s.StorageClass, 253) || !qualificationText(s.Driver, 253) || !qualificationText(s.PolicyRevision, 128) || n > 0 && d.Storage[n-1].Reference >= s.Reference {
			return false
		}
	}
	if len(r.Images) == 0 || len(r.Images) > 32 {
		return false
	}
	profiles := []string{"mount-only", "workdir", "workdir-output", "workdir-setup", "workdir-setup-output"}
	for n, image := range r.Images {
		if !configImage(image.Image) || !configHex(image.PlatformDigest) || !qualificationText(image.OS, 32) || !qualificationText(image.Architecture, 32) || !configHex(image.FixedPathsSHA256) || n > 0 && r.Images[n-1].Image >= image.Image || len(image.Profiles) == 0 || len(image.Profiles) > len(profiles) || len(image.Directories) > 256 || len(image.Mounts) > 512 {
			return false
		}
		for j, profile := range image.Profiles {
			if !slices.Contains(profiles, profile) || j > 0 && image.Profiles[j-1] >= profile {
				return false
			}
		}
		for j, directory := range image.Directories {
			if !qualificationPath(directory) || j > 0 && image.Directories[j-1] >= directory {
				return false
			}
		}
		for j, m := range image.Mounts {
			if len(validation.IsDNS1123Label(m.Reference)) != 0 || !qualificationPath(m.Destination) || !strings.HasPrefix(m.Destination, "/mnt/skygo/") {
				return false
			}
			if j > 0 && compareQualificationMount(image.Mounts[j-1], m) >= 0 {
				return false
			}
		}
	}
	e := r.Evidence
	if !qualificationRunID(e.RunID) || e.CompletedAt.IsZero() || e.CompletedAt.Year() < 1 || e.CompletedAt.Year() > 9999 || e.CompletedAt.Location() != time.UTC || !configHex(e.ReceiptSHA256) || len(e.Cases) == 0 || len(e.Cases) > 256 {
		return false
	}
	for n, c := range e.Cases {
		if !qualificationText(c.Name, 128) || !configHex(c.ReceiptSHA256) || n > 0 && e.Cases[n-1].Name >= c.Name {
			return false
		}
	}
	return true
}

func qualificationRunID(s string) bool {
	if len(s) != 32 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil && strings.ToLower(s) == s
}

func compareQualificationMount(a, b qualificationMount) int {
	if n := strings.Compare(a.Reference, b.Reference); n != 0 {
		return n
	}
	if n := strings.Compare(a.Destination, b.Destination); n != 0 {
		return n
	}
	if a.ReadOnly == b.ReadOnly {
		return 0
	}
	if a.ReadOnly {
		return 1
	}
	return -1
}

type qualificationIdentity struct {
	device, inode uint64
	size          int64
	modified      time.Time
	mode          os.FileMode
	uid           uint32
}

// A Close error does not prove the original descriptor is still open, nor does
// retaining File recover hidden OS ownership. Retain the exact original/error
// frame so the caller cannot silently turn that uncertainty into success.
type qualificationFileError struct {
	file  *os.File
	cause error
}

func (e *qualificationFileError) Error() string {
	return "kubernetes qualification file close remains uncertain"
}
func (e *qualificationFileError) Unwrap() error { return e.cause }

func loadQualificationRecord(name string) (*qualificationRecord, error) {
	f, err := openQualificationFile(name)
	if err != nil {
		return nil, fmt.Errorf("open kubernetes qualification file: %w", err)
	}
	return readQualificationRecord(f)
}

// readQualificationRecord consumes this original file, including Close once.
// Parent directories and the operator's filesystem are trusted configuration.
func readQualificationRecord(f *os.File) (r *qualificationRecord, err error) {
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			r = nil
			err = &qualificationFileError{file: f, cause: errors.Join(err, closeErr)}
		}
	}()
	before, err := qualificationFileIdentity(f)
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(io.LimitReader(f, maxQualificationRecord+1))
	if err != nil {
		return nil, fmt.Errorf("read kubernetes qualification file: %w", err)
	}
	after, err := qualificationFileIdentity(f)
	if err != nil || before != after {
		return nil, errors.Join(errors.New("kubernetes qualification file changed"), err)
	}
	return decodeQualificationRecord(b)
}
