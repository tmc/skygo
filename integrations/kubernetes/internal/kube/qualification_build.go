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
	"runtime"
)

const qualificationBuildVersion = "skygo-kubernetes-build-receipt-v1"

// maxQualificationBuild bounds the build receipt file.
const maxQualificationBuild = 4 << 10

// maxQualificationExecutable bounds the server executable hashed at startup.
const maxQualificationExecutable = 1 << 30

// A qualificationBuild is the operator's build receipt. It binds one server
// executable digest to the helper image, helper executable digest and helper
// platform that build was produced with. It is build input written by the
// operator or release process, not a runtime attestation: the consumer checks
// only that the executable it actually runs has the named digest.
//
// The file holds exactly the canonical JSON encoding, with fields in this
// order and no surrounding whitespace.
type qualificationBuild struct {
	Version              string `json:"version"`
	ExecutableSHA256     string `json:"executable_sha256"`
	HelperImage          string `json:"helper_image"`
	HelperSHA256         string `json:"helper_sha256"`
	HelperPlatformDigest string `json:"helper_platform_digest"`
	HelperOS             string `json:"helper_os"`
	HelperArchitecture   string `json:"helper_architecture"`
}

var errQualificationBuild = errors.New("invalid kubernetes build receipt")

func validQualificationBuild(b *qualificationBuild) bool {
	return b.Version == qualificationBuildVersion && configHex(b.ExecutableSHA256) && configImage(b.HelperImage) &&
		configHex(b.HelperSHA256) && configHex(b.HelperPlatformDigest) &&
		qualificationText(b.HelperOS, 32) && qualificationText(b.HelperArchitecture, 32)
}

func decodeQualificationBuild(raw []byte) (*qualificationBuild, error) {
	if len(raw) == 0 || len(raw) > maxQualificationBuild {
		return nil, errQualificationBuild
	}
	var b qualificationBuild
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&b); err != nil || !validQualificationBuild(&b) {
		return nil, errQualificationBuild
	}
	// As for the record, exact reencoding rejects duplicate keys, trailing
	// values and alternate spellings of the same receipt.
	canonical, err := json.Marshal(&b)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, errQualificationBuild
	}
	return &b, nil
}

// loadQualificationBuild returns the actual implementation identity of this
// process: the digest of its executable file, its platform, and the build
// receipt's digest and helper facts. The receipt must name that executable.
// Nothing is taken from a qualification record.
func loadQualificationBuild(name string) (qualificationImplementation, error) {
	f, err := openQualificationFile(name)
	if err != nil {
		return qualificationImplementation{}, fmt.Errorf("open kubernetes build receipt: %w", err)
	}
	raw, err := readQualificationFile(f, maxQualificationBuild)
	if err != nil {
		return qualificationImplementation{}, err
	}
	b, err := decodeQualificationBuild(raw)
	if err != nil {
		return qualificationImplementation{}, err
	}
	exe, err := executableSHA256()
	if err != nil {
		return qualificationImplementation{}, err
	}
	if b.ExecutableSHA256 != exe {
		return qualificationImplementation{}, errors.New("kubernetes build receipt names a different executable")
	}
	sum := sha256.Sum256(raw)
	return qualificationImplementation{
		ExecutableSHA256: exe, BuildReceiptSHA256: hex.EncodeToString(sum[:]),
		HelperImage: b.HelperImage, HelperSHA256: b.HelperSHA256, HelperPlatformDigest: b.HelperPlatformDigest,
		HelperOS: b.HelperOS, HelperArchitecture: b.HelperArchitecture,
		OS: runtime.GOOS, Architecture: runtime.GOARCH,
	}, nil
}

// readQualificationFile consumes the original private file f, including its
// single Close, and returns at most max bytes read while its identity held.
// A failed Close returns the retained qualificationFileError.
func readQualificationFile(f *os.File, max int64) (b []byte, err error) {
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			b = nil
			err = &qualificationFileError{file: f, cause: errors.Join(err, closeErr)}
		}
	}()
	before, err := qualificationFileIdentity(f)
	if err != nil {
		return nil, err
	}
	b, err = io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, fmt.Errorf("read kubernetes qualification file: %w", err)
	}
	after, err := qualificationFileIdentity(f)
	if err != nil || before != after {
		return nil, errors.Join(errors.New("kubernetes qualification file changed"), err)
	}
	if int64(len(b)) > max {
		return nil, errors.New("kubernetes qualification file too large")
	}
	return b, nil
}

// executableSHA256 hashes the file os.Executable names, read through one
// original handle whose identity must not change while it is read. It is the
// file at that path during startup, not an attestation of the loaded image.
func executableSHA256() (sum string, err error) {
	name, err := os.Executable()
	if err != nil {
		return "", errors.New("locate server executable")
	}
	f, err := os.Open(name)
	if err != nil {
		return "", errors.New("open server executable")
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			sum = ""
			err = &qualificationFileError{file: f, cause: errors.Join(err, closeErr)}
		}
	}()
	before, err := f.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Size() > maxQualificationExecutable {
		return "", errors.New("unusable server executable")
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, maxQualificationExecutable+1))
	if err != nil {
		return "", errors.New("read server executable")
	}
	after, err := f.Stat()
	if err != nil || !os.SameFile(before, after) || n != before.Size() || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		return "", errors.New("server executable changed")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
