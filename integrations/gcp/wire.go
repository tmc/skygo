package gce

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// The narrow JSON forms of the Compute Engine v1 resources this package
// reads and writes. Response fields not listed are ignored; listed
// identity and lifecycle fields are validated by the callers.

type projectWire struct {
	Name                   string       `json:"name"`
	ID                     string       `json:"id"`
	CommonInstanceMetadata metadataWire `json:"commonInstanceMetadata"`
}

type metadataWire struct {
	Items []metadataItem `json:"items"`
}

type metadataItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type imageWire struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Status       string `json:"status"`
	SelfLink     string `json:"selfLink"`
	Architecture string `json:"architecture"`
}

// machineTypeWire is the projection of compute.machineTypes.get. Zone is
// the bare zone name, such as us-central1-a, not a link.
type machineTypeWire struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Zone         string `json:"zone"`
	SelfLink     string `json:"selfLink"`
	Architecture string `json:"architecture"`
}

// archARM64 is the only admitted value of Image.architecture and
// MachineType.architecture. ARCHITECTURE_UNSPECIFIED, X86_64, an empty
// value and any unknown value are refused.
const archARM64 = "ARM64"

type diskWire struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Zone          string            `json:"zone"`
	Type          string            `json:"type"`
	SizeGb        string            `json:"sizeGb"`
	SourceImage   string            `json:"sourceImage"`
	SourceImageID string            `json:"sourceImageId"`
	Status        string            `json:"status"`
	Users         []string          `json:"users"`
	Labels        map[string]string `json:"labels"`
}

type instanceWire struct {
	ID                string             `json:"id"`
	Name              string             `json:"name"`
	Zone              string             `json:"zone"`
	MachineType       string             `json:"machineType"`
	Status            string             `json:"status"`
	Disks             []attachedDiskWire `json:"disks"`
	NetworkInterfaces []networkWire      `json:"networkInterfaces"`
	Metadata          metadataWire       `json:"metadata"`
	ServiceAccounts   []json.RawMessage  `json:"serviceAccounts"`
	Labels            map[string]string  `json:"labels"`
}

type attachedDiskWire struct {
	Boot       bool   `json:"boot"`
	AutoDelete bool   `json:"autoDelete"`
	Source     string `json:"source"`
	Mode       string `json:"mode"`
	Type       string `json:"type"`
}

type networkWire struct {
	Subnetwork    string            `json:"subnetwork"`
	AccessConfigs []json.RawMessage `json:"accessConfigs,omitempty"`
}

type operationWire struct {
	ID                  string          `json:"id"`
	Name                string          `json:"name"`
	Zone                string          `json:"zone"`
	OperationType       string          `json:"operationType"`
	TargetLink          string          `json:"targetLink"`
	TargetID            string          `json:"targetId"`
	Status              string          `json:"status"`
	ClientOperationID   string          `json:"clientOperationId"`
	HTTPErrorStatusCode int             `json:"httpErrorStatusCode"`
	Error               *operationError `json:"error"`
}

type operationError struct {
	Errors []struct {
		Code string `json:"code"`
	} `json:"errors"`
}

type operationListWire struct {
	Items         []operationWire `json:"items"`
	NextPageToken string          `json:"nextPageToken"`
}

// diskInsertBody and instanceInsertBody are the exact mutation bodies.

type diskInsertBody struct {
	Name        string            `json:"name"`
	SizeGb      string            `json:"sizeGb"`
	Type        string            `json:"type"`
	SourceImage string            `json:"sourceImage"`
	Labels      map[string]string `json:"labels"`
	Description string            `json:"description"`
}

type instanceInsertBody struct {
	Name              string             `json:"name"`
	MachineType       string             `json:"machineType"`
	Disks             []attachedDiskWire `json:"disks"`
	NetworkInterfaces []networkWire      `json:"networkInterfaces"`
	Metadata          metadataWire       `json:"metadata"`
	ServiceAccounts   []json.RawMessage  `json:"serviceAccounts"`
	Labels            map[string]string  `json:"labels"`
	Description       string             `json:"description"`
}

const ownerLabel = "skygo-gce-owner"

// resourcePath returns the trailing projects/... path of a Compute
// resource link, accepting the documented API hosts, or "" if link is
// not such a link.
func resourcePath(link string) string {
	for _, p := range []string{
		"https://www.googleapis.com/compute/v1/",
		"https://compute.googleapis.com/compute/v1/",
	} {
		if rest, ok := strings.CutPrefix(link, p); ok && strings.HasPrefix(rest, "projects/") {
			return rest
		}
	}
	return ""
}

// sameID reports whether got is a canonical decimal id equal to want.
func sameID(got, want string) bool {
	n, err := strconv.ParseUint(got, 10, 64)
	return err == nil && n != 0 && strconv.FormatUint(n, 10) == got && got == want
}

// codes returns at most four error codes of an operation, in received
// order, never its server-written messages.
func (e *operationError) codes() string {
	if e == nil {
		return ""
	}
	var c []string
	for i, x := range e.Errors {
		if i == 4 {
			c = append(c, "...")
			break
		}
		code := x.Code
		if len(code) > 64 || strings.ContainsAny(code, " \n\t") {
			code = "INVALID_CODE"
		}
		c = append(c, code)
	}
	if len(c) == 0 {
		return "UNSPECIFIED"
	}
	return strings.Join(c, ",")
}

// metadataValue returns the value of key in m and whether it is present
// exactly once.
func (m metadataWire) value(key string) (string, int) {
	var v string
	n := 0
	for _, it := range m.Items {
		if it.Key == key {
			v = it.Value
			n++
		}
	}
	return v, n
}

func wireErrorf(what, format string, args ...any) error {
	return fmt.Errorf("%s: %s", what, fmt.Sprintf(format, args...))
}
