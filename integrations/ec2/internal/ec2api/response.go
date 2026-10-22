package ec2api

import (
	"bytes"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
)

// Bounds on decoded responses.
const (
	maxResponseBytes  = 1 << 20
	maxXMLDepth       = 32
	maxXMLElements    = 1 << 16
	maxReservations   = 1000
	maxInstances      = 1000 // per reservation
	maxInstanceTags   = 50
	maxInstanceGroups = 16
	maxConsoleBytes   = 1 << 17
	maxNextToken      = 4096
)

const (
	ec2Namespace = "http://ec2.amazonaws.com/doc/" + ec2Version + "/"
	stsNamespace = "https://sts.amazonaws.com/doc/" + stsVersion + "/"
)

var (
	reservationIDRE = regexp.MustCompile(`^r-([0-9a-f]{8}|[0-9a-f]{17})$`)
	vpcRE           = regexp.MustCompile(`^vpc-([0-9a-f]{8}|[0-9a-f]{17})$`)
	zoneRE          = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-[0-9]{1,2}[a-z]$`)
	codeRE          = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9.]{0,127}$`)
	requestIDRE     = regexp.MustCompile(`^[A-Za-z0-9-]{1,128}$`)
	ipRE            = regexp.MustCompile(`^[0-9a-f.:]{0,45}$`)
)

var instanceStates = []string{"pending", "running", "shutting-down", "terminated", "stopping", "stopped"}

// An Instance is one observed EC2 instance.
type Instance struct {
	ID               string
	State            string // pending, running, shutting-down, terminated, stopping or stopped
	ImageID          string
	InstanceType     string
	SubnetID         string
	VPCID            string
	AvailabilityZone string
	KeyName          string
	ClientToken      string
	SecurityGroupIDs []string // sorted
	Tags             []Tag    // sorted by key
	PublicIP         string
	PrivateIP        string
}

// A Reservation is the account and instances of one launch.
type Reservation struct {
	ID        string
	OwnerID   string // account that owns the instances
	Instances []Instance
}

// A StateChange is the state transition a lifecycle mutation reported.
type StateChange struct {
	InstanceID string
	Previous   string
	Current    string
}

type xmlState struct {
	Name string `xml:"name"`
}

type xmlTag struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

type xmlInstance struct {
	ID           string   `xml:"instanceId"`
	State        xmlState `xml:"instanceState"`
	ImageID      string   `xml:"imageId"`
	InstanceType string   `xml:"instanceType"`
	SubnetID     string   `xml:"subnetId"`
	VPCID        string   `xml:"vpcId"`
	Zone         string   `xml:"placement>availabilityZone"`
	KeyName      string   `xml:"keyName"`
	ClientToken  string   `xml:"clientToken"`
	Groups       []struct {
		ID string `xml:"groupId"`
	} `xml:"groupSet>item"`
	Tags      []xmlTag `xml:"tagSet>item"`
	PublicIP  string   `xml:"ipAddress"`
	PrivateIP string   `xml:"privateIpAddress"`
}

type xmlReservation struct {
	ID        string        `xml:"reservationId"`
	OwnerID   string        `xml:"ownerId"`
	Instances []xmlInstance `xml:"instancesSet>item"`
}

type runResponse struct {
	xmlReservation
}

type describeResponse struct {
	Reservations []xmlReservation `xml:"reservationSet>item"`
	NextToken    string           `xml:"nextToken"`
}

type stateChangeResponse struct {
	Items []struct {
		ID       string   `xml:"instanceId"`
		Current  xmlState `xml:"currentState"`
		Previous xmlState `xml:"previousState"`
	} `xml:"instancesSet>item"`
}

type creditResponse struct {
	Items []struct {
		ID         string `xml:"instanceId"`
		CPUCredits string `xml:"cpuCredits"`
	} `xml:"instanceCreditSpecificationSet>item"`
	NextToken string `xml:"nextToken"`
}

type consoleResponse struct {
	ID     string `xml:"instanceId"`
	Output string `xml:"output"`
}

type callerIdentityResponse struct {
	Account string `xml:"GetCallerIdentityResult>Account"`
}

type xmlError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// errorResponse covers both the EC2 <Response><Errors><Error> and the
// STS <ErrorResponse><Error> shapes.
type errorResponse struct {
	Errors       []xmlError `xml:"Errors>Error"`
	Error        []xmlError `xml:"Error"`
	RequestID    string     `xml:"RequestID"`
	STSRequestID string     `xml:"RequestId"`
}

// decodeXML decodes data, which must be one bounded XML document with the
// given root element, into v.
func decodeXML(data []byte, root xml.Name, v any) error {
	if err := checkXML(data, func(n xml.Name) bool { return n == root }); err != nil {
		return err
	}
	return xml.Unmarshal(data, v)
}

// checkXML reports whether data is exactly one XML document whose root
// element satisfies root, within the depth and element bounds, in which no
// element has two children of the same name other than list items.
func checkXML(data []byte, root func(xml.Name) bool) error {
	if len(data) > maxResponseBytes {
		return fmt.Errorf("response exceeds %d bytes", maxResponseBytes)
	}
	d := xml.NewDecoder(bytes.NewReader(data))
	var stack []map[string]bool
	elements := 0
	done := false
	for {
		tok, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if done {
				return errors.New("response has content after its root element")
			}
			if elements++; elements > maxXMLElements {
				return fmt.Errorf("response has more than %d elements", maxXMLElements)
			}
			if len(stack) == 0 {
				if !root(t.Name) {
					return errors.New("response root is not the expected element")
				}
			} else if name := t.Name.Local; name != "item" && name != "Error" {
				parent := stack[len(stack)-1]
				if parent[name] {
					return errors.New("response repeats a non-list element")
				}
				parent[name] = true
			}
			if len(stack) == maxXMLDepth {
				return fmt.Errorf("response nests deeper than %d", maxXMLDepth)
			}
			stack = append(stack, map[string]bool{})
		case xml.EndElement:
			stack = stack[:len(stack)-1]
			done = len(stack) == 0
		case xml.CharData:
			if len(stack) == 0 && len(bytes.TrimSpace(t)) > 0 {
				return errors.New("response has text outside its root element")
			}
		case xml.Directive:
			return errors.New("response has a directive")
		}
	}
	if !done {
		return errors.New("response has no root element")
	}
	return nil
}

func (x *xmlReservation) reservation() (*Reservation, error) {
	if !reservationIDRE.MatchString(x.ID) {
		return nil, errors.New("reservation ID is invalid")
	}
	if !accountRE.MatchString(x.OwnerID) {
		return nil, errors.New("reservation owner is not an account ID")
	}
	if len(x.Instances) > maxInstances {
		return nil, fmt.Errorf("reservation has more than %d instances", maxInstances)
	}
	r := &Reservation{ID: x.ID, OwnerID: x.OwnerID}
	for i := range x.Instances {
		inst, err := x.Instances[i].instance()
		if err != nil {
			return nil, err
		}
		r.Instances = append(r.Instances, *inst)
	}
	return r, nil
}

func (x *xmlInstance) instance() (*Instance, error) {
	switch {
	case !instanceIDRE.MatchString(x.ID):
		return nil, errors.New("instance ID is invalid")
	case !slices.Contains(instanceStates, x.State.Name):
		return nil, fmt.Errorf("instance %s state is invalid", x.ID)
	case x.ImageID != "" && !imageRE.MatchString(x.ImageID):
		return nil, fmt.Errorf("instance %s image ID is invalid", x.ID)
	case x.InstanceType != "" && !instanceTypeRE.MatchString(x.InstanceType):
		return nil, fmt.Errorf("instance %s type is invalid", x.ID)
	case x.SubnetID != "" && !subnetRE.MatchString(x.SubnetID):
		return nil, fmt.Errorf("instance %s subnet ID is invalid", x.ID)
	case x.VPCID != "" && !vpcRE.MatchString(x.VPCID):
		return nil, fmt.Errorf("instance %s VPC ID is invalid", x.ID)
	case x.Zone != "" && !zoneRE.MatchString(x.Zone):
		return nil, fmt.Errorf("instance %s availability zone is invalid", x.ID)
	case x.KeyName != "" && !keyNameRE.MatchString(x.KeyName):
		return nil, fmt.Errorf("instance %s key name is invalid", x.ID)
	case len(x.ClientToken) > maxTokenLen:
		return nil, fmt.Errorf("instance %s client token is too long", x.ID)
	case !ipRE.MatchString(x.PublicIP) || !ipRE.MatchString(x.PrivateIP):
		return nil, fmt.Errorf("instance %s address is invalid", x.ID)
	case len(x.Groups) > maxInstanceGroups:
		return nil, fmt.Errorf("instance %s has more than %d security groups", x.ID, maxInstanceGroups)
	case len(x.Tags) > maxInstanceTags:
		return nil, fmt.Errorf("instance %s has more than %d tags", x.ID, maxInstanceTags)
	}
	inst := &Instance{
		ID:               x.ID,
		State:            x.State.Name,
		ImageID:          x.ImageID,
		InstanceType:     x.InstanceType,
		SubnetID:         x.SubnetID,
		VPCID:            x.VPCID,
		AvailabilityZone: x.Zone,
		KeyName:          x.KeyName,
		ClientToken:      x.ClientToken,
		PublicIP:         x.PublicIP,
		PrivateIP:        x.PrivateIP,
	}
	for _, g := range x.Groups {
		if !groupRE.MatchString(g.ID) {
			return nil, fmt.Errorf("instance %s security group is invalid", x.ID)
		}
		inst.SecurityGroupIDs = append(inst.SecurityGroupIDs, g.ID)
	}
	slices.Sort(inst.SecurityGroupIDs)
	for _, t := range x.Tags {
		if t.Key == "" || len(t.Key) > 128 || len(t.Value) > 256 {
			return nil, fmt.Errorf("instance %s has an invalid tag", x.ID)
		}
		inst.Tags = append(inst.Tags, Tag{t.Key, t.Value})
	}
	slices.SortFunc(inst.Tags, func(a, b Tag) int { return strings.Compare(a.Key, b.Key) })
	for i := 1; i < len(inst.Tags); i++ {
		if inst.Tags[i].Key == inst.Tags[i-1].Key {
			return nil, fmt.Errorf("instance %s repeats tag %q", x.ID, inst.Tags[i].Key)
		}
	}
	return inst, nil
}

func (x *describeResponse) reservations() ([]Reservation, error) {
	if len(x.Reservations) > maxReservations {
		return nil, fmt.Errorf("page has more than %d reservations", maxReservations)
	}
	if len(x.NextToken) > maxNextToken {
		return nil, fmt.Errorf("next token exceeds %d bytes", maxNextToken)
	}
	var rs []Reservation
	for i := range x.Reservations {
		r, err := x.Reservations[i].reservation()
		if err != nil {
			return nil, err
		}
		rs = append(rs, *r)
	}
	return rs, nil
}

// stateChange returns the single transition of instance id.
func (x *stateChangeResponse) stateChange(id string) (*StateChange, error) {
	if len(x.Items) != 1 || x.Items[0].ID != id {
		return nil, fmt.Errorf("response does not report exactly instance %s", id)
	}
	it := x.Items[0]
	if !slices.Contains(instanceStates, it.Current.Name) || !slices.Contains(instanceStates, it.Previous.Name) {
		return nil, fmt.Errorf("response reports an invalid state for %s", id)
	}
	return &StateChange{InstanceID: id, Previous: it.Previous.Name, Current: it.Current.Name}, nil
}

// credits returns the CPU credit option reported for exactly instance id.
func (x *creditResponse) credits(id string) (string, error) {
	switch {
	case x.NextToken != "":
		return "", errors.New("credit specification response has an unexpected next token")
	case len(x.Items) != 1 || x.Items[0].ID != id:
		return "", fmt.Errorf("credit specification response does not report exactly instance %s", id)
	case x.Items[0].CPUCredits != "standard" && x.Items[0].CPUCredits != "unlimited":
		return "", fmt.Errorf("instance %s CPU credit option is invalid", id)
	}
	return x.Items[0].CPUCredits, nil
}

// output returns the decoded console output of instance id.
func (x *consoleResponse) output(id string) ([]byte, error) {
	if x.ID != id {
		return nil, fmt.Errorf("response does not report instance %s", id)
	}
	s := strings.Join(strings.Fields(x.Output), "")
	if base64.StdEncoding.DecodedLen(len(s)) > maxConsoleBytes+2 {
		return nil, fmt.Errorf("console output exceeds %d bytes", maxConsoleBytes)
	}
	out, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, errors.New("console output is not base64")
	}
	if len(out) > maxConsoleBytes {
		return nil, fmt.Errorf("console output exceeds %d bytes", maxConsoleBytes)
	}
	return out, nil
}

// decodeError extracts the provider error code, message and request ID
// from an error response whose root element is envelope: "Response" for
// EC2, whose documented envelope is Response>Errors>Error>Code with a
// RequestID, or "ErrorResponse" for STS Query, ErrorResponse>Error>Code with
// a RequestId. The document must pass the same structural bounds as a
// successful response, so a repeated Code is refused, not resolved. The
// root's namespace is not checked: no offline primary source pins the STS
// error namespace, and the code is trusted only for classification.
func decodeError(data []byte, envelope string) (code, message, requestID string, err error) {
	if err := checkXML(data, func(n xml.Name) bool { return n.Local == envelope }); err != nil {
		return "", "", "", err
	}
	var x errorResponse
	if err := xml.Unmarshal(data, &x); err != nil {
		return "", "", "", err
	}
	var errs []xmlError
	switch envelope {
	case "Response":
		if len(x.Error) != 0 || x.STSRequestID != "" {
			return "", "", "", errors.New("EC2 error response has STS elements")
		}
		errs, requestID = x.Errors, x.RequestID
	case "ErrorResponse":
		if len(x.Errors) != 0 || x.RequestID != "" || len(x.Error) > 1 {
			return "", "", "", errors.New("STS error response is not one Error")
		}
		errs, requestID = x.Error, x.STSRequestID
	default:
		return "", "", "", fmt.Errorf("unknown error envelope %q", envelope)
	}
	if len(errs) == 0 || !codeRE.MatchString(errs[0].Code) {
		return "", "", "", errors.New("error response has no valid code")
	}
	if requestID != "" && !requestIDRE.MatchString(requestID) {
		return "", "", "", errors.New("error response request ID is invalid")
	}
	return errs[0].Code, errs[0].Message, requestID, nil
}
