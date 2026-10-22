package ec2api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func profile() *Profile {
	return &Profile{
		Account:          "123456789012",
		Region:           "us-west-2",
		SubnetID:         "subnet-0123456789abcdef0",
		SecurityGroupIDs: []string{"sg-0bbbbbbbbbbbbbbbb", "sg-0aaaaaaaaaaaaaaaa"},
		ImageID:          "ami-0123456789abcdef0",
		InstanceType:     "t3.micro",
		RootDevice:       "/dev/xvda",
		RootVolumeGiB:    16,
		RootVolumeType:   "gp3",
		EncryptRoot:      true,
	}
}

var identity = Identity{Owner: "install-7f3a", Environment: "env-0123456789abcdef", Generation: 1}

func Example() {
	req, err := NewCreateRequest(profile(), identity, nil)
	if err != nil {
		log.Fatal(err)
	}
	// Persist req.Body() before sending; after a lost reply, reissue the
	// request parsed from those bytes.
	again, err := ParseCreateRequest(req.Body())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(again.ClientToken() == req.ClientToken())
	// Output: true
}

func ExampleIdentity_ClientToken() {
	fmt.Println(identity.ClientToken())
	// Output: skygo-55e39499ed37635201145febc23225f7968c94e1b96a477577cc0dedc0
}

func ExampleProfile_Hash() {
	fmt.Println(profile().Hash())
	// Output: b85c987d09a77a0bacd2bed09255a284b8c768fc6e454da9e67a17c8210f3e4d
}

func ExampleProfile_Validate() {
	p := profile()
	p.RootVolumeType = "st1"
	fmt.Println(p.Validate())
	// Output: profile root volume type "st1" is not gp2 or gp3
}

func ExampleParseCreateRequest() {
	_, err := ParseCreateRequest([]byte("Action=RunInstances&ClientToken=skygo-1&MinCount=1&MaxCount=5"))
	fmt.Println(err != nil)
	// Output: true
}

func ExampleVerifyOwner() {
	req, err := NewCreateRequest(profile(), identity, nil)
	if err != nil {
		log.Fatal(err)
	}
	// An instance with the right ID and token but foreign tags.
	r := Reservation{
		ID:      "r-0123456789abcdef0",
		OwnerID: "123456789012",
		Instances: []Instance{{
			ID:               "i-0123456789abcdef0",
			ClientToken:      req.ClientToken(),
			AvailabilityZone: "us-west-2a",
			Tags:             []Tag{{Key: "Name", Value: "someone-else"}},
		}},
	}
	_, err = VerifyOwner(profile(), req, []Reservation{r}, "i-0123456789abcdef0")
	fmt.Println(errors.Is(err, ErrNotOwned))
	// Output: true
}

func ExampleParseConsole() {
	out := "-----BEGIN SSH HOST KEY FINGERPRINTS-----\n" +
		"256 SHA256:b3ZlcmVkIGFuZCB0aGVuIGZvcmdvdHRlbiBmaWxlc3l root@host (ED25519)\n" +
		"-----END SSH HOST KEY FINGERPRINTS-----\n" +
		"-----BEGIN SKYGO AGENT IDENTITY-----\n" +
		"tls-sha256: 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08\n" +
		"request-domain: 0123456789abcdef0123456789abcdef:7\n" +
		"-----END SKYGO AGENT IDENTITY-----\n"
	c, err := ParseConsole([]byte(out))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(c.HostKeys[0].Type, c.RequestDomain)
	// Output: ED25519 0123456789abcdef0123456789abcdef:7
}

func ExampleNewClient() {
	// The caller resolves credentials; NewClient reads no files or
	// environment variables and sends nothing.
	creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{}, errors.New("no credentials configured")
	})
	_, err := NewClient(profile(), creds)
	fmt.Println(err)
	// Output: <nil>
}

func ExampleNewCreateRequest() {
	r, err := NewCreateRequest(profile(), identity, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(r.Identity() == identity)
	// Output: true
}

func ExampleCreateRequest_ClientToken() {
	r := exampleRequest()
	fmt.Println(r.ClientToken() == identity.ClientToken())
	// Output: true
}

func ExampleCreateRequest_Body() {
	r := exampleRequest()
	body := r.Body() // an independent copy, suitable for durable storage
	body[0] = '!'
	fmt.Println(bytes.Equal(body, r.Body()))
	// Output: false
}

func ExampleCreateRequest_Identity() {
	fmt.Println(exampleRequest().Identity().Generation)
	// Output: 1
}

func ExampleCreateRequest_ProfileHash() {
	fmt.Println(exampleRequest().ProfileHash() == profile().Hash())
	// Output: true
}

func ExampleIdentity_Validate() {
	id := identity
	id.Generation = 0
	fmt.Println(id.Validate())
	// Output: identity generation must be positive
}

func ExampleCreditMismatchError_Error() {
	err := &CreditMismatchError{InstanceID: "i-0123456789abcdef0", Want: "standard", Got: "unlimited"}
	fmt.Println(err.Error())
	// Output: ec2api: instance i-0123456789abcdef0 CPU credit option is unlimited, profile requires standard
}

func ExampleClass_String() {
	fmt.Println(ClassInstanceNotFound.String())
	// Output: instance not found
}

func ExampleError_Error() {
	err := &Error{Action: "RunInstances", Class: ClassThrottled, StatusCode: 503}
	fmt.Println(err.Error())
	// Output: ec2api: RunInstances: throttled status 503
}

func ExampleError_Unwrap() {
	// Locally constructed metadata need not contain an underlying cause.
	err := &Error{Action: "RunInstances", Class: ClassThrottled}
	fmt.Println(len(err.Unwrap()))
	// Output: 0
}

func ExampleError_Retryable() {
	err := &Error{Action: "RunInstances", Class: ClassThrottled, Sent: true}
	// Retryable does not authorize a replacement request or ClientToken.
	fmt.Println(err.Retryable(), err.Sent)
	// Output: true true
}

func ExampleClient_RunInstances() {
	r := exampleRequest()
	c := exampleClient(r, "pending") // in-memory HTTP fixture; never AWS
	reservation, err := c.RunInstances(context.Background(), r)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(reservation.Instances[0].ID)
	// Output: i-0123456789abcdef0
}

func ExampleClient_DescribeOwned() {
	r := exampleRequest()
	c := exampleClient(r, "running") // in-memory HTTP fixture; never AWS
	instance, err := c.DescribeOwned(context.Background(), r, "i-0123456789abcdef0")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(instance.State)
	// Output: running
}

func ExampleClient_FindOwned() {
	r := exampleRequest()
	c := exampleClient(r, "running") // in-memory HTTP fixture; never AWS
	instance, err := c.FindOwned(context.Background(), r)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(instance.ID)
	// Output: i-0123456789abcdef0
}

func ExampleClient_CheckCPUCredits() {
	r := exampleRequest()
	c := exampleClient(r, "running") // in-memory HTTP fixture; never AWS
	credits, err := c.CheckCPUCredits(context.Background(), r, "i-0123456789abcdef0")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(credits)
	// Output: standard
}

func ExampleClient_Stop() {
	r := exampleRequest()
	c := exampleClient(r, "running") // in-memory HTTP fixture; never AWS
	change, err := c.Stop(context.Background(), r, "i-0123456789abcdef0")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(change.Current)
	// Output: stopping
}

func ExampleClient_Start() {
	r := exampleRequest()
	c := exampleClient(r, "stopped") // in-memory HTTP fixture; never AWS
	change, err := c.Start(context.Background(), r, "i-0123456789abcdef0")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(change.Current)
	// Output: pending
}

func ExampleClient_Terminate() {
	r := exampleRequest()
	c := exampleClient(r, "running") // in-memory HTTP fixture; never AWS
	change, err := c.Terminate(context.Background(), r, "i-0123456789abcdef0")
	if err != nil {
		log.Fatal(err)
	}
	// A mutation reply does not prove all resources have been retired.
	fmt.Println(change.Current)
	// Output: shutting-down
}

func ExampleClient_ConsoleOutput() {
	r := exampleRequest()
	c := exampleClient(r, "running") // in-memory HTTP fixture; never AWS
	output, err := c.ConsoleOutput(context.Background(), r, "i-0123456789abcdef0")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(output))
	// Output: boot complete
}

func exampleRequest() *CreateRequest {
	r, err := NewCreateRequest(profile(), identity, nil)
	if err != nil {
		log.Fatal(err)
	}
	return r
}

// exampleClient is a private, synchronous test fixture. NewClient does not
// expose endpoint or transport injection. This fixture installs a local
// RoundTripper before any call, starts no goroutines and opens no sockets.
func exampleClient(r *CreateRequest, state string) *Client {
	creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "EXAMPLE", SecretAccessKey: "fixture-only"}, nil
	})
	c, err := newClient(profile(), creds, "https://ec2.invalid/", "https://sts.invalid/", nil)
	if err != nil {
		log.Fatal(err)
	}
	c.http.Transport = exampleTransport{request: r, state: state}
	return c
}

type exampleTransport struct {
	request *CreateRequest
	state   string
}

func (f exampleTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(io.LimitReader(req.Body, MaxCreateBody+1))
	closeErr := req.Body.Close()
	if err != nil || closeErr != nil {
		return nil, errors.Join(err, closeErr)
	}
	if len(body) > MaxCreateBody {
		return nil, errors.New("example request exceeds bound")
	}
	query, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, err
	}
	p := profile()
	inst := xmlInstance{
		ID: "i-0123456789abcdef0", State: xmlState{Name: f.state},
		ImageID: p.ImageID, InstanceType: p.InstanceType, SubnetID: p.SubnetID,
		VPCID: "vpc-0123456789abcdef0", Zone: "us-west-2a",
		ClientToken: f.request.ClientToken(),
	}
	for _, id := range p.SecurityGroupIDs {
		inst.Groups = append(inst.Groups, struct {
			ID string `xml:"groupId"`
		}{id})
	}
	for _, tag := range f.request.spec.tags {
		inst.Tags = append(inst.Tags, xmlTag{Key: tag.Key, Value: tag.Value})
	}
	reservation := xmlReservation{ID: "r-0123456789abcdef0", OwnerID: p.Account, Instances: []xmlInstance{inst}}
	action := query.Get("Action")
	namespace := ec2Namespace
	var out any
	switch action {
	case "GetCallerIdentity":
		namespace = stsNamespace
		out = callerIdentityResponse{Account: p.Account}
	case "RunInstances":
		out = runResponse{reservation}
	case "DescribeInstances":
		out = describeResponse{Reservations: []xmlReservation{reservation}}
	case "DescribeInstanceCreditSpecifications":
		out = struct {
			ID      string `xml:"instanceCreditSpecificationSet>item>instanceId"`
			Credits string `xml:"instanceCreditSpecificationSet>item>cpuCredits"`
		}{inst.ID, "standard"}
	case "StopInstances", "StartInstances", "TerminateInstances":
		current := map[string]string{"StopInstances": "stopping", "StartInstances": "pending", "TerminateInstances": "shutting-down"}[action]
		out = struct {
			ID       string `xml:"instancesSet>item>instanceId"`
			Previous string `xml:"instancesSet>item>previousState>name"`
			Current  string `xml:"instancesSet>item>currentState>name"`
		}{inst.ID, f.state, current}
	case "GetConsoleOutput":
		out = consoleResponse{ID: inst.ID, Output: base64.StdEncoding.EncodeToString([]byte("boot complete"))}
	default:
		return nil, fmt.Errorf("unexpected example action %q", action)
	}
	var response bytes.Buffer
	enc := xml.NewEncoder(&response)
	if err := enc.EncodeElement(out, xml.StartElement{Name: xml.Name{Space: namespace, Local: action + "Response"}}); err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(response.Bytes())), Request: req}, nil
}
