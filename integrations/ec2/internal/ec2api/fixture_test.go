package ec2api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// The fixture is a local TLS EC2 and STS endpoint pair. It verifies every
// request's SigV4 signature with its own implementation of the signing
// process and models only the EC2 behavior these tests depend on:
// ClientToken idempotency, tag and client-token filters, NextToken pages,
// lifecycle transitions and console output.

var fixtureCreds = aws.Credentials{
	AccessKeyID:     "AKIDFIXTURE0000",
	SecretAccessKey: "fixture/secret/key",
	SessionToken:    "fixture-session-token",
	Source:          "fixture",
}

func testProfile() *Profile {
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

func credentialsFunc(fn func() (id, secret, token string)) aws.CredentialsProvider {
	return aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		id, secret, token := fn()
		return aws.Credentials{AccessKeyID: id, SecretAccessKey: secret, SessionToken: token}, nil
	})
}

func testIdentity() Identity {
	return Identity{Owner: "install-7f3a", Environment: "env-0123456789abcdef", Generation: 1}
}

func testRequest(t *testing.T) *CreateRequest {
	t.Helper()
	req, err := NewCreateRequest(testProfile(), testIdentity(), []byte("#cloud-config\n"))
	if err != nil {
		t.Fatal(err)
	}
	return req
}

type fixtureRequest struct {
	service string
	action  string
	form    url.Values
	body    []byte
	key     string // access key ID that signed the request
}

type fixtureInstance struct {
	reservation string
	owner       string
	id          string
	state       string
	image       string
	itype       string
	subnet      string
	zone        string
	key         string
	token       string
	credits     string
	groups      []string
	tags        []Tag
}

type fixtureLaunch struct {
	body []byte
	inst *fixtureInstance
}

// A fixtureIdentity is an accepted credential and the account STS reports
// for it.
type fixtureIdentity struct {
	creds   aws.Credentials
	account string
}

type fixture struct {
	t      *testing.T
	region string
	creds  aws.Credentials

	ec2, sts *httptest.Server

	mu        sync.Mutex
	account   string                     // GetCallerIdentity account for creds
	others    map[string]fixtureIdentity // further accepted credentials, by access key ID
	owner     string                     // reservation owner of new instances
	conns     int
	requests  []fixtureRequest
	launches  map[string]*fixtureLaunch // by ClientToken
	instances []*fixtureInstance
	console   string
	pageSize  int

	// before, if set, may answer a verified request before the model
	// applies it. after, if set, may answer it after the model applied it.
	before func(action string, w http.ResponseWriter, r *http.Request) bool
	after  func(action string, w http.ResponseWriter, r *http.Request) bool
}

func newFixture(t *testing.T) *fixture {
	f := &fixture{
		t:        t,
		region:   "us-west-2",
		creds:    fixtureCreds,
		account:  "123456789012",
		owner:    "123456789012",
		others:   make(map[string]fixtureIdentity),
		launches: make(map[string]*fixtureLaunch),
		pageSize: 1000,
	}
	f.ec2 = f.start("ec2")
	f.sts = f.start("sts")
	return f
}

func (f *fixture) start(service string) *httptest.Server {
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.serve(service, w, r)
	}))
	s.EnableHTTP2 = false
	s.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		if st == http.StateNew {
			f.mu.Lock()
			f.conns++
			f.mu.Unlock()
		}
	}
	s.StartTLS()
	f.t.Cleanup(s.Close)
	return s
}

// client returns a client for profile p, or testProfile if p is nil.
func (f *fixture) client(p *Profile) *Client {
	f.t.Helper()
	if p == nil {
		p = testProfile()
	}
	roots := x509.NewCertPool()
	roots.AddCert(f.ec2.Certificate())
	creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return f.creds, nil })
	return f.clientWith(p, creds)
}

// clientWith returns a client for profile p using provider creds.
func (f *fixture) clientWith(p *Profile, creds aws.CredentialsProvider) *Client {
	f.t.Helper()
	roots := x509.NewCertPool()
	roots.AddCert(f.ec2.Certificate())
	c, err := newClient(p, creds, f.ec2.URL+"/", f.sts.URL+"/", roots)
	if err != nil {
		f.t.Fatal(err)
	}
	return c
}

// identity returns the accepted credentials and account for an access key.
func (f *fixture) identity(key string) (fixtureIdentity, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if key == f.creds.AccessKeyID {
		return fixtureIdentity{f.creds, f.account}, true
	}
	id, ok := f.others[key]
	return id, ok
}

// count returns the number of verified requests for action.
func (f *fixture) count(action string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.requests {
		if r.action == action {
			n++
		}
	}
	return n
}

// ec2Requests returns the number of verified EC2 requests.
func (f *fixture) ec2Requests() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.requests {
		if r.service == "ec2" {
			n++
		}
	}
	return n
}

func (f *fixture) connections() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.conns
}

func (f *fixture) serve(service string, w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxCreateBody+1))
	if err != nil {
		http.Error(w, "read", http.StatusBadRequest)
		return
	}
	key, _, _ := strings.Cut(strings.TrimPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential="), "/")
	id, ok := f.identity(key)
	if !ok {
		writeError(w, service, http.StatusForbidden, "InvalidClientTokenId")
		return
	}
	if err := verifySigV4(r, body, id.creds, f.region, service, time.Now()); err != nil {
		f.t.Logf("fixture: signature rejected: %v", err)
		writeError(w, service, http.StatusForbidden, "SignatureDoesNotMatch")
		return
	}
	if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/x-www-form-urlencoded; charset=utf-8" {
		writeError(w, service, http.StatusBadRequest, "InvalidRequest")
		return
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		writeError(w, service, http.StatusBadRequest, "MalformedQueryString")
		return
	}
	action := form.Get("Action")
	f.mu.Lock()
	f.requests = append(f.requests, fixtureRequest{service, action, form, body, key})
	f.mu.Unlock()
	if f.before != nil && f.before(action, w, r) {
		return
	}
	if service == "sts" {
		account := id.account
		if action != "GetCallerIdentity" || form.Get("Version") != stsVersion {
			writeError(w, service, http.StatusBadRequest, "InvalidAction")
			return
		}
		fmt.Fprintf(w, `<GetCallerIdentityResponse xmlns="%s"><GetCallerIdentityResult><Arn>arn:aws:sts::%s:assumed-role/fixture/session</Arn><UserId>AROAFIXTURE:session</UserId><Account>%s</Account></GetCallerIdentityResult><ResponseMetadata><RequestId>sts-1</RequestId></ResponseMetadata></GetCallerIdentityResponse>`, stsNamespace, account, account)
		return
	}
	if form.Get("Version") != ec2Version {
		writeError(w, service, http.StatusBadRequest, "InvalidAction")
		return
	}
	var reply string
	var status int
	switch action {
	case "RunInstances":
		reply, status = f.run(form, body)
	case "DescribeInstances":
		reply, status = f.describe(form)
	case "StopInstances", "StartInstances", "TerminateInstances":
		reply, status = f.transition(action, form)
	case "GetConsoleOutput":
		reply, status = f.consoleOutput(form)
	case "DescribeInstanceCreditSpecifications":
		reply, status = f.credits(form)
	default:
		writeError(w, service, http.StatusBadRequest, "InvalidAction")
		return
	}
	if f.after != nil && f.after(action, w, r) {
		return
	}
	if status != http.StatusOK {
		writeError(w, service, status, reply)
		return
	}
	io.WriteString(w, reply)
}

func (f *fixture) run(form url.Values, body []byte) (string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	token := form.Get("ClientToken")
	if token == "" || form.Get("MinCount") != "1" || form.Get("MaxCount") != "1" {
		return "InvalidParameterValue", http.StatusBadRequest
	}
	if l, ok := f.launches[token]; ok {
		if string(l.body) != string(body) {
			return "IdempotentParameterMismatch", http.StatusBadRequest
		}
		return runXML(l.inst), http.StatusOK
	}
	n := len(f.instances) + 1
	inst := &fixtureInstance{
		reservation: fmt.Sprintf("r-%017x", n),
		owner:       f.owner,
		id:          fmt.Sprintf("i-%017x", 0xa0000+n),
		state:       "pending",
		image:       form.Get("ImageId"),
		itype:       form.Get("InstanceType"),
		subnet:      form.Get("SubnetId"),
		zone:        f.region + "a",
		key:         form.Get("KeyName"),
		token:       token,
		credits:     form.Get("CreditSpecification.CpuCredits"),
	}
	if inst.credits == "" {
		inst.credits = "standard"
	}
	for i := 1; form.Has("SecurityGroupId." + strconv.Itoa(i)); i++ {
		inst.groups = append(inst.groups, form.Get("SecurityGroupId."+strconv.Itoa(i)))
	}
	for i := 1; form.Has("TagSpecification." + strconv.Itoa(i) + ".ResourceType"); i++ {
		p := "TagSpecification." + strconv.Itoa(i) + "."
		if form.Get(p+"ResourceType") != "instance" {
			continue
		}
		for j := 1; form.Has(p + "Tag." + strconv.Itoa(j) + ".Key"); j++ {
			tp := p + "Tag." + strconv.Itoa(j) + "."
			inst.tags = append(inst.tags, Tag{form.Get(tp + "Key"), form.Get(tp + "Value")})
		}
	}
	f.instances = append(f.instances, inst)
	f.launches[token] = &fixtureLaunch{body: body, inst: inst}
	return runXML(inst), http.StatusOK
}

func (f *fixture) describe(form url.Values) (string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var match []*fixtureInstance
	if id := form.Get("InstanceId.1"); id != "" {
		for _, inst := range f.instances {
			if inst.id == id {
				match = append(match, inst)
			}
		}
		if len(match) == 0 {
			return "InvalidInstanceID.NotFound", http.StatusBadRequest
		}
	} else {
	instances:
		for _, inst := range f.instances {
			for i := 1; form.Has("Filter." + strconv.Itoa(i) + ".Name"); i++ {
				name := form.Get("Filter." + strconv.Itoa(i) + ".Name")
				value := form.Get("Filter." + strconv.Itoa(i) + ".Value.1")
				switch {
				case name == "client-token":
					if inst.token != value {
						continue instances
					}
				case strings.HasPrefix(name, "tag:"):
					if !slices.Contains(inst.tags, Tag{strings.TrimPrefix(name, "tag:"), value}) {
						continue instances
					}
				default:
					return "InvalidParameterValue", http.StatusBadRequest
				}
			}
			match = append(match, inst)
		}
	}
	start := 0
	if next := form.Get("NextToken"); next != "" {
		var err error
		if start, err = strconv.Atoi(strings.TrimPrefix(next, "page-")); err != nil {
			return "InvalidParameterValue", http.StatusBadRequest
		}
	}
	end := min(start+f.pageSize, len(match))
	var b strings.Builder
	fmt.Fprintf(&b, `<DescribeInstancesResponse xmlns="%s"><requestId>d-1</requestId><reservationSet>`, ec2Namespace)
	for _, inst := range match[start:end] {
		fmt.Fprintf(&b, `<item><reservationId>%s</reservationId><ownerId>%s</ownerId><groupSet/><instancesSet>%s</instancesSet></item>`,
			inst.reservation, inst.owner, instanceXML(inst))
	}
	b.WriteString(`</reservationSet>`)
	if end < len(match) {
		fmt.Fprintf(&b, `<nextToken>page-%d</nextToken>`, end)
	}
	b.WriteString(`</DescribeInstancesResponse>`)
	return b.String(), http.StatusOK
}

func (f *fixture) transition(action string, form url.Values) (string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := form.Get("InstanceId.1")
	if form.Has("InstanceId.2") {
		return "InvalidParameterCombination", http.StatusBadRequest
	}
	var inst *fixtureInstance
	for _, i := range f.instances {
		if i.id == id {
			inst = i
		}
	}
	if inst == nil {
		return "InvalidInstanceID.NotFound", http.StatusBadRequest
	}
	prev := inst.state
	var cur string
	switch action {
	case "StopInstances":
		cur, inst.state = "stopping", "stopped"
	case "StartInstances":
		cur, inst.state = "pending", "running"
	case "TerminateInstances":
		cur, inst.state = "shutting-down", "terminated"
	}
	return fmt.Sprintf(`<%[1]sResponse xmlns="%[2]s"><requestId>t-1</requestId><instancesSet><item><instanceId>%[3]s</instanceId><currentState><code>0</code><name>%[4]s</name></currentState><previousState><code>0</code><name>%[5]s</name></previousState></item></instancesSet></%[1]sResponse>`,
		action, ec2Namespace, id, cur, prev), http.StatusOK
}

// credits answers DescribeInstanceCreditSpecifications for exactly one
// instance ID, the only form the client may send.
func (f *fixture) credits(form url.Values) (string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(form) != 3 || !form.Has("InstanceId.1") {
		return "InvalidParameterCombination", http.StatusBadRequest
	}
	id := form.Get("InstanceId.1")
	for _, inst := range f.instances {
		if inst.id == id {
			return fmt.Sprintf(`<DescribeInstanceCreditSpecificationsResponse xmlns="%s"><requestId>cs-1</requestId><instanceCreditSpecificationSet><item><instanceId>%s</instanceId><cpuCredits>%s</cpuCredits></item></instanceCreditSpecificationSet></DescribeInstanceCreditSpecificationsResponse>`,
				ec2Namespace, id, inst.credits), http.StatusOK
		}
	}
	return "InvalidInstanceID.NotFound", http.StatusBadRequest
}

// creditProfile returns testProfile with standard CPU credits.
func creditProfile() *Profile {
	p := testProfile()
	p.CPUCredits = "standard"
	return p
}

// creditRequest returns the create request for creditProfile.
func creditRequest(t *testing.T) *CreateRequest {
	t.Helper()
	req, err := NewCreateRequest(creditProfile(), testIdentity(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func (f *fixture) consoleOutput(form url.Values) (string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := form.Get("InstanceId")
	return fmt.Sprintf(`<GetConsoleOutputResponse xmlns="%s"><requestId>c-1</requestId><instanceId>%s</instanceId><timestamp>2026-10-21T00:00:00.000Z</timestamp><output>%s</output></GetConsoleOutputResponse>`,
		ec2Namespace, id, base64.StdEncoding.EncodeToString([]byte(f.console))), http.StatusOK
}

func runXML(inst *fixtureInstance) string {
	return fmt.Sprintf(`<RunInstancesResponse xmlns="%s"><requestId>run-1</requestId><reservationId>%s</reservationId><ownerId>%s</ownerId><groupSet/><instancesSet>%s</instancesSet></RunInstancesResponse>`,
		ec2Namespace, inst.reservation, inst.owner, instanceXML(inst))
}

func instanceXML(inst *fixtureInstance) string {
	var b strings.Builder
	fmt.Fprintf(&b, `<item><instanceId>%s</instanceId><imageId>%s</imageId><instanceState><code>16</code><name>%s</name></instanceState>`,
		inst.id, inst.image, inst.state)
	fmt.Fprintf(&b, `<instanceType>%s</instanceType><placement><availabilityZone>%s</availabilityZone><tenancy>default</tenancy></placement>`, inst.itype, inst.zone)
	fmt.Fprintf(&b, `<subnetId>%s</subnetId><vpcId>vpc-0123456789abcdef0</vpcId><privateIpAddress>10.0.0.5</privateIpAddress>`, inst.subnet)
	if inst.key != "" {
		fmt.Fprintf(&b, `<keyName>%s</keyName>`, inst.key)
	}
	b.WriteString(`<groupSet>`)
	for _, g := range inst.groups {
		fmt.Fprintf(&b, `<item><groupId>%s</groupId><groupName>n</groupName></item>`, g)
	}
	fmt.Fprintf(&b, `</groupSet><clientToken>%s</clientToken><tagSet>`, inst.token)
	for _, t := range inst.tags {
		b.WriteString(`<item><key>`)
		xml.EscapeText(&b, []byte(t.Key))
		b.WriteString(`</key><value>`)
		xml.EscapeText(&b, []byte(t.Value))
		b.WriteString(`</value></item>`)
	}
	b.WriteString(`</tagSet></item>`)
	return b.String()
}

func writeError(w http.ResponseWriter, service string, status int, code string) {
	w.WriteHeader(status)
	if service == "sts" {
		fmt.Fprintf(w, `<ErrorResponse xmlns="%s"><Error><Type>Sender</Type><Code>%s</Code><Message>fixture %s</Message></Error><RequestId>err-1</RequestId></ErrorResponse>`, stsNamespace, code, code)
		return
	}
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Response><Errors><Error><Code>%s</Code><Message>fixture %s</Message></Error></Errors><RequestID>err-1</RequestID></Response>`, code, code)
}

// verifySigV4 verifies the AWS Signature Version 4 Authorization header of
// r, whose body is body, independently of the SDK signer.
func verifySigV4(r *http.Request, body []byte, creds aws.Credentials, region, service string, now time.Time) error {
	const alg = "AWS4-HMAC-SHA256"
	rest, ok := strings.CutPrefix(r.Header.Get("Authorization"), alg+" ")
	if !ok {
		return errors.New("no SigV4 authorization")
	}
	fields := strings.Split(rest, ", ")
	if len(fields) != 3 {
		return errors.New("authorization has wrong field count")
	}
	credential, ok1 := strings.CutPrefix(fields[0], "Credential=")
	signedList, ok2 := strings.CutPrefix(fields[1], "SignedHeaders=")
	signature, ok3 := strings.CutPrefix(fields[2], "Signature=")
	if !ok1 || !ok2 || !ok3 {
		return errors.New("authorization fields malformed")
	}
	scope := strings.Split(credential, "/")
	if len(scope) != 5 || scope[0] != creds.AccessKeyID || scope[2] != region || scope[3] != service || scope[4] != "aws4_request" {
		return fmt.Errorf("credential scope %q is wrong", credential)
	}
	amzDate := r.Header.Get("X-Amz-Date")
	t, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil || scope[1] != amzDate[:8] {
		return errors.New("x-amz-date is malformed or disagrees with the scope")
	}
	if d := now.Sub(t); d > 5*time.Minute || d < -5*time.Minute {
		return errors.New("x-amz-date is outside the allowed skew")
	}
	signed := strings.Split(signedList, ";")
	if !slices.IsSorted(signed) || len(slices.Compact(slices.Clone(signed))) != len(signed) ||
		!slices.Contains(signed, "host") || !slices.Contains(signed, "x-amz-date") {
		return errors.New("signed headers are incomplete or unsorted")
	}
	if creds.SessionToken != "" {
		if r.Header.Get("X-Amz-Security-Token") != creds.SessionToken || !slices.Contains(signed, "x-amz-security-token") {
			return errors.New("session token is missing or unsigned")
		}
	}
	var canon strings.Builder
	canon.WriteString(r.Method + "\n")
	path := r.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	canon.WriteString(path + "\n")
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return err
	}
	canon.WriteString(strings.ReplaceAll(query.Encode(), "+", "%20") + "\n")
	for _, h := range signed {
		var v string
		switch h {
		case "host":
			v = r.Host
		case "content-length":
			v = strconv.FormatInt(r.ContentLength, 10)
		default:
			v = strings.Join(r.Header.Values(h), ",")
		}
		canon.WriteString(h + ":" + strings.Join(strings.Fields(v), " ") + "\n")
	}
	canon.WriteString("\n" + signedList + "\n")
	sum := sha256.Sum256(body)
	canon.WriteString(hex.EncodeToString(sum[:]))

	reqHash := sha256.Sum256([]byte(canon.String()))
	toSign := alg + "\n" + amzDate + "\n" + strings.Join(scope[1:], "/") + "\n" + hex.EncodeToString(reqHash[:])
	key := []byte("AWS4" + creds.SecretAccessKey)
	for _, s := range []string{scope[1], region, service, "aws4_request"} {
		key = hmacSHA256(key, s)
	}
	want := hex.EncodeToString(hmacSHA256(key, toSign))
	if !hmac.Equal([]byte(want), []byte(signature)) {
		return errors.New("signature does not match")
	}
	return nil
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// TestVerifySigV4KnownAnswer checks the fixture's verifier against the
// worked IAM ListUsers example in the AWS Signature Version 4 documentation.
func TestVerifySigV4KnownAnswer(t *testing.T) {
	creds := aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}
	now := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	newReq := func(sig string) *http.Request {
		r := httptest.NewRequest("GET", "https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08", nil)
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
		r.Header.Set("X-Amz-Date", "20150830T123600Z")
		r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature="+sig)
		return r
	}
	const sig = "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
	if err := verifySigV4(newReq(sig), nil, creds, "us-east-1", "iam", now); err != nil {
		t.Fatalf("documented signature rejected: %v", err)
	}
	bad := "6" + sig[1:]
	if err := verifySigV4(newReq(bad), nil, creds, "us-east-1", "iam", now); err == nil {
		t.Fatal("altered signature accepted")
	}
}
