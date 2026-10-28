// Package ec2test serves a local TLS model of the EC2 and STS Query APIs
// to a real [ec2api.Client], for tests of code built on that client.
//
// A [Server] verifies every request's SigV4 signature independently of the
// SDK signer and models only the behavior skygo depends on: ClientToken
// idempotency, client-token and tag filters, NextToken pages, settled
// lifecycle transitions and CPU credit options. [Rule] values inject the
// failures a caller must survive: provider errors, replies lost after the
// request took effect, and requests the client abandons in flight.
//
// The model is not EC2. Passing tests against it is not evidence of AWS
// behavior.
package ec2test

import (
	"bytes"
	"crypto/x509"
	"fmt"
	"io"
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
	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
)

// Wire constants of the API versions ec2api speaks.
const (
	ec2Version   = "2016-11-15"
	stsVersion   = "2011-06-15"
	ec2Namespace = "http://ec2.amazonaws.com/doc/" + ec2Version + "/"
	stsNamespace = "https://sts.amazonaws.com/doc/" + stsVersion + "/"
)

// ProviderMessage is the message of every error the server answers. Code
// that keeps provider text private never prints it.
const ProviderMessage = "fixture provider message that must stay private"

// Options configures a Server.
type Options struct {
	Account string          // account STS reports and new reservations belong to
	Region  string          // region every request must be signed for
	Creds   aws.Credentials // the one accepted credential
}

// A Rule replaces the server's answer to one request, or to every request,
// of an action.
type Rule struct {
	Action string // Query action, such as "RunInstances"
	Nth    int    // which request of Action, counting from 1; 0 matches every one

	// Apply applies the request to the model before answering. Without
	// Apply, Drop or Hang, the request has no effect.
	Apply bool

	// Code, if set, answers with a provider error of that code and
	// Status (400 if zero).
	Code   string
	Status int

	// Drop applies the request and closes the connection without a
	// response.
	Drop bool

	// Hang applies the request, waits until the client abandons it, and
	// closes the connection without a response.
	Hang bool

	// Hit, if not nil, is closed when the rule matches. Done, if not nil,
	// is closed when the handler that matched it returns. Use them only
	// with a rule that matches once.
	Hit, Done chan struct{}
}

// An Instance is the server's view of one instance.
type Instance struct {
	ID          string
	State       string
	ClientToken string
}

// A Server is a local EC2 and STS endpoint pair.
type Server struct {
	tb    testing.TB
	opt   Options
	ec2   *httptest.Server
	sts   *httptest.Server
	roots *x509.CertPool

	mu         sync.Mutex
	account    string
	counts     map[string]int
	total      int
	rules      []Rule
	launches   map[string]*launch // by ClientToken
	instances  []*instance
	pageSize   int
	emptyPages int
}

type instance struct {
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
	tags        []ec2api.Tag
}

type launch struct {
	body []byte
	inst *instance
}

// NewServer starts a server that closes when tb's test ends.
func NewServer(tb testing.TB, opt Options) *Server {
	s := &Server{
		tb:       tb,
		opt:      opt,
		account:  opt.Account,
		counts:   make(map[string]int),
		launches: make(map[string]*launch),
		pageSize: 1000,
	}
	s.ec2 = s.start("ec2")
	s.sts = s.start("sts")
	s.roots = x509.NewCertPool()
	s.roots.AddCert(s.ec2.Certificate())
	s.roots.AddCert(s.sts.Certificate())
	return s
}

func (s *Server) start(service string) *httptest.Server {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.serve(service, w, r)
	}))
	srv.EnableHTTP2 = false
	srv.StartTLS()
	s.tb.Cleanup(srv.Close)
	return srv
}

// NewClient returns a client for profile p that sends its requests to s
// and trusts only s's certificate. Its signature matches ec2api.NewClient.
func (s *Server) NewClient(p *ec2api.Profile, creds aws.CredentialsProvider) (*ec2api.Client, error) {
	return ec2api.NewClientWithEndpoints(p, creds, s.ec2.URL+"/", s.sts.URL+"/", s.roots)
}

// AddRule adds r. The first matching rule answers a request.
func (s *Server) AddRule(r Rule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules = append(s.rules, r)
}

// SetAccount sets the account STS reports.
func (s *Server) SetAccount(account string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.account = account
}

// SetPaging makes searches by filter answer size reservations per page,
// after empty pages that carry only a NextToken.
func (s *Server) SetPaging(size, empty int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pageSize, s.emptyPages = size, empty
}

// SetState sets instance id's state, as a change made outside skygo.
func (s *Server) SetState(id, state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if inst := s.find(id); inst != nil {
		inst.state = state
	}
}

// AddTag adds a tag to instance id, as a change made outside skygo.
func (s *Server) AddTag(id, key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if inst := s.find(id); inst != nil {
		inst.tags = append(inst.tags, ec2api.Tag{Key: key, Value: value})
	}
}

// SetCredits sets the CPU credit option of instance id.
func (s *Server) SetCredits(id, credits string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if inst := s.find(id); inst != nil {
		inst.credits = credits
	}
}

// Instances returns every instance the server has launched.
func (s *Server) Instances() []Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Instance
	for _, inst := range s.instances {
		out = append(out, Instance{ID: inst.id, State: inst.state, ClientToken: inst.token})
	}
	return out
}

// Count returns the number of signed requests for action.
func (s *Server) Count(action string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[action]
}

// Total returns the number of signed requests.
func (s *Server) Total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

func (s *Server) find(id string) *instance {
	for _, inst := range s.instances {
		if inst.id == id {
			return inst
		}
	}
	return nil
}

func (s *Server) serve(service string, w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err != nil {
		http.Error(w, "read", http.StatusBadRequest)
		return
	}
	if err := verifySigV4(r, body, s.opt.Creds, s.opt.Region, service, time.Now()); err != nil {
		s.tb.Logf("ec2test: signature rejected: %v", err)
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

	s.mu.Lock()
	s.total++
	s.counts[action]++
	n := s.counts[action]
	var rule Rule
	matched := false
	for _, rl := range s.rules {
		if rl.Action == action && (rl.Nth == 0 || rl.Nth == n) {
			rule, matched = rl, true
			break
		}
	}
	s.mu.Unlock()

	if matched {
		if rule.Hit != nil {
			close(rule.Hit)
		}
		if rule.Done != nil {
			defer close(rule.Done)
		}
		if !rule.Apply && !rule.Drop && !rule.Hang {
			writeError(w, service, rule.Status, rule.Code)
			return
		}
	}
	reply, status, code := s.apply(service, action, form, body)
	switch {
	case !matched:
	case rule.Hang:
		<-r.Context().Done()
		panic(http.ErrAbortHandler)
	case rule.Drop:
		panic(http.ErrAbortHandler)
	case rule.Code != "":
		writeError(w, service, rule.Status, rule.Code)
		return
	}
	if status != http.StatusOK {
		writeError(w, service, status, code)
		return
	}
	io.WriteString(w, reply)
}

// apply applies one request to the model and returns its answer: a reply,
// or a status and provider error code.
func (s *Server) apply(service, action string, form url.Values, body []byte) (reply string, status int, code string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if service == "sts" {
		if action != "GetCallerIdentity" || form.Get("Version") != stsVersion {
			return "", http.StatusBadRequest, "InvalidAction"
		}
		return fmt.Sprintf(`<GetCallerIdentityResponse xmlns="%s"><GetCallerIdentityResult><Arn>arn:aws:sts::%s:assumed-role/fixture/session</Arn><UserId>AROAFIXTURE:session</UserId><Account>%s</Account></GetCallerIdentityResult><ResponseMetadata><RequestId>sts-1</RequestId></ResponseMetadata></GetCallerIdentityResponse>`,
			stsNamespace, s.account, s.account), http.StatusOK, ""
	}
	if form.Get("Version") != ec2Version {
		return "", http.StatusBadRequest, "InvalidAction"
	}
	switch action {
	case "RunInstances":
		return s.run(form, body)
	case "DescribeInstances":
		return s.describe(form)
	case "StopInstances", "StartInstances", "TerminateInstances":
		return s.transition(action, form)
	case "DescribeInstanceCreditSpecifications":
		return s.credits(form)
	}
	return "", http.StatusBadRequest, "InvalidAction"
}

func (s *Server) run(form url.Values, body []byte) (string, int, string) {
	token := form.Get("ClientToken")
	if token == "" || form.Get("MinCount") != "1" || form.Get("MaxCount") != "1" {
		return "", http.StatusBadRequest, "InvalidParameterValue"
	}
	if l, ok := s.launches[token]; ok {
		if !bytes.Equal(l.body, body) {
			return "", http.StatusBadRequest, "IdempotentParameterMismatch"
		}
		return runXML(l.inst), http.StatusOK, ""
	}
	n := len(s.instances) + 1
	inst := &instance{
		reservation: fmt.Sprintf("r-%017x", n),
		owner:       s.account,
		id:          fmt.Sprintf("i-%017x", 0xa0000+n),
		state:       "pending",
		image:       form.Get("ImageId"),
		itype:       form.Get("InstanceType"),
		subnet:      form.Get("SubnetId"),
		zone:        s.opt.Region + "a",
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
			inst.tags = append(inst.tags, ec2api.Tag{Key: form.Get(tp + "Key"), Value: form.Get(tp + "Value")})
		}
	}
	s.instances = append(s.instances, inst)
	s.launches[token] = &launch{body: bytes.Clone(body), inst: inst}
	reply := runXML(inst)
	inst.state = "running" // the launch settles before the next request
	return reply, http.StatusOK, ""
}

func (s *Server) describe(form url.Values) (string, int, string) {
	var match []*instance
	filtered := false
	if id := form.Get("InstanceId.1"); id != "" {
		if inst := s.find(id); inst != nil {
			match = append(match, inst)
		} else {
			return "", http.StatusBadRequest, "InvalidInstanceID.NotFound"
		}
	} else {
		filtered = true
	instances:
		for _, inst := range s.instances {
			for i := 1; form.Has("Filter." + strconv.Itoa(i) + ".Name"); i++ {
				name := form.Get("Filter." + strconv.Itoa(i) + ".Name")
				value := form.Get("Filter." + strconv.Itoa(i) + ".Value.1")
				switch {
				case name == "client-token":
					if inst.token != value {
						continue instances
					}
				case strings.HasPrefix(name, "tag:"):
					if !slices.Contains(inst.tags, ec2api.Tag{Key: strings.TrimPrefix(name, "tag:"), Value: value}) {
						continue instances
					}
				default:
					return "", http.StatusBadRequest, "InvalidParameterValue"
				}
			}
			match = append(match, inst)
		}
	}

	// Tokens: "" starts; "empty-K" follows K empty pages; "page-N"
	// starts at match N.
	empties, start := 0, 0
	var err error
	switch next := form.Get("NextToken"); {
	case next == "":
	case strings.HasPrefix(next, "empty-"):
		empties, err = strconv.Atoi(strings.TrimPrefix(next, "empty-"))
	case strings.HasPrefix(next, "page-"):
		empties = s.emptyPages
		start, err = strconv.Atoi(strings.TrimPrefix(next, "page-"))
	default:
		err = strconv.ErrSyntax
	}
	if err != nil || start < 0 || start > len(match) {
		return "", http.StatusBadRequest, "InvalidParameterValue"
	}

	var b strings.Builder
	fmt.Fprintf(&b, `<DescribeInstancesResponse xmlns="%s"><requestId>d-1</requestId><reservationSet>`, ec2Namespace)
	if filtered && empties < s.emptyPages {
		fmt.Fprintf(&b, `</reservationSet><nextToken>empty-%d</nextToken></DescribeInstancesResponse>`, empties+1)
		return b.String(), http.StatusOK, ""
	}
	end := min(start+s.pageSize, len(match))
	for _, inst := range match[start:end] {
		fmt.Fprintf(&b, `<item><reservationId>%s</reservationId><ownerId>%s</ownerId><groupSet/><instancesSet>%s</instancesSet></item>`,
			inst.reservation, inst.owner, instanceXML(inst))
	}
	b.WriteString(`</reservationSet>`)
	if end < len(match) {
		fmt.Fprintf(&b, `<nextToken>page-%d</nextToken>`, end)
	}
	b.WriteString(`</DescribeInstancesResponse>`)
	return b.String(), http.StatusOK, ""
}

// transition answers a lifecycle action with the state the instance moves
// through and settles it in its final state before the next request.
func (s *Server) transition(action string, form url.Values) (string, int, string) {
	if form.Has("InstanceId.2") {
		return "", http.StatusBadRequest, "InvalidParameterCombination"
	}
	id := form.Get("InstanceId.1")
	inst := s.find(id)
	if inst == nil {
		return "", http.StatusBadRequest, "InvalidInstanceID.NotFound"
	}
	prev := inst.state
	cur, final := prev, prev
	switch action {
	case "StopInstances":
		if prev != "stopped" {
			cur, final = "stopping", "stopped"
		}
	case "StartInstances":
		if prev != "running" {
			cur, final = "pending", "running"
		}
	case "TerminateInstances":
		if prev != "terminated" {
			cur, final = "shutting-down", "terminated"
		}
	}
	if prev == "terminated" && action != "TerminateInstances" {
		return "", http.StatusBadRequest, "IncorrectInstanceState"
	}
	inst.state = final
	return fmt.Sprintf(`<%[1]sResponse xmlns="%[2]s"><requestId>t-1</requestId><instancesSet><item><instanceId>%[3]s</instanceId><currentState><code>0</code><name>%[4]s</name></currentState><previousState><code>0</code><name>%[5]s</name></previousState></item></instancesSet></%[1]sResponse>`,
		action, ec2Namespace, id, cur, prev), http.StatusOK, ""
}

func (s *Server) credits(form url.Values) (string, int, string) {
	if len(form) != 3 || !form.Has("InstanceId.1") {
		return "", http.StatusBadRequest, "InvalidParameterCombination"
	}
	id := form.Get("InstanceId.1")
	inst := s.find(id)
	if inst == nil {
		return "", http.StatusBadRequest, "InvalidInstanceID.NotFound"
	}
	return fmt.Sprintf(`<DescribeInstanceCreditSpecificationsResponse xmlns="%s"><requestId>cs-1</requestId><instanceCreditSpecificationSet><item><instanceId>%s</instanceId><cpuCredits>%s</cpuCredits></item></instanceCreditSpecificationSet></DescribeInstanceCreditSpecificationsResponse>`,
		ec2Namespace, id, inst.credits), http.StatusOK, ""
}

func runXML(inst *instance) string {
	return fmt.Sprintf(`<RunInstancesResponse xmlns="%s"><requestId>run-1</requestId><reservationId>%s</reservationId><ownerId>%s</ownerId><groupSet/><instancesSet>%s</instancesSet></RunInstancesResponse>`,
		ec2Namespace, inst.reservation, inst.owner, instanceXML(inst))
}

func instanceXML(inst *instance) string {
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
		fmt.Fprintf(&b, `<item><key>%s</key><value>%s</value></item>`, escape(t.Key), escape(t.Value))
	}
	b.WriteString(`</tagSet></item>`)
	return b.String()
}

func escape(s string) string {
	var b strings.Builder
	for _, c := range s {
		switch c {
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		case '&':
			b.WriteString("&amp;")
		default:
			b.WriteRune(c)
		}
	}
	return b.String()
}

func writeError(w http.ResponseWriter, service string, status int, code string) {
	if status == 0 {
		status = http.StatusBadRequest
	}
	w.WriteHeader(status)
	if service == "sts" {
		fmt.Fprintf(w, `<ErrorResponse xmlns="%s"><Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error><RequestId>err-1</RequestId></ErrorResponse>`, stsNamespace, code, ProviderMessage)
		return
	}
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Response><Errors><Error><Code>%s</Code><Message>%s</Message></Error></Errors><RequestID>err-1</RequestID></Response>`, code, ProviderMessage)
}
