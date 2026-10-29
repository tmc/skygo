//go:build unix

package gce

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

// This file models Compute Engine with a local TLS server. Tests using it
// are MODELED. Machines reach it through newConfiguredClient, the same
// construction Create and Open use, changing only the endpoint origin and
// the trusted root (the fixture's certificate). A test that wraps the
// transport to fault a body's Close says so. All tests are NOT_RUN by
// their author.

const (
	testKey   = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl test"
	testToken = "ya29.test-access-token-never-logged"
)

func testConfig() Config {
	return Config{
		Project:          "skygo-test",
		ComputeProjectID: "123456789012",
		Zone:             "us-central1-a",
		MachineType:      "zones/us-central1-a/machineTypes/t2a-standard-1",
		Subnet:           "projects/skygo-net/regions/us-central1/subnetworks/default",
		Image:            Image{Project: "debian-cloud", Name: "debian-12-arm64-v20261001", ID: "4242"},
		DiskType:         "zones/us-central1-a/diskTypes/pd-balanced",
		DiskGiB:          20,
		SSHUser:          "skygo",
		AuthorizedKey:    testKey,
	}
}

func testTok() oauth2.Token {
	return oauth2.Token{AccessToken: testToken, TokenType: "Bearer", Expiry: time.Now().Add(time.Hour)}
}

type fakeReq struct {
	Method, Path, Query, Auth, Proto string
	Body                             []byte
}

type fakeDisk struct {
	ID, Name, ImageID, Status string
	Labels                    map[string]string
	Users                     []string
}

type fakeVM struct {
	ID, Name, Status string
	Body             instanceInsertBody
}

// fakeCompute is a stateful model of the methods this package calls. Its
// faults change what Compute does, never what the test asserts.
type fakeCompute struct {
	t     *testing.T
	srv   *httptest.Server
	conns atomic.Int32 // TLS connections accepted

	mu     sync.Mutex
	reqs   []fakeReq
	disks  map[string]*fakeDisk
	vms    map[string]*fakeVM
	ops    map[string]*operationWire
	order  []string
	nextID uint64

	imageID         string // what images.get reports
	diskImageID     string // what a new disk reports as sourceImageId
	osLogin         bool
	foreignDisk     bool            // any disk name exists, owned by someone else
	dropReply       map[string]bool // "POST disks" etc: apply, then abort the reply
	hideOps         bool            // operations absent from list and get
	pageSize        int             // operations per list page; 0 means all
	truncate        bool            // list always has another page
	extraOps        []*operationWire
	opStatus        string        // if set, what operations.get reports
	stopNoEffect    bool          // stop operation DONE, VM stays RUNNING
	startNoEffect   bool          // start operation DONE, VM stays TERMINATED
	mutationStarted chan struct{} // optional notification before a held mutation
	opError         string        // mutations finish DONE with this error code
	replyPatch      func(*operationWire)
	afterGetVM      func(*fakeCompute)
	imagePatch      func(map[string]any) // edits the images.get reply
	typePatch       func(map[string]any) // edits the machineTypes.get reply
	getStatus       int                  // if set, every GET fails with it and a secret body
	mutStatus       int                  // if set, every mutation fails with it and a secret body
	root            *stateRoot           // of the last machine created for f

	hold          chan struct{} // closed by release
	holdOnce      sync.Once
	holdMutations bool // mutations block on hold before replying
	holdReadBody  bool // reads send headers, then block on hold before the body
}

// newFake starts the model. Its server's Close is registered first, so it
// runs after every cleanup a test registers later: by then those cleanups
// have released held replies and joined their originals.
func newFake(t *testing.T) *fakeCompute {
	f := &fakeCompute{t: t, disks: map[string]*fakeDisk{}, vms: map[string]*fakeVM{}, ops: map[string]*operationWire{}, nextID: 9000, imageID: "4242", diskImageID: "4242", dropReply: map[string]bool{}}
	f.srv = httptest.NewUnstartedServer(http.HandlerFunc(f.serve))
	f.srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			f.conns.Add(1)
		}
	}
	f.srv.StartTLS()
	t.Cleanup(f.srv.Close)
	t.Cleanup(f.release)
	return f
}

// holdReplies makes f's hold channel; release closes it.
func (f *fakeCompute) holdReplies() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.hold == nil {
		f.hold = make(chan struct{})
	}
	return f.hold
}

// release lets every held reply and body Close proceed. It is safe to
// call more than once.
func (f *fakeCompute) release() {
	f.mu.Lock()
	h := f.hold
	f.mu.Unlock()
	if h != nil {
		f.holdOnce.Do(func() { close(h) })
	}
}

// client returns the configured client for f: production transport
// choices, f's origin and f's certificate as the only trusted root.
func (f *fakeCompute) client() *computeClient {
	roots := x509.NewCertPool()
	roots.AddCert(f.srv.Certificate())
	return newConfiguredClient(f.srv.URL+"/compute/v1/", roots)
}

// faultClient is f.client with each matching response body's Close
// replaced (see bodyFault). MODELED.
func (f *fakeCompute) faultClient(b *bodyFault) *computeClient {
	cl := f.client()
	b.rt = cl.hc.Transport
	cl.hc.Transport = b
	return cl
}

// bodyFault wraps a transport. For responses to requests of method, it
// replaces the body's Close: Close waits for hold, if set, then runs the
// real Close and returns err if set. It counts Close calls, so a test can
// see that no body is closed twice.
type bodyFault struct {
	rt       http.RoundTripper
	method   string
	hold     <-chan struct{}
	err      error
	readErr  error           // if set, every Read of the body fails with it
	readHold <-chan struct{} // if set, every Read of the body waits for it
	suffix   string          // if set, only requests whose path ends so
	closes   atomic.Int32
}

func (b *bodyFault) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := b.rt.RoundTrip(req)
	if err != nil || req.Method != b.method || !strings.HasSuffix(req.URL.Path, b.suffix) {
		return resp, err
	}
	resp.Body = &faultBody{ReadCloser: resp.Body, b: b}
	return resp, nil
}

type faultBody struct {
	io.ReadCloser
	b *bodyFault
}

func (fb *faultBody) Read(p []byte) (int, error) {
	if fb.b.readHold != nil {
		<-fb.b.readHold
	}
	if fb.b.readErr != nil {
		return 0, fb.b.readErr
	}
	return fb.ReadCloser.Read(p)
}

func (fb *faultBody) Close() error {
	fb.b.closes.Add(1)
	if fb.b.hold != nil {
		<-fb.b.hold
	}
	err := fb.ReadCloser.Close()
	if fb.b.err != nil {
		return fb.b.err
	}
	return err
}

func (f *fakeCompute) link(p string) string { return "https://www.googleapis.com/compute/v1/" + p }

func (f *fakeCompute) id() string { f.nextID++; return strconv.FormatUint(f.nextID, 10) }

func (f *fakeCompute) count(method, suffix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.reqs {
		if r.Method == method && strings.HasSuffix(r.Path, suffix) {
			n++
		}
	}
	return n
}

// mutations returns the number of requests other than GET.
func (f *fakeCompute) mutations() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.reqs {
		if r.Method != "GET" {
			n++
		}
	}
	return n
}

func (f *fakeCompute) requests() []fakeReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeReq(nil), f.reqs...)
}

func (f *fakeCompute) serve(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	path := strings.TrimPrefix(req.URL.Path, "/compute/v1/")
	f.mu.Lock()
	f.reqs = append(f.reqs, fakeReq{req.Method, path, req.URL.RawQuery, req.Header.Get("Authorization"), req.Proto, body})
	hold, holdMut, holdRead, started := f.hold, f.holdMutations, f.holdReadBody, f.mutationStarted
	f.mu.Unlock()
	if req.Method != "GET" && started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if req.Method != "GET" && holdMut && hold != nil {
		<-hold
	}
	f.mu.Lock()
	code, v, drop, after := f.route(req.Method, path, req.URL.Query(), body)
	f.mu.Unlock()
	if after != nil {
		after(f)
	}
	if drop {
		panic(http.ErrAbortHandler)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if req.Method == "GET" && holdRead && hold != nil {
		w.(http.Flusher).Flush()
		<-hold
	}
	json.NewEncoder(w).Encode(v)
}

var notFound = map[string]any{"error": map[string]any{"code": 404}}

// secretText is server text that no error message may carry.
const secretText = "secret-server-text ya29.leaked"

// testTypeID is the numeric id the model reports for the machine type.
const testTypeID = "777000"

func (f *fakeCompute) route(method, path string, q map[string][]string, body []byte) (int, any, bool, func(*fakeCompute)) {
	cfg := testConfig()
	zone := "projects/" + cfg.Project + "/zones/" + cfg.Zone
	secret := map[string]any{"error": map[string]any{"message": secretText}}
	switch {
	case method == "GET" && f.getStatus != 0:
		return f.getStatus, secret, false, nil
	case method != "GET" && f.mutStatus != 0:
		return f.mutStatus, secret, false, nil
	case method == "GET" && path == "projects/"+cfg.Image.Project+"/global/images/"+cfg.Image.Name:
		v := map[string]any{"name": cfg.Image.Name, "id": f.imageID, "status": "READY",
			"selfLink": f.link(path), "architecture": "ARM64"}
		if f.imagePatch != nil {
			f.imagePatch(v)
		}
		return 200, v, false, nil
	case method == "GET" && path == "projects/"+cfg.Project+"/"+cfg.MachineType:
		v := map[string]any{"name": "t2a-standard-1", "id": testTypeID, "zone": cfg.Zone,
			"selfLink": f.link(path), "architecture": "ARM64"}
		if f.typePatch != nil {
			f.typePatch(v)
		}
		return 200, v, false, nil
	case method == "GET" && path == "projects/"+cfg.Project:
		md := []metadataItem{}
		if f.osLogin {
			md = append(md, metadataItem{"enable-oslogin", "TRUE"})
		}
		return 200, map[string]any{"name": cfg.Project, "id": cfg.ComputeProjectID, "commonInstanceMetadata": map[string]any{"items": md}}, false, nil
	case !strings.HasPrefix(path, zone+"/"):
		return 404, notFound, false, nil
	}
	p := strings.Split(strings.TrimPrefix(path, zone+"/"), "/")
	switch {
	case p[0] == "operations" && len(p) == 1:
		var items []*operationWire
		if !f.hideOps {
			for _, n := range f.order {
				items = append(items, f.ops[n])
			}
		}
		items = append(items, f.extraOps...)
		start := 0
		if tok := q["pageToken"]; len(tok) > 0 {
			start, _ = strconv.Atoi(tok[0])
		}
		end := len(items)
		if f.pageSize > 0 && start+f.pageSize < end {
			end = start + f.pageSize
		}
		if start > len(items) {
			start = len(items)
		}
		next := ""
		if end < len(items) {
			next = strconv.Itoa(end)
		} else if f.truncate {
			next = strconv.Itoa(len(items))
		}
		return 200, map[string]any{"items": items[start:end], "nextPageToken": next}, false, nil
	case p[0] == "operations":
		op := f.ops[p[1]]
		if op == nil || f.hideOps {
			return 404, notFound, false, nil
		}
		op.Status = "DONE"
		if f.opStatus != "" {
			op.Status = f.opStatus
		}
		return 200, op, false, nil
	case p[0] == "disks" && method == "GET":
		d := f.disks[p[1]]
		if d == nil && f.foreignDisk {
			return 200, map[string]any{"id": "77", "name": p[1]}, false, nil
		}
		if d == nil {
			return 404, notFound, false, nil
		}
		users := []string{}
		for _, u := range d.Users {
			users = append(users, f.link(u))
		}
		return 200, map[string]any{"id": d.ID, "name": d.Name, "zone": f.link(zone), "type": f.link("projects/" + cfg.Project + "/" + cfg.DiskType), "sizeGb": "20", "sourceImageId": d.ImageID, "status": d.Status, "users": users, "labels": d.Labels}, false, nil
	case p[0] == "disks" && method == "POST":
		var b diskInsertBody
		json.Unmarshal(body, &b)
		d := &fakeDisk{ID: f.id(), Name: b.Name, ImageID: f.diskImageID, Status: "READY", Labels: b.Labels}
		f.disks[b.Name] = d
		return f.mutate("insert", zone, "disks/"+b.Name, d.ID, q, "POST disks")
	case p[0] == "disks" && method == "DELETE":
		d := f.disks[p[1]]
		if d == nil {
			return 404, notFound, false, nil
		}
		delete(f.disks, p[1])
		return f.mutate("delete", zone, "disks/"+p[1], d.ID, q, "DELETE disks")
	case p[0] == "instances" && method == "GET":
		vm := f.vms[p[1]]
		if vm == nil {
			return 404, notFound, false, nil
		}
		b := vm.Body
		disks := []map[string]any{}
		for _, d := range b.Disks {
			disks = append(disks, map[string]any{"boot": d.Boot, "autoDelete": d.AutoDelete, "source": f.link(d.Source)})
		}
		return 200, map[string]any{"id": vm.ID, "name": vm.Name, "zone": f.link(zone), "status": vm.Status, "machineType": f.link(b.MachineType), "disks": disks, "networkInterfaces": []map[string]any{{"subnetwork": f.link(b.NetworkInterfaces[0].Subnetwork)}}, "metadata": b.Metadata, "labels": b.Labels}, false, f.afterGetVM
	case p[0] == "instances" && method == "POST" && len(p) == 1:
		var b instanceInsertBody
		json.Unmarshal(body, &b)
		vm := &fakeVM{ID: f.id(), Name: b.Name, Status: "RUNNING", Body: b}
		f.vms[b.Name] = vm
		src := strings.Split(b.Disks[0].Source, "/")
		if d := f.disks[src[len(src)-1]]; d != nil {
			d.Users = []string{zone + "/instances/" + b.Name}
		}
		return f.mutate("insert", zone, "instances/"+b.Name, vm.ID, q, "POST instances")
	case p[0] == "instances" && method == "POST" && len(p) == 3:
		vm := f.vms[p[1]]
		if vm == nil {
			return 404, notFound, false, nil
		}
		switch {
		case p[2] == "stop" && !f.stopNoEffect:
			vm.Status = "TERMINATED"
		case p[2] == "start" && !f.startNoEffect:
			vm.Status = "RUNNING"
		}
		return f.mutate(p[2], zone, "instances/"+p[1], vm.ID, q, "POST "+p[2])
	case p[0] == "instances" && method == "DELETE":
		vm := f.vms[p[1]]
		if vm == nil {
			return 404, notFound, false, nil
		}
		delete(f.vms, p[1])
		for _, d := range f.disks {
			d.Users = nil
		}
		return f.mutate("delete", zone, "instances/"+p[1], vm.ID, q, "DELETE instances")
	}
	return 404, notFound, false, nil
}

func (f *fakeCompute) mutate(typ, zone, target, targetID string, q map[string][]string, key string) (int, any, bool, func(*fakeCompute)) {
	name := fmt.Sprintf("operation-%d", len(f.order)+1)
	op := &operationWire{ID: f.id(), Name: name, Zone: f.link(zone), OperationType: typ, TargetLink: f.link(zone + "/" + target), TargetID: targetID, Status: "RUNNING", ClientOperationID: q["requestId"][0]}
	if f.opError != "" {
		op.Error = &operationError{Errors: []struct {
			Code string `json:"code"`
		}{{Code: f.opError}}}
	}
	if f.replyPatch != nil {
		f.replyPatch(op)
	}
	f.ops[name] = op
	f.order = append(f.order, name)
	return 200, op, f.dropReply[key], nil
}

// A stateRoot is a test-owned directory for state directories and the
// owner of every Machine and constructor error registered with it. It is
// removed after the test only if each owner positively settled: every
// Machine closed, and no constructor error keeps a file or the lock.
// Otherwise the root, with its machines and errors, joins unsettled.
type stateRoot struct {
	dir      string
	machines []*Machine
	errs     []error
	keep     bool
}

// unsettled keeps every stateRoot whose owners did not settle, and so the
// files and locks they own, until the test process exits.
var (
	unsettledMu sync.Mutex
	unsettled   []*stateRoot
)

func newStateRoot(t *testing.T) *stateRoot {
	t.Helper()
	dir, err := os.MkdirTemp("", "gce-test-")
	if err != nil {
		t.Fatal(err)
	}
	sr := &stateRoot{dir: dir}
	t.Cleanup(func() {
		if sr.keep {
			unsettledMu.Lock()
			unsettled = append(unsettled, sr)
			unsettledMu.Unlock()
			t.Logf("kept %s: an owner there did not settle", dir)
			return
		}
		os.RemoveAll(dir)
	})
	return sr
}

// ownsFile reports whether err keeps a file or the state lock.
func ownsFile(err error) bool {
	var le *lockedError
	var ce *closeError
	return errors.As(err, &le) || errors.As(err, &ce)
}

// own registers the result of a constructor with sr. Call it at once,
// before any check can fail. sr retains a constructor error; one that
// keeps a file or the lock keeps sr. For m, own registers the cleanup that
// releases f's held replies, waits without a timeout for m's pending
// original to return, then closes m; if Close fails, m keeps sr.
func (sr *stateRoot) own(t *testing.T, f *fakeCompute, m *Machine, err error) {
	if err != nil {
		sr.errs = append(sr.errs, err)
		if ownsFile(err) {
			sr.keep = true
		}
	}
	if m == nil {
		return
	}
	sr.machines = append(sr.machines, m)
	t.Cleanup(func() {
		if f != nil {
			f.release()
		}
		if c := m.pending; c != nil {
			<-c.done
		}
		if err := m.Close(); err != nil {
			sr.keep = true
			t.Logf("machine not closed: %v", err)
		}
	})
}

// newTestMachine creates a machine for f in a new state directory through
// create with f's configured client and registers its cleanup.
func newTestMachine(t *testing.T, f *fakeCompute) (*Machine, string) {
	t.Helper()
	return newTestMachineWith(t, f, f.client())
}

func newTestMachineWith(t *testing.T, f *fakeCompute, cl *computeClient) (*Machine, string) {
	t.Helper()
	sr := newStateRoot(t)
	f.root = sr
	dir := filepath.Join(sr.dir, "state")
	m, err := create(dir, testConfig(), cl)
	sr.own(t, f, m, err)
	if err != nil {
		t.Fatal(err)
	}
	return m, dir
}

// reopen opens dir for f through open with f's configured client and
// registers its result.
func reopen(t *testing.T, f *fakeCompute, dir string) *Machine {
	t.Helper()
	m, err := open(dir, testConfig(), f.client())
	f.root.own(t, f, m, err)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func drive(t *testing.T, m *Machine, steps int) (Status, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return m.Drive(ctx, testTok(), DriveOptions{Steps: steps, Interval: time.Second})
}

// TestProductionClient inspects the fields of the production client. It
// sends nothing and does not by itself qualify the transport; see
// TestConfiguredClientExecutes.
func TestProductionClient(t *testing.T) {
	c := newComputeClient()
	if c.base != "https://compute.googleapis.com/compute/v1/" {
		t.Errorf("base = %q", c.base)
	}
	tr, ok := c.hc.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T", c.hc.Transport)
	}
	if tr.Proxy != nil || tr.TLSNextProto == nil || len(tr.TLSNextProto) != 0 || tr.ForceAttemptHTTP2 || !tr.DisableKeepAlives {
		t.Error("production transport may use a proxy, HTTP/2 or connection reuse")
	}
	if tr.TLSClientConfig.InsecureSkipVerify || tr.TLSClientConfig.RootCAs != nil || tr.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Error("production transport does not use system TLS verification")
	}
	if tr.ResponseHeaderTimeout == 0 || tr.TLSHandshakeTimeout == 0 {
		t.Error("production transport has unbounded waits")
	}
	if err := c.hc.CheckRedirect(nil, nil); err != http.ErrUseLastResponse {
		t.Errorf("CheckRedirect = %v, want ErrUseLastResponse", err)
	}
	sr := newStateRoot(t)
	m, err := Create(filepath.Join(sr.dir, "s"), testConfig())
	sr.own(t, nil, m, err)
	if err != nil {
		t.Fatal(err)
	}
	if m.client.base != computeBase || m.client.hc.Transport.(*http.Transport).TLSClientConfig.RootCAs != nil {
		t.Error("Create does not use the production client")
	}
}

// TestConfiguredClientExecutes runs a whole lifecycle through the client
// construction Create uses, with only the origin and trusted root set to
// the local fixture. Every request is HTTP/1.1 on its own connection and
// carries only the explicit token. The proxy variables are set as a
// tripwire, though ProxyFromEnvironment would skip a loopback origin
// anyway; TestProductionClient checks Proxy is nil.
func TestConfiguredClientExecutes(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:9") // discard port: a used proxy fails
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:9")
	f := newFake(t)
	m, _ := newTestMachine(t, f)
	if st, err := drive(t, m, 50); err != nil || !st.Reached {
		t.Fatalf("start = %+v, %v", st, err)
	}
	if err := m.SetGoal(Absent); err != nil {
		t.Fatal(err)
	}
	if st, err := drive(t, m, 50); err != nil || !st.Reached || st.InstanceState != stateAbsent || st.DiskState != stateAbsent {
		t.Fatalf("down = %+v, %v", st, err)
	}
	reqs := f.requests()
	for _, r := range reqs {
		if r.Proto != "HTTP/1.1" || r.Auth != "Bearer "+testToken {
			t.Errorf("%s %s: proto %s, auth %q", r.Method, r.Path, r.Proto, r.Auth)
		}
	}
	if n := int(f.conns.Load()); n != len(reqs) {
		t.Errorf("%d connections for %d requests; connections were reused", n, len(reqs))
	}
}

// TestConfiguredClientTrust shows the configured client refuses a server
// its roots do not trust: the system roots do not include the fixture.
func TestConfiguredClientTrust(t *testing.T) {
	f := newFake(t)
	cl := newConfiguredClient(f.srv.URL+"/compute/v1/", nil)
	tok := testTok()
	c, err := cl.start("projects.get", "GET", "projects/skygo-test", "", nil, &tok)
	if err != nil {
		t.Fatal(err)
	}
	<-c.done
	if c.err == nil || c.resp != nil {
		t.Fatalf("untrusted server answered: status %d", c.status)
	}
	if len(f.requests()) != 0 {
		t.Error("request reached an untrusted server")
	}
}

func TestReplyBounds(t *testing.T) {
	tests := []struct {
		name string
		h    http.HandlerFunc
	}{
		{"redirect", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/elsewhere", http.StatusFound) }},
		{"malformed", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"name":`) }},
		{"duplicate key", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"name":"a","name":"b"}`) }},
		{"escaped duplicate key", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"name":"a","n`+jsonEscape('a')+`me":"b"}`)
		}},
		{"case duplicate key", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"name":"a","NAME":"b"}`) }},
		{"trailing", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{} {}`) }},
		{"oversize", func(w http.ResponseWriter, r *http.Request) { w.Write(make([]byte, maxBodyBytes+1)) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var n atomic.Int32
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { n.Add(1); tt.h(w, r) }))
			t.Cleanup(srv.Close)
			roots := x509.NewCertPool()
			roots.AddCert(srv.Certificate())
			cl := newConfiguredClient(srv.URL+"/", roots)
			tok := testTok()
			c, err := cl.start("projects.get", "GET", "projects/x", "", nil, &tok)
			if err != nil {
				t.Fatal(err)
			}
			<-c.done
			var p projectWire
			err = c.readResult(&p)
			if err == nil {
				t.Fatal("readResult succeeded")
			}
			if strings.Contains(err.Error(), testToken) {
				t.Error("error carries the token")
			}
			if n.Load() != 1 {
				t.Errorf("server saw %d requests, want 1", n.Load())
			}
		})
	}
}

// jsonEscape returns the JSON \uXXXX escape of r, built at run time so the
// escaped alias is plainly visible in the tests that use it.
func jsonEscape(r rune) string {
	return fmt.Sprintf("%cu%04x", '\\', r)
}

// TestTransientKeepsCause shows a read's transport error stays reachable
// through the classified error.
func TestTransientKeepsCause(t *testing.T) {
	cause := errors.New("connection reset")
	c := &call{what: "disks.get", err: cause}
	err := c.readResult(new(diskWire))
	if !errors.Is(err, errTransient) || !errors.Is(err, cause) {
		t.Errorf("readResult = %v", err)
	}
}
