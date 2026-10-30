//go:build unix

package gce

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// targetReplies models only Compute's response projection. It uses the
// existing model TLS server and production client; no SSH code is linked.
// The original response is read and closed before replacing its JSON.
type targetReplies struct {
	rt    http.RoundTripper
	patch func(string, map[string]any)
}

func (r targetReplies) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.rt.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	data, err := io.ReadAll(resp.Body)
	cerr := resp.Body.Close()
	if err != nil || cerr != nil {
		return nil, errors.Join(err, cerr)
	}
	var v map[string]any
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	if strings.Contains(req.URL.Path, "/instances/") {
		if nics, ok := v["networkInterfaces"].([]any); ok && len(nics) == 1 {
			nics[0].(map[string]any)["networkIP"] = "10.42.0.8"
		}
	}
	if r.patch != nil {
		r.patch(req.URL.Path, v)
	}
	data, err = json.Marshal(v)
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(data))
	resp.ContentLength = int64(len(data))
	return resp, nil
}

func targetFixture(t *testing.T) (*Machine, *fakeCompute, string, Target) {
	t.Helper()
	f := newFake(t)
	m, dir := newTestMachine(t, f)
	if st, err := drive(t, m, 50); err != nil || !st.Reached {
		t.Fatalf("Drive: reached=%v err=%v", st.Reached, err)
	}
	m.client.hc.Transport = targetReplies{rt: m.client.hc.Transport}
	r := m.rec
	return m, f, dir, Target{ComputeProjectID: r.Config.ComputeProjectID, Zone: r.Config.Zone, InstanceID: r.InstanceID, DiskID: r.DiskID, ImageID: r.ImageID, Address: "10.42.0.8"}
}

func verifyTarget(t *testing.T, m *Machine, target Target) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return m.VerifyTarget(ctx, testTok(), target)
}

func TestVerifyTarget(t *testing.T) {
	m, f, dir, target := targetFixture(t)
	before, err := os.ReadFile(filepath.Join(dir, stateName))
	if err != nil {
		t.Fatal(err)
	}
	rec, _ := json.Marshal(m.rec)
	reqs := len(f.requests())
	if err := verifyTarget(t, m, target); err != nil {
		t.Fatal(err)
	}
	got := f.requests()[reqs:]
	wantPaths := []string{"projects/" + m.rec.Config.Project, m.instancePath(), m.diskPath()}
	if len(got) != len(wantPaths) {
		t.Fatalf("requests=%d want3", len(got))
	}
	for i, r := range got {
		if r.Method != "GET" || r.Path != wantPaths[i] || len(r.Body) != 0 || r.Query != "" || r.Auth != "Bearer "+testToken {
			t.Errorf("request %d differs from expected GET", i)
		}
	}
	after, _ := os.ReadFile(filepath.Join(dir, stateName))
	current, _ := json.Marshal(m.rec)
	if !bytes.Equal(before, after) || !bytes.Equal(rec, current) {
		t.Fatal("verification changed durable or observed lifecycle state")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m = reopen(t, f, dir)
	m.client.hc.Transport = targetReplies{rt: m.client.hc.Transport}
	if err := verifyTarget(t, m, target); err != nil {
		t.Fatal(err)
	}
	if err := m.SetGoal(Restarted); err != nil {
		t.Fatalf("SetGoal: %v", err)
	}
	if st, err := drive(t, m, 50); err != nil || !st.Reached {
		t.Fatalf("restart: %v", err)
	}
	if err := verifyTarget(t, m, target); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyTargetLocalRefusals(t *testing.T) {
	m, f, dir, target := targetFixture(t)
	original, _ := json.Marshal(m.rec)
	state, _ := os.ReadFile(filepath.Join(dir, stateName))
	for _, tc := range []struct {
		name string
		edit func(*record, *Target)
	}{
		{"project", func(r *record, x *Target) { x.ComputeProjectID = "999" }},
		{"zone", func(r *record, x *Target) { x.Zone = "us-central1-b" }},
		{"vm", func(r *record, x *Target) { x.InstanceID = "999" }},
		{"disk", func(r *record, x *Target) { x.DiskID = "999" }},
		{"image", func(r *record, x *Target) { x.ImageID = "999" }},
		{"public", func(r *record, x *Target) { x.Address = "8.8.8.8" }},
		{"loopback", func(r *record, x *Target) { x.Address = "127.0.0.1" }},
		{"ipv6", func(r *record, x *Target) { x.Address = "fd00::1" }},
		{"mapped", func(r *record, x *Target) { x.Address = "::ffff:10.42.0.8" }},
		{"hostname", func(r *record, x *Target) { x.Address = "host" }},
		{"port", func(r *record, x *Target) { x.Address = "10.42.0.8:22" }},
		{"noncanonical", func(r *record, x *Target) { x.Address = "010.42.0.8" }},
		{"noaddress", func(r *record, x *Target) { x.Address = "" }},
		{"unreached", func(r *record, x *Target) { r.Reached = false }},
		{"incomplete", func(r *record, x *Target) { r.CompletedIntent-- }},
		{"held", func(r *record, x *Target) { r.Held = heldBudget }},
		{"stopped", func(r *record, x *Target) { r.Desired = Stopped }},
		{"absent", func(r *record, x *Target) { r.Desired = Absent }},
		{"restartunfinished", func(r *record, x *Target) { r.Desired = Restarted; r.Restart = "start" }},
		{"phase", func(r *record, x *Target) { r.Phase = phaseImagePinned }},
		{"unadmitted", func(r *record, x *Target) { r.Admission = nil }},
		{"admissiondrift", func(r *record, x *Target) { r.Admission.ImageID = "999" }},
		{"diskwitness", func(r *record, x *Target) { r.DiskCreate = nil }},
		{"vmwitness", func(r *record, x *Target) { r.InstanceCreate = nil }},
		{"witnessdrift", func(r *record, x *Target) { r.InstanceCreate.TargetID = "999" }},
		{"prepared", func(r *record, x *Target) { r.Op = &operation{State: opPrepared} }},
		{"lost", func(r *record, x *Target) { r.Op = &operation{State: opLost} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var r record
			if err := json.Unmarshal(original, &r); err != nil {
				t.Fatal(err)
			}
			m.rec = &r
			x := target
			tc.edit(&r, &x)
			n := len(f.requests())
			if err := verifyTarget(t, m, x); err == nil {
				t.Fatal("accepted")
			}
			if len(f.requests()) != n {
				t.Fatal("GET before local admission")
			}
			after, _ := os.ReadFile(filepath.Join(dir, stateName))
			if !bytes.Equal(state, after) {
				t.Fatal("state changed")
			}
		})
	}
	if err := json.Unmarshal(original, &m.rec); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	cancel()
	for _, ctx := range []context.Context{context.Background(), ctx} {
		n := len(f.requests())
		if err := m.VerifyTarget(ctx, testTok(), target); err == nil {
			t.Fatal("accepted invalid context")
		}
		if len(f.requests()) != n {
			t.Fatal("sent request")
		}
	}
	tok := testTok()
	tok.RefreshToken = "private"
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.VerifyTarget(ctx, tok, target); err == nil {
		t.Fatal("refresh token accepted")
	}
}

func TestVerifyTargetRemoteRefusals(t *testing.T) {
	m, f, dir, target := targetFixture(t)
	base := m.client.hc.Transport.(targetReplies).rt
	state, _ := os.ReadFile(filepath.Join(dir, stateName))
	rec, _ := json.Marshal(m.rec)
	for _, tc := range []struct {
		name, kind, field string
		value             any
	}{
		{"projectid", "project", "id", "999"}, {"projectname", "project", "name", "other"},
		{"oslogin", "project", "commonInstanceMetadata", map[string]any{"items": []any{map[string]any{"key": "enable-oslogin", "value": "true"}}}},
		{"vmid", "instance", "id", "999"}, {"vmname", "instance", "name", "other"}, {"zone", "instance", "zone", "other"},
		{"state", "instance", "status", "TERMINATED"}, {"owner", "instance", "labels", map[string]any{}},
		{"metadata", "instance", "metadata", map[string]any{}}, {"serviceaccount", "instance", "serviceAccounts", []any{map[string]any{"email": "other"}}},
		{"privateip", "instance", "networkInterfaces", []any{map[string]any{"subnetwork": testConfig().Subnet, "networkIP": "10.42.0.9"}}},
		{"external", "instance", "networkInterfaces", []any{map[string]any{"subnetwork": testConfig().Subnet, "networkIP": target.Address, "accessConfigs": []any{map[string]any{}}}}},
		{"subnet", "instance", "networkInterfaces", []any{map[string]any{"subnetwork": "other", "networkIP": target.Address}}},
		{"missingnic", "instance", "networkInterfaces", []any{}},
		{"externalipv6", "instance", "networkInterfaces", []any{map[string]any{"subnetwork": testConfig().Subnet, "networkIP": target.Address, "ipv6AccessConfigs": []any{map[string]any{}}}}},
		{"ipv6type", "instance", "networkInterfaces", []any{map[string]any{"subnetwork": testConfig().Subnet, "networkIP": target.Address, "ipv6AccessType": "EXTERNAL"}}},
		{"ipv6address", "instance", "networkInterfaces", []any{map[string]any{"subnetwork": testConfig().Subnet, "networkIP": target.Address, "ipv6Address": "fd00::1"}}},
		{"dualstack", "instance", "networkInterfaces", []any{map[string]any{"subnetwork": testConfig().Subnet, "networkIP": target.Address, "stackType": "IPV4_IPV6"}}},
		{"diskid", "disk", "id", "999"}, {"diskname", "disk", "name", "other"}, {"diskimage", "disk", "sourceImageId", "999"},
		{"diskstate", "disk", "status", "CREATING"}, {"diskusers", "disk", "users", []any{}}, {"foreignuser", "disk", "users", []any{"other"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m.client.hc.Transport = targetReplies{rt: base, patch: func(path string, v map[string]any) {
				kind := "project"
				if strings.Contains(path, "/instances/") {
					kind = "instance"
				}
				if strings.Contains(path, "/disks/") {
					kind = "disk"
				}
				if kind == tc.kind {
					v[tc.field] = tc.value
				}
			}}
			n := len(f.requests())
			if err := verifyTarget(t, m, target); err == nil {
				t.Fatal("accepted remote drift")
			}
			for _, r := range f.requests()[n:] {
				if r.Method != "GET" {
					t.Fatal("mutation")
				}
			}
			after, _ := os.ReadFile(filepath.Join(dir, stateName))
			current, _ := json.Marshal(m.rec)
			if !bytes.Equal(state, after) || !bytes.Equal(rec, current) {
				t.Fatal("changed lifecycle state")
			}
		})
	}
}

func TestVerifyTargetOriginalCustody(t *testing.T) {
	m, f, dir, target := targetFixture(t)
	state, _ := os.ReadFile(filepath.Join(dir, stateName))
	sentinel := errors.New("modeled first Close failure")
	b := &bodyFault{rt: m.client.hc.Transport.(targetReplies).rt, method: "GET", suffix: "/" + m.rec.Config.Project, err: sentinel}
	m.client.hc.Transport = b
	if err := verifyTarget(t, m, target); !errors.Is(err, sentinel) {
		t.Fatalf("lost first Close cause: %v", err)
	}
	if m.held == nil || m.fault == nil || b.closes.Load() != 1 {
		t.Fatal("original not held")
	}
	n := len(f.requests())
	if err := verifyTarget(t, m, target); err == nil {
		t.Fatal("fenced machine accepted")
	}
	if err := m.Close(); !errors.Is(err, sentinel) {
		t.Fatal("Close lost original cause")
	}
	if len(f.requests()) != n || b.closes.Load() != 1 {
		t.Fatal("retried failed original")
	}
	after, _ := os.ReadFile(filepath.Join(dir, stateName))
	if !bytes.Equal(state, after) {
		t.Fatal("state changed")
	}
}

func ExampleTarget() {
	target := Target{ComputeProjectID: "123456789012", Zone: "us-central1-a", InstanceID: "9001", DiskID: "9002", ImageID: "4242", Address: "10.42.0.8"}
	fmt.Println(target.Address)
	// Output: 10.42.0.8
}

func ExampleMachine_VerifyTarget() {
	var m Machine
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fmt.Println(m.VerifyTarget(ctx, testTok(), Target{}))
	// Output: gce: machine owns no state
}

func TestVerifyTargetPendingOriginal(t *testing.T) {
	m, f, dir, target := targetFixture(t)
	state, _ := os.ReadFile(filepath.Join(dir, stateName))
	// This modeled body Read ignores cancellation until release, so VerifyTarget
	// must retain the actual production client's original call across its deadline.
	release := f.holdReplies()
	b := &bodyFault{rt: m.client.hc.Transport.(targetReplies).rt, method: "GET", suffix: "/" + m.rec.Config.Project, readHold: release}
	m.client.hc.Transport = b
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := m.VerifyTarget(ctx, testTok(), target); err == nil {
		t.Fatal("pending accepted")
	}
	c := m.pending
	if c == nil || c.settled() {
		t.Fatal("original not retained")
	}
	n := len(f.requests())
	if err := verifyTarget(t, m, target); err == nil {
		t.Fatal("admitted with original pending")
	}
	if len(f.requests()) != n {
		t.Fatal("overlapped original")
	}
	checkFencedPending(t, f, m, dir)
	f.release()
	<-c.done
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, stateName))
	if !bytes.Equal(state, after) {
		t.Fatal("state changed")
	}
}

func TestVerifyTargetHTTPRefusals(t *testing.T) {
	m, f, dir, target := targetFixture(t)
	state, _ := os.ReadFile(filepath.Join(dir, stateName))
	for _, code := range []int{401, 403, 404, 429, 503} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			f.mu.Lock()
			f.getStatus = code
			f.mu.Unlock()
			n := len(f.requests())
			if err := verifyTarget(t, m, target); err == nil || strings.Contains(err.Error(), secretText) {
				t.Fatal("missing or unsafe refusal")
			}
			if len(f.requests()) != n+1 || m.pending != nil {
				t.Fatal("retry or unsettled original")
			}
			after, _ := os.ReadFile(filepath.Join(dir, stateName))
			if !bytes.Equal(state, after) {
				t.Fatal("state changed")
			}
		})
	}
}

// Each malformed reply below is written verbatim by a real loopback TLS
// server. In particular no map decoder may normalize its duplicate key
// before the production client's strict JSON parser sees the bytes.
func TestVerifyTargetRawReplyErrors(t *testing.T) {
	m, f, dir, target := targetFixture(t)
	before, _ := os.ReadFile(filepath.Join(dir, stateName))
	rec, _ := json.Marshal(m.rec)
	paths := []string{"projects/" + m.rec.Config.Project, m.instancePath(), m.diskPath()}
	for i, stage := range []string{"project", "instance", "disk"} {
		t.Run(stage, func(t *testing.T) {
			for _, raw := range []struct{ name, body string }{
				{"duplicate", `{"provider-private-marker":0,"provider-private-marker":1}`},
				{"malformed", `{"provider-private-marker":`},
			} {
				t.Run(raw.name, func(t *testing.T) {
					var requests atomic.Int32
					srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						requests.Add(1)
						if req.Method != "GET" || req.URL.RawQuery != "" {
							t.Error("unexpected mutation or query")
						}
						if req.Header.Get("Authorization") != "Bearer "+testToken {
							t.Error("unexpected credential")
						}
						path := strings.TrimPrefix(req.URL.Path, "/compute/v1/")
						if path == paths[i] {
							w.Header().Set("Content-Type", "application/json")
							w.WriteHeader(http.StatusOK)
							io.WriteString(w, raw.body)
							return
						}
						// Only preceding successful replies use the ordinary model. The
						// selected raw failure is never decoded or remarshaled here.
						rr := httptest.NewRecorder()
						f.serve(rr, req)
						var v map[string]any
						if err := json.Unmarshal(rr.Body.Bytes(), &v); err != nil {
							t.Error(err)
							return
						}
						if strings.Contains(path, "/instances/") {
							v["networkInterfaces"].([]any)[0].(map[string]any)["networkIP"] = target.Address
						}
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(rr.Code)
						json.NewEncoder(w).Encode(v)
					}))
					defer srv.Close()
					roots := x509.NewCertPool()
					roots.AddCert(srv.Certificate())
					m.client = newConfiguredClient(srv.URL+"/compute/v1/", roots)
					err := verifyTarget(t, m, target)
					want := "gce: verify target " + stage + " failed"
					if err == nil || err.Error() != want || strings.Contains(err.Error(), "provider-private-marker") {
						t.Fatalf("error=%v want fixed stage failure", err)
					}
					var ce *causeError
					if !errors.As(err, &ce) || len(ce.causes) != 1 || !errors.Is(err, ce.causes[0]) {
						t.Fatal("original parse cause not reachable")
					}
					if raw.name == "duplicate" && !strings.Contains(ce.causes[0].Error(), "provider-private-marker") {
						t.Fatal("raw duplicate did not reach original strict parser")
					}
					if requests.Load() != int32(i+1) || m.pending != nil || m.held != nil || m.fault != nil {
						t.Fatal("unexpected retry, stage advance or unsettled original")
					}
					after, _ := os.ReadFile(filepath.Join(dir, stateName))
					current, _ := json.Marshal(m.rec)
					if !bytes.Equal(before, after) || !bytes.Equal(rec, current) {
						t.Fatal("verification changed lifecycle state")
					}
				})
			}
		})
	}
}
