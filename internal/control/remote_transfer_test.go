//go:build darwin || linux

package control

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

func TestRemoteTransferCanonicalToken(t *testing.T) {
	key := store.RemoteKey{JobID: strings.Repeat("a", 32), AttemptID: strings.Repeat("b", 32)}
	raw := make([]byte, 32)
	token, err := remoteToken(key, raw)
	if err != nil || len(token) != 114 {
		t.Fatalf("canonical token length=%d err=%v", len(token), err)
	}
	got, decoded, err := parseRemoteToken(string(token))
	if err != nil || got != key || !bytes.Equal(decoded, raw) {
		t.Fatalf("canonical token roundtrip: %v", err)
	}
	clear(decoded)
	if remoteTokenHash(store.RemoteInput, raw) == remoteTokenHash(store.RemoteOutput, raw) {
		t.Fatal("scope purposes share a token digest")
	}
	noncanonical := bytes.Clone(token)
	noncanonical[len(noncanonical)-1] = 'B' // Nonzero unused base64 bits.
	wrongSeparator := bytes.Clone(token)
	wrongSeparator[37] = '-'
	badID := bytes.Clone(token)
	badID[5] = 'A'
	badAlphabet := bytes.Clone(token)
	badAlphabet[71] = '+'
	for name, value := range map[string]string{
		"empty": "", "short": string(token[:113]), "long": string(token) + "x",
		"wrong-version": "sgd2." + string(token[5:]), "wrong-separator": string(wrongSeparator),
		"uppercase-id": string(badID), "wrong-alphabet": string(badAlphabet),
		"noncanonical-pad-bits": string(noncanonical), "padding": string(token[:113]) + "=",
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := parseRemoteToken(value); !errors.Is(err, ErrInvalid) {
				t.Fatalf("malformed token error=%v", err)
			}
		})
	}
	if _, err := remoteToken(key, raw[:31]); !errors.Is(err, ErrInvalid) {
		t.Fatal("wrong secret length accepted")
	}
}

// This creates real authenticated Store metadata and a real published input.
// Only the explicit native creation values are MODELED; no native function,
// Pod identity qualification, helper, UID deletion or stop proof is implied.
func remoteTransferAttempt(t *testing.T, f *dataControlFixture, id string, ref artifact.Ref, outputs bool) (store.RemoteKey, string) {
	t.Helper()
	authority, err := f.engine.Delegate(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	target := f.target()
	policy := backend.DefaultDataPolicy()
	now := time.Now().UTC()
	task := backend.Task{Argv: []string{"reader"}, Image: "modeled-image", Workdir: &ref}
	if outputs {
		task.Outputs = []string{"result"}
	}
	job, err := f.store.SubmitData(f.actor, nil, id, task, target.Name, target.Binding, authority, policy, now)
	if err != nil {
		t.Fatal(err)
	}
	secret := sha256.Sum256([]byte(id))
	hashes := store.RemoteHashes{Input: remoteTokenHash(store.RemoteInput, secret[:])}
	if outputs {
		outputSecret := sha256.Sum256([]byte(id + ":output"))
		hashes.Output = remoteTokenHash(store.RemoteOutput, outputSecret[:])
		clear(outputSecret[:])
	}
	start, began, err := f.store.BeginRemoteAttempt(job.ID, job.CurrentAttemptID(), hashes, policy, now)
	if err != nil || !began {
		t.Fatalf("original start intent: began=%v err=%v", began, err)
	}
	key := store.RemoteKey{JobID: job.ID, AttemptID: job.CurrentAttemptID()}
	created := backend.DataCreated{Instance: "modeled-pod-" + id, Pod: backend.DataResource{Name: "modeled-pod", UID: "modeled-pod-" + id}, InputSecret: backend.DataResource{Name: "modeled-input", UID: "modeled-input-" + id}}
	if outputs {
		created.OutputSecret = backend.DataResource{Name: "modeled-output", UID: "modeled-output-" + id}
	}
	if _, err = f.store.RecordRemoteCreated(key, start.Revision, store.RemoteCreation{Succeeded: true, Resources: created}, now); err != nil {
		t.Fatal(err)
	}
	token, err := remoteToken(key, secret[:])
	clear(secret[:])
	if err != nil {
		t.Fatal(err)
	}
	return key, string(token)
}

func TestRemoteAuthorizeRefusalDoesNotAttachOrRevokeForeignHash(t *testing.T) {
	f := newDataControlFixture(t)
	ref, _ := f.input(t)
	key, token := remoteTransferAttempt(t, f, "foreign-hash", ref, true)
	original, err := f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for actual file-engine observation of revocation. A foreign token must
	// still fail before authority-side revocation can alter this attempt.
	f.prepareRevocation(t)
	changed := f.engine.Changed()
	if err = auth.Revoke(f.configPath, f.actor.TokenID()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
	case <-time.After(5 * time.Second):
		f.hold(errors.New("original auth refresh did not observe revocation"))
		t.Fatal("auth refresh remains held")
	}
	foreignKey, raw, err := parseRemoteToken(token)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] ^= 1
	foreign, err := remoteToken(foreignKey, raw)
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	call, err := f.control.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	if err = f.control.AuthorizeRemoteCall(context.Background(), call, string(foreign), RemoteReadInput); err == nil {
		t.Fatal("foreign hash authorized")
	}
	s := f.control.artifacts
	s.mu.Lock()
	attached := call.worker != nil
	s.mu.Unlock()
	if attached {
		f.hold(errors.New("refused foreign credential acquired a worker owner"))
		t.Fatal("foreign credential attached")
	}
	call.Done()
	after, err := f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, original) {
		t.Fatal("foreign hash revoked or mutated another original attempt")
	}
	if f.backend.native.Load() != 0 {
		t.Fatal("credential refusal invoked native work")
	}
}

// lateCancelContext is a real cancelable context. Its first Err call reports
// the context's state and then cancels it, so cancellation lands after
// remoteCredential's initial check and before its Store and authority
// checks. This MODELS the scheduling of an HTTP disconnect or deadline; it is
// not native network evidence.
type lateCancelContext struct {
	context.Context
	cancel context.CancelFunc
	calls  atomic.Int32
}

func newLateCancelContext() *lateCancelContext {
	ctx, cancel := context.WithCancel(context.Background())
	return &lateCancelContext{Context: ctx, cancel: cancel}
}

func (c *lateCancelContext) Err() error {
	err := c.Context.Err()
	if c.calls.Add(1) == 1 {
		c.cancel()
	}
	return err
}

// stateDigest hashes every file of the fixture's Store directory.
func stateDigest(t *testing.T, f *dataControlFixture) map[string][32]byte {
	t.Helper()
	out := map[string][32]byte{}
	root := filepath.Join(f.directory, "state")
	err := filepath.WalkDir(root, func(name string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		out[name[len(root):]] = sha256.Sum256(data)
		return nil
	})
	if err != nil || len(out) == 0 {
		t.Fatalf("state digest: %d files, %v", len(out), err)
	}
	return out
}

// remoteOutputToken returns the output-scope token of a remoteTransferAttempt.
func remoteOutputToken(t *testing.T, key store.RemoteKey, id string) string {
	t.Helper()
	secret := sha256.Sum256([]byte(id + ":output"))
	token, err := remoteToken(key, secret[:])
	clear(secret[:])
	if err != nil {
		t.Fatal(err)
	}
	return string(token)
}

// A valid request canceled after its initial check is refused without
// changing the canonical Store or either scope. ReadInput and PollRelease are
// the operations that pass every Store check in this attempt state, one per
// scope, so both reach the final recheck.
func TestRemoteAuthorizeLateCancellationDoesNotRevoke(t *testing.T) {
	f := newDataControlFixture(t)
	ref, _ := f.input(t)
	const id = "late-cancel"
	key, input := remoteTransferAttempt(t, f, id, ref, true)
	output := remoteOutputToken(t, key, id)
	for _, tc := range []struct {
		name, token string
		op          RemoteOperation
	}{
		{"read-input", input, RemoteReadInput},
		{"poll-release", output, RemotePollRelease},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			before := stateDigest(t, f)
			call, err := f.control.TryArtifactCall()
			if err != nil {
				t.Fatal(err)
			}
			ctx := newLateCancelContext()
			err = f.control.AuthorizeRemoteCall(ctx, call, tc.token, tc.op)
			s := f.control.artifacts
			s.mu.Lock()
			attached := call.worker != nil
			s.mu.Unlock()
			if !attached {
				call.Done()
			}
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("late-canceled request: %v", err)
			}
			if ctx.calls.Load() != 2 {
				t.Fatalf("context checked %d times, want initial and final", ctx.calls.Load())
			}
			if attached {
				t.Fatal("canceled request attached a permit")
			}
			after, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, original) || after.Active.ClosedReason != 0 {
				t.Fatalf("cancellation changed the attempt: closed=%v", after.Active.ClosedReason)
			}
			if !reflect.DeepEqual(stateDigest(t, f), before) {
				t.Fatal("cancellation changed the canonical Store")
			}
			// Both scopes remain usable by a live request.
			if _, _, err := f.control.remoteCredential(context.Background(), key, after.Active.Input.Hash, RemoteReadInput); err != nil {
				t.Fatalf("input scope unusable after cancellation: %v", err)
			}
			if _, _, err := f.control.remoteCredential(context.Background(), key, after.Active.Output.Hash, RemotePollRelease); err != nil {
				t.Fatalf("output scope unusable after cancellation: %v", err)
			}
			if f.backend.native.Load() != 0 {
				t.Fatal("canceled request invoked native work")
			}
		})
	}
}

// Lost delegation still closes the matching scope, with or without a
// canceled request. A foreign or misdirected token closes nothing.
func TestRemoteLostDelegationRevokesDespiteCancellation(t *testing.T) {
	f := newDataControlFixture(t)
	ref, _ := f.input(t)
	key, input := remoteTransferAttempt(t, f, "lost-delegation", ref, true)
	other, _ := remoteTransferAttempt(t, f, "bystander", ref, true)
	f.prepareRevocation(t)
	changed := f.engine.Changed()
	if err := auth.Revoke(f.configPath, f.actor.TokenID()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
	case <-time.After(5 * time.Second):
		f.hold(errors.New("original auth refresh did not observe revocation"))
		t.Fatal("auth refresh remains held")
	}
	bystander, err := f.store.GetRemoteAttempt(other)
	if err != nil {
		t.Fatal(err)
	}
	// Foreign selectors: another attempt's key with this secret, a flipped
	// secret, and a token used for the other scope's purpose.
	_, raw, err := parseRemoteToken(input)
	if err != nil {
		t.Fatal(err)
	}
	crossed, err := remoteToken(other, raw)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] ^= 1
	flipped, err := remoteToken(key, raw)
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	original, err := f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, token string
		op          RemoteOperation
	}{
		{"crossed-key", string(crossed), RemoteReadInput},
		{"flipped-secret", string(flipped), RemoteReadInput},
		{"wrong-purpose", input, RemoteBeginOutput},
	} {
		call, err := f.control.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		if err = f.control.AuthorizeRemoteCall(newLateCancelContext(), call, tc.token, tc.op); err == nil {
			t.Fatalf("%s authorized", tc.name)
		}
		call.Done()
		for k, want := range map[store.RemoteKey]store.RemoteAttempt{key: original, other: bystander} {
			if got, err := f.store.GetRemoteAttempt(k); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("%s changed attempt %s: %v", tc.name, k.AttemptID, err)
			}
		}
	}
	call, err := f.control.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	if err = f.control.AuthorizeRemoteCall(newLateCancelContext(), call, input, RemoteReadInput); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("lost delegation authorized: %v", err)
	}
	call.Done()
	row, err := f.store.GetRemoteAttempt(key)
	if err != nil || row.Active.ClosedReason != store.RemoteAuthorityLost {
		t.Fatalf("lost delegation left scope open: %+v %v", row.Active, err)
	}
	if got, err := f.store.GetRemoteAttempt(other); err != nil || !reflect.DeepEqual(got, bystander) {
		t.Fatal("revocation reached a bystander attempt")
	}
	if f.backend.native.Load() != 0 {
		t.Fatal("revocation invoked native work")
	}
}

// The final recheck itself: cancellation refuses without change; lost
// delegation closes the scope even when the request is also canceled.
func TestRemoteRecheckSeparatesCancellationFromLostAuthority(t *testing.T) {
	f := newDataControlFixture(t)
	ref, _ := f.input(t)
	key, _ := remoteTransferAttempt(t, f, "recheck", ref, true)
	row, err := f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	before := stateDigest(t, f)
	if err = f.control.recheckRemote(canceled, key, row.Active.Input.Hash, store.RemoteInput, row.Authority); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("canceled recheck: %v", err)
	}
	if after, err := f.store.GetRemoteAttempt(key); err != nil || !reflect.DeepEqual(after, row) || !reflect.DeepEqual(stateDigest(t, f), before) {
		t.Fatal("canceled recheck changed the Store")
	}
	if err = f.control.recheckRemote(context.Background(), key, row.Active.Input.Hash, store.RemoteInput, row.Authority); err != nil {
		t.Fatalf("live recheck: %v", err)
	}
	f.prepareRevocation(t)
	changed := f.engine.Changed()
	if err = auth.Revoke(f.configPath, f.actor.TokenID()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
	case <-time.After(5 * time.Second):
		f.hold(errors.New("original auth refresh did not observe revocation"))
		t.Fatal("auth refresh remains held")
	}
	if err = f.control.recheckRemote(canceled, key, row.Active.Input.Hash, store.RemoteInput, row.Authority); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("lost-authority recheck: %v", err)
	}
	after, err := f.store.GetRemoteAttempt(key)
	if err != nil || after.Active.ClosedReason != store.RemoteAuthorityLost {
		t.Fatalf("lost delegation under cancellation left scope open: %+v %v", after.Active, err)
	}
}

func TestRemoteAuthorizePreAttachAndEarlyDone(t *testing.T) {
	f := newDataControlFixture(t)
	ref, _ := f.input(t)
	key, token := remoteTransferAttempt(t, f, "call-custody", ref, true)
	for _, tc := range []struct {
		name, token string
		op          RemoteOperation
	}{
		{"malformed", "sgd1", RemoteReadInput}, {"wrong-purpose", token, RemotePollRelease}, {"invalid-operation", token, RemoteOperation(99)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			call, err := f.control.TryArtifactCall()
			if err != nil {
				t.Fatal(err)
			}
			if err = f.control.AuthorizeRemoteCall(context.Background(), call, tc.token, tc.op); err == nil {
				t.Fatal("refused credential attached")
			}
			s := f.control.artifacts
			s.mu.Lock()
			p := call.worker
			s.mu.Unlock()
			if p != nil {
				f.hold(errors.New("refused authorization retained unexpected worker"))
				t.Fatal("worker attached before authorization")
			}
			call.Done()
		})
	}
	call, err := f.control.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	if err = f.control.AuthorizeRemoteCall(context.Background(), call, token, RemoteReadInput); err != nil {
		t.Fatal(err)
	}
	call.Done() // Deliberately early: this is not a response/reader completion.
	s := f.control.artifacts
	s.mu.Lock()
	_, kept := s.calls[call]
	pending := call.worker != nil && call.worker.pending
	s.mu.Unlock()
	if !kept || !pending {
		t.Fatal("early Done erased attached original worker")
	}
	if ready, err := f.control.remoteTransfersPrepared(); err != nil || ready {
		t.Fatalf("attached pending call prepared: %v %v", ready, err)
	}
	_, generated := skyv1connect.NewRemoteDataServiceHandler(remoteHTTPControlService{f.control}, connect.WithCompressMinBytes(1<<20), connect.WithReadMaxBytes(96<<10), connect.WithSendMaxBytes(96<<10))
	// The exact invoker observes its own refusal and closes its actual request
	// body. It cannot invoke ReadRemoteInput after early Done or invent INPUT4.
	request := httptest.NewRequest("POST", skyv1connect.RemoteDataServiceReadInputProcedure, strings.NewReader(""))
	request = request.Clone(context.WithValue(request.Context(), remoteHTTPTestCallKey{}, call))
	if err = f.control.ServeRemoteHTTP(call, httptest.NewRecorder(), request, generated); err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	s.mu.Lock()
	_, kept = s.calls[call]
	s.mu.Unlock()
	if kept {
		f.hold(errors.New("original refusal response did not retire attached call"))
		t.Fatal("refusal owner remains held")
	}
	row, err := f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	if row.Active.Input.State != 2 {
		t.Fatal("early Done/refusal manufactured normal input completion")
	}
}

// Cleanup observes preparation, drains with the original Run still alive, then
// cancels and positively joins that exact Run. A failure retains the fixture and
// leaves original resources available; context cancellation is never a join.
func remoteTransferRetireRun(t *testing.T, f *dataControlFixture, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	if err := f.control.BeginShutdown(); err != nil {
		f.hold(err)
		t.Error(err)
		return
	}
	prepared, end := context.WithTimeout(context.Background(), 15*time.Second)
	err := f.control.WaitShutdownPrepared(prepared)
	end()
	if err != nil {
		f.hold(err)
		t.Errorf("retain original Run/resources %s: %v", f.directory, err)
		return
	}
	drain, stop := context.WithTimeout(context.Background(), 10*time.Second)
	err = f.control.DrainArtifacts(drain)
	stop()
	if err != nil {
		f.hold(err)
		t.Error(err)
		return
	}
	cancel()
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) {
			failure := fmt.Errorf("unexpected original Run return: %v", err)
			f.hold(failure)
			t.Error(failure)
			return
		}
	case <-time.After(5 * time.Second):
		f.hold(errors.New("original remote transfer Run did not return"))
		t.Error("original Run remains held")
		return
	}
	join, end := context.WithTimeout(context.Background(), 5*time.Second)
	defer end()
	if err = f.control.Join(join); err != nil {
		f.hold(err)
		t.Error(err)
	}
}

type remoteTransferHTTPResult struct {
	call      *ArtifactCall
	finish    *remoteInputFinish
	reader    *artifact.Reader
	published int64
	encoding  string
	err       error
}

func TestRemoteGeneratedFullInputFourCommonSlotsMaintenanceCloses(t *testing.T) {
	for _, jsonCodec := range []bool{false, true} {
		t.Run(map[bool]string{false: "proto", true: "json"}[jsonCodec], func(t *testing.T) {
			f := newDataControlFixture(t)
			ref, archive := f.input(t)
			keys := make([]store.RemoteKey, 4)
			tokens := make([]string, 4)
			for i := range keys {
				keys[i], tokens[i] = remoteTransferAttempt(t, f, fmt.Sprintf("full-%d", i), ref, false)
			}
			_, generated := skyv1connect.NewRemoteDataServiceHandler(remoteHTTPControlService{f.control}, connect.WithCompressMinBytes(1<<20), connect.WithReadMaxBytes(96<<10), connect.WithSendMaxBytes(96<<10))
			registered := make(chan *ArtifactCall, 4)
			joined := make(chan remoteTransferHTTPResult, 4)
			release := make(chan struct{})
			releaseOriginals := sync.OnceFunc(func() { close(release) })
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call, err := f.control.TryArtifactCall()
				if err != nil {
					joined <- remoteTransferHTTPResult{err: err}
					return
				}
				token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				if err = f.control.AuthorizeRemoteCall(r.Context(), call, token, RemoteReadInput); err != nil {
					if closeErr := r.Body.Close(); closeErr != nil {
						joined <- remoteTransferHTTPResult{call: call, err: closeErr}
						return
					}
					call.Done()
					joined <- remoteTransferHTTPResult{call: call, err: err}
					return
				}
				registered <- call
				<-release // This actual incoming HTTP frame retains its original call.
				request := r.Clone(context.WithValue(r.Context(), remoteHTTPTestCallKey{}, call))
				err = f.control.ServeRemoteHTTP(call, w, request, generated)
				result := remoteTransferHTTPResult{call: call, err: err}
				s := f.control.artifacts
				s.mu.Lock()
				if p := call.worker; p != nil {
					result.finish = p.input
					if p.response != nil {
						result.encoding = p.response.contentEncoding
					}
					if p.input != nil {
						result.reader = p.input.download.reader
						result.published = p.input.download.remote.published
					}
				}
				s.mu.Unlock()
				if err == nil {
					call.Done()
				}
				joined <- result
			}))
			closeServer := sync.OnceFunc(server.Close)
			var clients sync.WaitGroup
			clientResults := make(chan error, 4)
			options := []connect.ClientOption{}
			if jsonCodec {
				options = append(options, connect.WithProtoJSON())
			}
			client := skyv1connect.NewRemoteDataServiceClient(server.Client(), server.URL, options...)
			contexts := make([]context.Context, 4)
			stops := make([]context.CancelFunc, 4)
			for i := range keys {
				contexts[i], stops[i] = context.WithTimeout(context.Background(), 10*time.Second)
			}
			for i := range keys {
				clients.Add(1)
				go func(i int) {
					defer clients.Done()
					ctx := contexts[i]
					request := connect.NewRequest(&skyv1.ReadInputRequest{Limit: 64 << 10})
					request.Header().Set("Authorization", "Bearer "+tokens[i])
					response, err := client.ReadInput(ctx, request)
					if err == nil && (!bytes.Equal(response.Msg.Data, archive) || response.Msg.NextOffset != ref.Size || !response.Msg.Complete || response.Msg.Reference == nil || response.Msg.Reference.Id != ref.ID || response.Msg.Reference.Sha256 != ref.SHA256 || response.Msg.Reference.Size != ref.Size) {
						err = errors.New("generated full response differs from actual input")
					}
					clientResults <- err
				}(i)
			}
			clientJoined := make(chan struct{})
			go func() { clients.Wait(); close(clientJoined) }()
			t.Cleanup(func() {
				releaseOriginals()
				select {
				case <-clientJoined:
				case <-time.After(15 * time.Second):
					f.hold(errors.New("original generated client calls did not join"))
					t.Error("generated clients remain held")
					return
				}
				closeServer()
				for _, stop := range stops {
					stop()
				}
			})
			for range keys {
				waitValue(t, registered)
			}
			if fifth, err := f.control.TryArtifactCall(); !errors.Is(err, ErrArtifactBusy) {
				if fifth != nil {
					fifth.Done()
				}
				t.Fatalf("fifth common slot admitted: %v", err)
			}
			if ready, err := f.control.remoteTransfersPrepared(); err != nil || ready {
				t.Fatalf("registered actual calls prepared: %v %v", ready, err)
			}
			releaseOriginals()
			frames := make([]remoteTransferHTTPResult, 0, 4)
			for range keys {
				if err := waitValue(t, clientResults); err != nil {
					t.Fatal(err)
				}
				result := waitValue(t, joined)
				if result.err != nil {
					f.hold(result.err)
					t.Fatal(result.err)
				}
				frames = append(frames, result)
			}
			<-clientJoined
			closeServer()
			for _, stop := range stops {
				stop()
			} // Join actual external HTTP callers before shutdown.
			if remoteResponseLimit != 96<<10 {
				t.Fatal("fixed identity response bound changed")
			}
			for _, result := range frames {
				if result.finish == nil || !result.finish.complete || result.reader == nil || result.published != ref.Size || result.encoding != "" {
					f.hold(errors.New("actual generated full response lacks original finish frame"))
					t.Fatal("full response was not armed by actual producer")
				}
			}
			if fifth, err := f.control.TryArtifactCall(); !errors.Is(err, ErrArtifactBusy) {
				if fifth != nil {
					fifth.Done()
				}
				t.Fatalf("full responses released slots before original reader close: %v", err)
			}
			if ready, err := f.control.remoteTransfersPrepared(); err != nil || ready {
				t.Fatalf("registered full input became prepared: %v %v", ready, err)
			}
			for _, key := range keys {
				row, err := f.store.GetRemoteAttempt(key)
				if err != nil {
					t.Fatal(err)
				}
				if row.Active.Input.State != 2 {
					t.Fatal("wire end flag manufactured INPUT4 before original close")
				}
			}
			// Configure the original target before Run. A healthy borrowed workload
			// reports MODELED staging, not an invented main stop or default refusal.
			f.backend.observeData = func(_ context.Context, ref backend.Reference) (backend.DataObservation, error) {
				return backend.DataObservation{Status: backend.Status{State: backend.Starting, Instance: ref.Instance}, Phase: backend.DataStage}, nil
			}
			if err := f.control.BeginShutdown(); err != nil {
				t.Fatal(err)
			}
			cancel, done := beginRun(t, f.control)
			t.Cleanup(func() { remoteTransferRetireRun(t, f, cancel, done) })
			prepared, end := context.WithTimeout(context.Background(), 15*time.Second)
			err := f.control.WaitShutdownPrepared(prepared)
			end()
			if err != nil {
				f.hold(err)
				t.Fatal(err)
			}
			for _, key := range keys {
				row, err := f.store.GetRemoteAttempt(key)
				if err != nil {
					t.Fatal(err)
				}
				if row.Active == nil || row.Active.Input == nil || row.Active.Input.State != 4 {
					t.Fatal("same original maintenance close did not commit INPUT4")
				}
			}
			s := f.control.artifacts
			s.mu.Lock()
			for _, result := range frames {
				frame := result.finish.close
				if frame == nil || frame.remote != result.finish || frame.reader != result.reader || frame.neverOpened || frame.err != nil || !result.finish.download.finished {
					s.mu.Unlock()
					t.Fatal("maintenance lost the same original opened descriptor/closure frame")
				}
			}
			calls, downloads := len(s.calls), len(s.downloads)
			s.mu.Unlock()
			if calls != 0 || downloads != 0 {
				t.Fatalf("original full owners remain: calls=%d downloads=%d", calls, downloads)
			}
		})
	}
}

func TestRemoteGeneratedPartialPreparedExceptionIsNotFullCompletion(t *testing.T) {
	f := newDataControlFixture(t)
	ref, archive := f.input(t)
	key, token := remoteTransferAttempt(t, f, "partial-prepared", ref, false)
	_, generated := skyv1connect.NewRemoteDataServiceHandler(remoteHTTPControlService{f.control}, connect.WithCompressMinBytes(1<<20), connect.WithReadMaxBytes(96<<10), connect.WithSendMaxBytes(96<<10))
	joined := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call, err := f.control.TryArtifactCall()
		if err != nil {
			joined <- err
			return
		}
		if err = f.control.AuthorizeRemoteCall(r.Context(), call, token, RemoteReadInput); err != nil {
			if closeErr := r.Body.Close(); closeErr != nil {
				joined <- closeErr
				return
			}
			call.Done()
			joined <- err
			return
		}
		request := r.Clone(context.WithValue(r.Context(), remoteHTTPTestCallKey{}, call))
		err = f.control.ServeRemoteHTTP(call, w, request, generated)
		if err == nil {
			call.Done()
		}
		joined <- err
	}))
	closeServer := sync.OnceFunc(server.Close)
	t.Cleanup(closeServer)
	client := skyv1connect.NewRemoteDataServiceClient(server.Client(), server.URL)
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	response, err := client.ReadInput(ctx, connect.NewRequest(&skyv1.ReadInputRequest{Limit: 128}))
	producerErr := waitValue(t, joined)
	closeServer()
	stop()
	if err != nil || producerErr != nil {
		failure := errors.Join(err, producerErr)
		f.hold(failure)
		t.Fatal(failure)
	}
	if !bytes.Equal(response.Msg.Data, archive[:128]) || response.Msg.Complete || response.Msg.NextOffset != 128 {
		t.Fatal("partial generated response differs from admitted actual range")
	}
	s := f.control.artifacts
	s.mu.Lock()
	var original *ArtifactDownload
	for d := range s.downloads {
		if d.remote != nil && d.remote.key == key {
			original = d
		}
	}
	valid := original != nil && original.reader != nil && original.owners == 0 && !original.closing && !original.settling && !original.finished && original.remote.call == nil && original.remote.finish == nil && original.remote.published == 128
	calls := len(s.calls)
	s.mu.Unlock()
	if !valid || calls != 0 {
		f.hold(errors.New("actual partial response did not detach to an untouched cached original reader"))
		t.Fatal("partial exception lacks original reader custody")
	}
	if ready, err := f.control.remoteTransfersPrepared(); err != nil || !ready {
		t.Fatalf("untouched partial reader not eligible for preparation: %v %v", ready, err)
	}
	if settled, err := f.control.remoteAttemptTransfersSettled(key); err != nil || settled {
		t.Fatalf("partial exception authorized strict completion: %v %v", settled, err)
	}
	f.backend.observeData = func(_ context.Context, ref backend.Reference) (backend.DataObservation, error) {
		return backend.DataObservation{Status: backend.Status{State: backend.Starting, Instance: ref.Instance}, Phase: backend.DataStage}, nil
	}
	if err = f.control.BeginShutdown(); err != nil {
		t.Fatal(err)
	}
	cancel, done := beginRun(t, f.control)
	t.Cleanup(func() { remoteTransferRetireRun(t, f, cancel, done) })
	prepared, end := context.WithTimeout(context.Background(), 10*time.Second)
	err = f.control.WaitShutdownPrepared(prepared)
	end()
	if err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	row, err := f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	if row.Active.Input.State != 2 {
		t.Fatal("Prepared partial reader invented normally complete input")
	}
	drain, end := context.WithTimeout(context.Background(), 10*time.Second)
	err = f.control.DrainArtifacts(drain)
	end()
	if err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	row, err = f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	if row.Active.Input.State != 2 {
		t.Fatal("partial original close invented INPUT4")
	}
	s.mu.Lock()
	finished := original.finished
	_, present := s.downloads[original]
	s.mu.Unlock()
	if !finished || present {
		t.Fatal("Drain did not positively retire the same original partial reader")
	}
}

func ExampleRemoteOperation() {
	fmt.Println(RemoteReadInput == RemoteWriteOutput)
	// Output: false
}

func ExampleControl_AuthorizeRemoteCall() {
	var c Control
	err := c.AuthorizeRemoteCall(context.Background(), nil, "", RemoteReadInput)
	fmt.Println(errors.Is(err, ErrUnavailable))
	// Output: true
}

func ExampleControl_ReadRemoteInput() {
	var c Control
	_, _, err := c.ReadRemoteInput(context.Background(), nil, 0, 128)
	fmt.Println(errors.Is(err, ErrInvalid))
	// Output: true
}

func ExampleControl_PollRemoteRelease() {
	var c Control
	_, err := c.PollRemoteRelease(context.Background(), nil)
	fmt.Println(errors.Is(err, ErrInvalid))
	// Output: true
}

func ExampleControl_BeginRemoteOutput() {
	var c Control
	_, err := c.BeginRemoteOutput(context.Background(), nil, strings.Repeat("a", 64), 512)
	fmt.Println(errors.Is(err, ErrInvalid))
	// Output: true
}

func ExampleControl_WriteRemoteOutput() {
	var c Control
	_, err := c.WriteRemoteOutput(context.Background(), nil, 0, []byte("data"))
	fmt.Println(errors.Is(err, ErrInvalid))
	// Output: true
}

func ExampleControl_FinishRemoteOutput() {
	var c Control
	_, err := c.FinishRemoteOutput(context.Background(), nil)
	fmt.Println(errors.Is(err, ErrInvalid))
	// Output: true
}

func ExampleControl_ServeRemoteHTTP() {
	var c Control
	// No original authorized call means no handler invocation or physical proof.
	err := c.ServeRemoteHTTP(nil, httptest.NewRecorder(), httptest.NewRequest("POST", "/", nil), nil)
	fmt.Println(errors.Is(err, ErrInvalid))
	// Output: true
}

func TestRemoteGeneratedOutputActualFilesLifecycleAndConflicts(t *testing.T) {
	for _, jsonCodec := range []bool{false, true} {
		t.Run(map[bool]string{false: "proto", true: "json"}[jsonCodec], func(t *testing.T) {
			f := newDataControlFixture(t)
			input, _ := f.input(t)
			const requestID = "actual-output"
			key, _ := remoteTransferAttempt(t, f, requestID, input, true)
			row, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			// These native stop/collector values are explicitly MODELED. The actual
			// Store transaction supplies its own release origin; this test qualifies
			// real Control/HTTP/Files I/O, not a Pod stop or native collection handoff.
			zero := int32(0)
			finished := time.Now().UTC()
			stop := &backend.DataTaskStop{
				Stage: &backend.ContainerExit{ContainerID: "modeled-stage", ExitCode: 0, StartedAt: row.Active.Began, FinishedAt: row.Active.Began},
				Task:  backend.ContainerExit{ContainerID: "modeled-main", ExitCode: 0, StartedAt: row.Active.Began, FinishedAt: finished},
			}
			collector := &backend.ContainerExit{ContainerID: "modeled-collector", ExitCode: 0, StartedAt: row.Active.Began, FinishedAt: finished}
			row, err = f.store.ObserveRemote(key, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Succeeded, Instance: row.Resources.Pod.UID, ExitCode: &zero, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: stop, Collector: collector}, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if row.Active.CollectionDeadline == nil || row.Active.ReleaseRevision == 0 {
				t.Fatal("real Store observation did not allocate collection release")
			}
			outputSecret := sha256.Sum256([]byte(requestID + ":output"))
			token, err := remoteToken(key, outputSecret[:])
			clear(outputSecret[:])
			if err != nil {
				t.Fatal(err)
			}
			outputToken := string(token)
			grant, err := f.store.CheckRemoteGrant(key.AttemptID, store.RemoteOutput, row.Active.Output.Hash, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}

			var archive bytes.Buffer
			writer := tar.NewWriter(&archive)
			payload := bytes.Repeat([]byte("actual scoped output\n"), 31)
			if err = writer.WriteHeader(&tar.Header{Name: "result/output.txt", Mode: 0600, Size: int64(len(payload))}); err != nil {
				t.Fatal(err)
			}
			if _, err = writer.Write(payload); err != nil {
				t.Fatal(err)
			}
			if err = writer.Close(); err != nil {
				t.Fatal(err)
			}
			data := bytes.Clone(archive.Bytes())
			digest := sha256.Sum256(data)
			sha := hex.EncodeToString(digest[:])

			_, generated := skyv1connect.NewRemoteDataServiceHandler(remoteHTTPControlService{f.control}, connect.WithCompressMinBytes(1<<20), connect.WithReadMaxBytes(96<<10), connect.WithSendMaxBytes(96<<10))
			joined := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var op RemoteOperation
				switch r.URL.Path {
				case skyv1connect.RemoteDataServicePollReleaseProcedure:
					op = RemotePollRelease
				case skyv1connect.RemoteDataServiceBeginOutputProcedure:
					op = RemoteBeginOutput
				case skyv1connect.RemoteDataServiceWriteOutputProcedure:
					op = RemoteWriteOutput
				case skyv1connect.RemoteDataServiceFinishOutputProcedure:
					op = RemoteFinishOutput
				default:
					joined <- ErrInvalid
					return
				}
				call, err := f.control.TryArtifactCall()
				if err != nil {
					joined <- err
					return
				}
				if err = f.control.AuthorizeRemoteCall(r.Context(), call, outputToken, op); err != nil {
					if closeErr := r.Body.Close(); closeErr != nil {
						joined <- closeErr
						return
					}
					call.Done()
					joined <- err
					return
				}
				request := r.Clone(context.WithValue(r.Context(), remoteHTTPTestCallKey{}, call))
				err = f.control.ServeRemoteHTTP(call, w, request, generated)
				if err == nil {
					call.Done()
				}
				joined <- err
			}))
			closeServer := sync.OnceFunc(server.Close)
			t.Cleanup(closeServer)
			options := []connect.ClientOption{}
			if jsonCodec {
				options = append(options, connect.WithProtoJSON())
			}
			client := skyv1connect.NewRemoteDataServiceClient(server.Client(), server.URL, options...)
			// This context remains alive until the exact producer result has joined.
			// No client-side cancel invents response, file or outer call completion.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			t.Cleanup(cancel)
			joinCall := func(clientErr error, wantError bool) {
				t.Helper()
				var producerErr error
				select {
				case producerErr = <-joined:
				case <-time.After(10 * time.Second):
					failure := errors.New("original output HTTP producer did not join")
					f.hold(failure)
					t.Fatal(failure)
				}
				if producerErr != nil {
					f.hold(producerErr)
					t.Fatalf("original output response remains held: %v", producerErr)
				}
				if (clientErr != nil) != wantError {
					t.Fatalf("output application error=%v wantError=%t", clientErr, wantError)
				}
				s := f.control.artifacts
				s.mu.Lock()
				calls, uploads := len(s.calls), len(s.uploads)
				s.mu.Unlock()
				if calls != 0 || uploads != 0 {
					failure := errors.New("joined output response retained common call/upload owner")
					f.hold(failure)
					t.Fatalf("%v: calls=%d uploads=%d", failure, calls, uploads)
				}
			}
			type outputSnapshot struct {
				upload       store.ArtifactUpload
				file         store.ArtifactFile
				manifest     store.ArtifactManifest
				row          store.RemoteAttempt
				reservations store.ArtifactReservations
			}
			snapshot := func() outputSnapshot {
				t.Helper()
				u, file, manifest, e := f.store.GetRemoteOutput(grant, time.Now().UTC())
				if e != nil {
					t.Fatal(e)
				}
				current, e := f.store.GetRemoteAttempt(key)
				if e != nil {
					t.Fatal(e)
				}
				reservations, e := f.store.ArtifactReservations()
				if e != nil {
					t.Fatal(e)
				}
				return outputSnapshot{u, file, manifest, current, reservations}
			}
			unchanged := func(before outputSnapshot) {
				t.Helper()
				if after := snapshot(); !reflect.DeepEqual(after, before) {
					t.Fatal("refused/replayed output changed exact cursor, file, manifest, attempt or logical reservations")
				}
			}

			release, err := client.PollRelease(ctx, connect.NewRequest(&skyv1.PollReleaseRequest{}))
			joinCall(err, false)
			if release.Msg.State != skyv1.RemoteReleaseState(2) || release.Msg.Revision != row.Active.ReleaseRevision || release.Msg.TaskContainerId != stop.Task.ContainerID || release.Msg.TaskExitCode == nil || *release.Msg.TaskExitCode != 0 {
				t.Fatal("actual generated release mapping lost the fixed successful main receipt")
			}
			uploadGrowthSample(t, f.control) // Actual Files/Store physical cohort, no cache-success stub.
			begun, err := client.BeginOutput(ctx, connect.NewRequest(&skyv1.BeginOutputRequest{Sha256: sha, Size: int64(len(data))}))
			joinCall(err, false)
			if begun.Msg.Acknowledged != 0 || begun.Msg.State != skyv1.RemoteOutputState_REMOTE_OUTPUT_STATE_WRITABLE {
				t.Fatal("actual Begin did not create a writable original part")
			}
			initial := snapshot()
			if initial.file.Device == 0 || initial.file.Inode == 0 || initial.upload.State != "writable" || initial.manifest != (store.ArtifactManifest{}) {
				t.Fatal("Begin lost actual exclusively created part identity or fabricated sealed manifest")
			}
			replay, err := client.BeginOutput(ctx, connect.NewRequest(&skyv1.BeginOutputRequest{Sha256: sha, Size: int64(len(data))}))
			joinCall(err, false)
			if replay.Msg.Acknowledged != begun.Msg.Acknowledged || replay.Msg.State != begun.Msg.State {
				t.Fatal("Begin exact replay changed wire state")
			}
			unchanged(initial)
			for _, request := range []*skyv1.BeginOutputRequest{{Sha256: strings.Repeat("e", 64), Size: int64(len(data))}, {Sha256: sha, Size: int64(len(data) + 1)}} {
				_, err = client.BeginOutput(ctx, connect.NewRequest(request))
				joinCall(err, true)
				unchanged(initial)
			}
			uploadGrowthSample(t, f.control)
			const prefix = 37
			written, err := client.WriteOutput(ctx, connect.NewRequest(&skyv1.WriteOutputRequest{Offset: 0, Data: bytes.Clone(data[:prefix])}))
			joinCall(err, false)
			if written.Msg.Acknowledged != prefix {
				t.Fatal("actual prefix write was not acknowledged")
			}
			afterPrefix := snapshot()
			retransmit, err := client.WriteOutput(ctx, connect.NewRequest(&skyv1.WriteOutputRequest{Offset: 0, Data: bytes.Clone(data[:prefix])}))
			joinCall(err, false)
			if retransmit.Msg.Acknowledged != prefix {
				t.Fatal("exact actual prefix retransmit changed acknowledged offset")
			}
			unchanged(afterPrefix)
			_, err = client.FinishOutput(ctx, connect.NewRequest(&skyv1.FinishOutputRequest{}))
			joinCall(err, true)
			unchanged(afterPrefix)
			changed := bytes.Clone(data[:prefix])
			changed[0] ^= 1
			for _, request := range []*skyv1.WriteOutputRequest{
				{Offset: 0, Data: changed},
				{Offset: prefix + 1, Data: []byte("hole")},
				{Offset: prefix - 1, Data: bytes.Clone(data[prefix-1 : prefix+1])},
				{Offset: int64(1<<63 - 1), Data: []byte("overflow")},
				{Offset: -1, Data: []byte("negative")},
				{Offset: prefix, Data: nil},
			} {
				_, err = client.WriteOutput(ctx, connect.NewRequest(request))
				joinCall(err, true)
				unchanged(afterPrefix)
			}
			// The ordinary changed-prefix refusal invalidates the physical sample, not
			// the original part. A fresh actual cohort permits its remaining append.
			uploadGrowthSample(t, f.control)
			completePrefix, err := client.WriteOutput(ctx, connect.NewRequest(&skyv1.WriteOutputRequest{Offset: prefix, Data: bytes.Clone(data[prefix:])}))
			joinCall(err, false)
			if completePrefix.Msg.Acknowledged != int64(len(data)) {
				t.Fatal("actual remaining bytes did not reach exact archive size")
			}
			full := snapshot()
			if full.upload.Acknowledged != uint64(len(data)) || full.file != initial.file || full.upload.State != "writable" {
				t.Fatal("append lost the original inode or durable prefix")
			}
			uploadGrowthSample(t, f.control)
			sealing, err := client.FinishOutput(ctx, connect.NewRequest(&skyv1.FinishOutputRequest{}))
			joinCall(err, false)
			if sealing.Msg.State != skyv1.RemoteOutputSealState_REMOTE_OUTPUT_SEAL_STATE_SEALING {
				t.Fatal("first Finish did not stop at durable publishing cohort")
			}
			publishing := snapshot()
			if publishing.upload.State != "finalizing" || publishing.file != initial.file || publishing.manifest != (store.ArtifactManifest{}) {
				t.Fatal("first Finish fabricated a sealed result")
			}
			uploadGrowthSample(t, f.control) // Observe the actual publishing cohort before linking.
			sealed, err := client.FinishOutput(ctx, connect.NewRequest(&skyv1.FinishOutputRequest{}))
			joinCall(err, false)
			if sealed.Msg.State != skyv1.RemoteOutputSealState_REMOTE_OUTPUT_SEAL_STATE_SEALED {
				t.Fatal("second Finish did not seal actual verified archive")
			}
			private := snapshot()
			if private.upload.State != "complete" || private.upload.Reference.SHA256 != sha || private.upload.Reference.Size != uint64(len(data)) || private.file != initial.file || private.manifest.Reference != private.upload.Reference || private.manifest.RegularFiles != 1 || private.manifest.ExpandedBytes != uint64(len(payload)) {
				t.Fatal("sealed private receipt differs from the actual original archive/file")
			}
			again, err := client.FinishOutput(ctx, connect.NewRequest(&skyv1.FinishOutputRequest{}))
			joinCall(err, false)
			if again.Msg.State != skyv1.RemoteOutputSealState_REMOTE_OUTPUT_SEAL_STATE_SEALED {
				t.Fatal("sealed exact replay lost its state")
			}
			unchanged(private)
			sealedBegin, err := client.BeginOutput(ctx, connect.NewRequest(&skyv1.BeginOutputRequest{Sha256: sha, Size: int64(len(data))}))
			joinCall(err, false)
			if sealedBegin.Msg.State != skyv1.RemoteOutputState_REMOTE_OUTPUT_STATE_SEALED || sealedBegin.Msg.Acknowledged != int64(len(data)) {
				t.Fatal("sealed Begin replay did not preserve acknowledged private result")
			}
			unchanged(private)
			_, err = client.WriteOutput(ctx, connect.NewRequest(&skyv1.WriteOutputRequest{Offset: int64(len(data)), Data: []byte("late")}))
			joinCall(err, true)
			unchanged(private)
			result, err := f.store.ArtifactResultFor(f.actor, key.JobID, key.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
			if result.State != "absent" || result.Reference != nil || private.row.State != 2 || private.row.Done != nil {
				t.Fatal("scoped file seal attached a job result or terminal decision")
			}
			if _, err = f.store.GetArtifactUpload(f.actor, private.upload.Reference.ID); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("ordinary Actor getter adopted scoped private upload: %v", err)
			}
			closeServer()
			cancel() // Actual external frames joined before fixture drain/close.
		})
	}
}
