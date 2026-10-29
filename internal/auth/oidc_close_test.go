package auth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// closeRecord records each descriptor passed to closeOIDCFile.
type closeRecord struct {
	mu    sync.Mutex
	files []*os.File
}

func (r *closeRecord) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.files)
}

func (r *closeRecord) named(base string) []*os.File {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*os.File
	for _, f := range r.files {
		if filepath.Base(f.Name()) == base {
			out = append(out, f)
		}
	}
	return out
}

// modelOIDCCloseFailure replaces closeOIDCFile for the test. Each call really
// closes the original descriptor readAuthFile opened and records it; for a
// file whose base name is name it then returns fail. The returned error is
// MODELED: the native Close succeeded, and fail stands in for an operating
// system Close failure.
func modelOIDCCloseFailure(t *testing.T, name string, fail error) *closeRecord {
	t.Helper()
	r := new(closeRecord)
	t.Cleanup(func() { closeOIDCFile = (*os.File).Close })
	closeOIDCFile = func(f *os.File) error {
		r.mu.Lock()
		r.files = append(r.files, f)
		r.mu.Unlock()
		if err := f.Close(); err != nil {
			t.Errorf("native close: %v", err)
			return err
		}
		if filepath.Base(f.Name()) == name {
			return fail
		}
		return nil
	}
	return r
}

// modeledError is a MODELED request, read or stat failure. Its type lets a
// test find the original cause with errors.As; no such fault occurred.
type modeledError struct{ op string }

func (e *modeledError) Error() string { return "modeled " + e.op + " failure" }

// modelBody is a MODELED response body standing in for a native one; no
// issuer, network or kernel fault occurs. Read first waits for block, if set,
// ignoring cancellation as a hung read could, then serves data and readErr in
// place of io.EOF. Close counts calls, then waits for closeBlock, if set,
// likewise ignoring cancellation, and returns closeErr. entered and
// closeEntered are closed when Read and Close first begin to wait.
type modelBody struct {
	data         io.Reader
	readErr      error
	block        chan struct{}
	entered      chan struct{}
	enter        sync.Once
	closeErr     error
	closeBlock   chan struct{}
	closeEntered chan struct{}
	closeEnter   sync.Once
	closes       atomic.Int32 // Close calls begun
	closed       atomic.Int32 // Close calls returned
}

func (b *modelBody) Read(p []byte) (int, error) {
	if b.block != nil {
		b.enter.Do(func() { close(b.entered) })
		<-b.block
	}
	n, err := b.data.Read(p)
	if err == io.EOF && b.readErr != nil {
		err = b.readErr
	}
	return n, err
}

func (b *modelBody) Close() error {
	b.closes.Add(1)
	defer b.closed.Add(1)
	if b.closeBlock != nil {
		b.closeEnter.Do(func() { close(b.closeEntered) })
		<-b.closeBlock
	}
	return b.closeErr
}

func modelResponse(status int, body *modelBody) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: body}
}

// modeledJWKSURL is allowed by newModeledJWKSFixture. Only modelJWKSDo
// answers it; nothing is sent to the network.
const modeledJWKSURL = "https://issuer.example/keys"

// newModeledJWKSFixture switches the OIDC engine fixture to a jwks_uri,
// without a CA file, that only a modelJWKSDo response answers.
func newModeledJWKSFixture(t *testing.T) *oidcEngineFixture {
	t.Helper()
	f := newOIDCEngineFixture(t)
	f.e.oidcKeys = newJWKSFetcher(modeledJWKSURL, f.clock.now)
	f.writeOIDC(t, 2, map[string]any{"jwks_file": nil, "jwks_uri": modeledJWKSURL})
	return f
}

// runJWKSUntilStopped runs f.e.runJWKS, which must stop by itself after an
// uncertain Close. If it does not within ten seconds, it is cancelled and
// joined before the test fails, so it never outlives the test.
func runJWKSUntilStopped(t *testing.T, e *Engine) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.runJWKS(ctx)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		cancel()
		<-done
		t.Fatal("runJWKS did not stop after an uncertain Close")
	}
}

// requireHeld checks that e is unavailable for good with err retained: new
// scoped tokens and Check of actor, minted before the hold, are refused, and
// neither reader reads or fetches again, even once every file reads and
// closes cleanly.
func requireHeld(t *testing.T, f *oidcEngineFixture, err error, actor Actor) {
	t.Helper()
	if f.e.Err() != err || f.e.Ready() {
		t.Fatalf("Err %v, Ready %v; want the retained error and not Ready", f.e.Err(), f.e.Ready())
	}
	if _, got := f.e.Authenticate("Bearer " + f.alice); got != ErrUnavailable {
		t.Fatalf("scoped Authenticate = %v, want ErrUnavailable", got)
	}
	if _, got := f.e.Check(actor, Read); got != ErrUnavailable {
		t.Fatalf("Check = %v, want ErrUnavailable", got)
	}
	creds := modelCloseFailure(t, nil)
	files := modelOIDCCloseFailure(t, "", nil)
	if got := f.e.refresh(); got != err {
		t.Fatalf("credential refresh = %v, want the retained error", got)
	}
	f.e.refreshOIDC()
	if f.e.oidcKeys != nil {
		runJWKSUntilStopped(t, f.e)
	}
	if len(*creds) != 0 || files.count() != 0 {
		t.Fatalf("read again after an uncertain Close: %d credential and %d OIDC file closes", len(*creds), files.count())
	}
	if f.e.Err() != err || f.e.Ready() {
		t.Fatal("uncertain Close was not sticky")
	}
}

// TestNewFileOIDCCloseUncertain checks that a constructor whose credential
// file Close fails returns the same partial Engine, retaining the original
// file, with the error; NewFile keeps returning no Engine.
func TestNewFileOIDCCloseUncertain(t *testing.T) {
	f := newOIDCEngineFixture(t)
	modeled := errors.New("modeled close failure")
	closed := modelCloseFailure(t, modeled)
	e, err := NewFileOIDC(f.auth, installation, f.oidc, modeledJWKSURL)
	if e == nil || !errors.Is(err, ErrCloseUncertain) || !errors.Is(err, modeled) || len(*closed) != 1 {
		t.Fatalf("NewFileOIDC = %v, %v, closes %d; want the Engine and ErrCloseUncertain", e, err, len(*closed))
	}
	if e.Err() != err {
		t.Fatalf("Engine Err %v, want the returned error %v", e.Err(), err)
	}
	var held *fileCloseError
	if !errors.As(err, &held) || held.file != (*closed)[0] {
		t.Fatal("the error does not retain the one original descriptor")
	}
	if e.Ready() {
		t.Fatal("Ready after an uncertain Close")
	}
	again := modelCloseFailure(t, nil)
	files := modelOIDCCloseFailure(t, "", nil)
	if got := e.refresh(); got != err {
		t.Fatalf("refresh = %v, want the retained error", got)
	}
	e.refreshOIDC()
	if len(*again) != 0 || files.count() != 0 {
		t.Fatal("the partial Engine read again")
	}
}

// TestOIDCFileCloseUncertain fails the MODELED Close of the OIDC file and of
// the JWKS file. Each failure retains its one original descriptor in the
// Engine's Err, makes the Engine unavailable for good, and stops all reads.
func TestOIDCFileCloseUncertain(t *testing.T) {
	for _, name := range []string{"oidc.json", "keys.json"} {
		t.Run(name, func(t *testing.T) {
			f := newOIDCEngineFixture(t)
			a := f.authenticate(t, f.idToken(t, nil))
			changed := f.e.Changed()
			modeled := errors.New("modeled close failure")
			rec := modelOIDCCloseFailure(t, name, modeled)
			f.e.refreshOIDC()
			err := f.e.Err()
			if !errors.Is(err, ErrCloseUncertain) || !errors.Is(err, modeled) {
				t.Fatalf("Err = %v, want ErrCloseUncertain wrapping the Close error", err)
			}
			var held *fileCloseError
			files := rec.named(name)
			if !errors.As(err, &held) || len(files) != 1 || held.file != files[0] {
				t.Fatalf("error does not retain the one original descriptor: closes %d", len(files))
			}
			if err.Error() != "close authentication file" || strings.Contains(err.Error(), f.dir) {
				t.Fatalf("error text %q exposes detail", err.Error())
			}
			select {
			case <-changed:
			default:
				t.Fatal("hold not notified")
			}
			requireHeld(t, f, err, a)
		})
	}
}

// TestOIDCCACloseUncertain fails the MODELED Close of the CA file a fetch
// reads. The original descriptor is retained, no request is sent, nothing is
// cached, and the fetcher stops.
func TestOIDCCACloseUncertain(t *testing.T) {
	f, s := newJWKSURIFixture(t, "/keys")
	f.refresh(t)
	scoped := f.authenticate(t, f.alice)
	modeled := errors.New("modeled close failure")
	rec := modelOIDCCloseFailure(t, "ca.pem", modeled)
	runJWKSUntilStopped(t, f.e)
	err := f.e.Err()
	var held *fileCloseError
	files := rec.named("ca.pem")
	if !errors.Is(err, modeled) || !errors.As(err, &held) || len(files) != 1 || held.file != files[0] {
		t.Fatalf("Err = %v, CA closes %d; want the one original CA descriptor retained", err, len(files))
	}
	if n := s.total(); n != 0 {
		t.Fatalf("%d requests after the CA file Close failed, want 0", n)
	}
	if f.e.oidcKeys.data != nil {
		t.Fatal("cached a key set from a fetch whose CA file Close failed")
	}
	requireHeld(t, f, err, scoped)
}

// TestOIDCBodyClose checks MODELED responses: each original body is closed
// exactly once, after its read; a failed Close retains that body; the
// original request or read error and the Close error are each found by
// errors.Is, the typed original by errors.As, behind fixed text; and only a
// clean, complete, valid response is cached, dated when it completed.
func TestOIDCBodyClose(t *testing.T) {
	oidcTestKeys(t)
	modeledClose := errors.New("modeled body close failure")
	modeledRead := &modeledError{"body read"}
	modeledDo := &modeledError{"request"}
	for _, tt := range []struct {
		name                     string
		status                   int
		noResponse               bool
		readErr, closeErr, doErr error
		primary                  *modeledError // the original cause retained, or nil
		text                     string        // the fixed error text; empty for success
	}{
		{"success", http.StatusOK, false, nil, nil, nil, nil, ""},
		{"close fails", http.StatusOK, false, nil, modeledClose, nil, nil, "close JWKS response"},
		{"read fails", http.StatusOK, false, modeledRead, nil, nil, modeledRead, "read JWKS response"},
		{"read and close fail", http.StatusOK, false, modeledRead, modeledClose, nil, modeledRead, "close JWKS response"},
		{"status refused and close fails", http.StatusInternalServerError, false, nil, modeledClose, nil, nil, "close JWKS response"},
		{"response with error", http.StatusOK, false, nil, nil, modeledDo, modeledDo, "fetch JWKS"},
		{"response with error and close fails", http.StatusOK, false, nil, modeledClose, modeledDo, modeledDo, "close JWKS response"},
		{"error without response", 0, true, nil, nil, modeledDo, modeledDo, "fetch JWKS"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newModeledJWKSFixture(t)
			f.refresh(t)
			b := &modelBody{data: bytes.NewReader(validJWKS(t)), readErr: tt.readErr, closeErr: tt.closeErr}
			modelJWKSDo(t, func(*http.Client, *http.Request) (*http.Response, error) {
				if tt.noResponse {
					return nil, tt.doErr
				}
				return modelResponse(tt.status, b), tt.doErr
			})
			err := f.e.oidcKeys.fetch(context.Background())
			want := int32(1)
			if tt.noResponse {
				want = 0
			}
			if n := b.closes.Load(); n != want {
				t.Fatalf("original body closed %d times, want exactly %d", n, want)
			}
			if tt.text == "" {
				if err != nil || f.e.oidcKeys.data == nil || !f.e.oidcKeys.fetched.Equal(f.clock.now()) {
					t.Fatalf("fetch = %v; want the key set cached as of its completion", err)
				}
				return
			}
			if err == nil {
				t.Fatal("fetch succeeded")
			}
			if f.e.oidcKeys.data != nil {
				t.Fatal("a failed fetch cached a key set")
			}
			if err.Error() != tt.text {
				t.Fatalf("fetch = %q, want fixed text %q", err.Error(), tt.text)
			}
			held := tt.closeErr != nil
			if errors.Is(err, ErrCloseUncertain) != held {
				t.Fatalf("fetch = %v; held %v, want %v", err, !held, held)
			}
			if tt.primary != nil {
				if !errors.Is(err, tt.primary) {
					t.Fatalf("fetch = %v; the original %v is lost", err, tt.primary)
				}
				var cause *modeledError
				if !errors.As(err, &cause) || cause != tt.primary {
					t.Fatalf("errors.As found %v, want the original %v", cause, tt.primary)
				}
			}
			if !held {
				return
			}
			if !errors.Is(err, modeledClose) {
				t.Fatalf("fetch = %v; the original Close error is lost", err)
			}
			var owner *bodyCloseError
			if !errors.As(err, &owner) || owner.body != io.ReadCloser(b) {
				t.Fatal("the error does not retain the original body")
			}
		})
	}
}

// TestOIDCFileReadCause fails, MODELED, the stat or the read of the same
// original OIDC file descriptor readAuthFile opened, with a clean or a
// failed Close. The error keeps fixed text and retains the original cause,
// found by errors.Is and errors.As; with a failed Close it also retains the
// Close error and the one original descriptor. The control reads cleanly.
func TestOIDCFileReadCause(t *testing.T) {
	modeledClose := errors.New("modeled file close failure")
	for _, tt := range []struct {
		name, op string
		closeErr error
	}{
		{"clean", "", nil},
		{"stat fails", "stat", nil},
		{"stat and close fail", "stat", modeledClose},
		{"read fails", "read", nil},
		{"read and close fail", "read", modeledClose},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newOIDCEngineFixture(t)
			primary := &modeledError{tt.op}
			rec := modelOIDCCloseFailure(t, "oidc.json", tt.closeErr)
			stat, read := statOIDCFile, readOIDCFile
			t.Cleanup(func() { statOIDCFile, readOIDCFile = stat, read })
			var seen *os.File
			statOIDCFile = func(file *os.File) (os.FileInfo, error) {
				info, err := stat(file)
				if tt.op != "stat" {
					return info, err
				}
				seen = file
				if err != nil {
					t.Errorf("native stat: %v", err)
				}
				return nil, primary
			}
			readOIDCFile = func(file *os.File, max int64) ([]byte, error) {
				data, err := read(file, max)
				if tt.op != "read" {
					return data, err
				}
				seen = file
				if err != nil {
					t.Errorf("native read: %v", err)
				}
				return nil, primary
			}
			data, err := readAuthFile(f.oidc, maxOIDCConfigBytes, true)
			files := rec.named("oidc.json")
			if len(files) != 1 {
				t.Fatalf("original file closed %d times, want 1", len(files))
			}
			if tt.op == "" {
				if err != nil || len(data) == 0 {
					t.Fatalf("readAuthFile = %v", err)
				}
				return
			}
			if data != nil || seen != files[0] {
				t.Fatal("returned bytes, or faulted another descriptor than the one closed")
			}
			if !errors.Is(err, primary) {
				t.Fatalf("readAuthFile = %v; the original %s error is lost", err, tt.op)
			}
			var cause *modeledError
			if !errors.As(err, &cause) || cause != primary {
				t.Fatalf("errors.As found %v, want the original %v", cause, primary)
			}
			text := "read OIDC file"
			if tt.closeErr != nil {
				text = "close authentication file"
				var owner *fileCloseError
				if !errors.Is(err, ErrCloseUncertain) || !errors.Is(err, modeledClose) || !errors.As(err, &owner) || owner.file != files[0] {
					t.Fatalf("readAuthFile = %v; want the Close error and the original descriptor retained", err)
				}
			} else if errors.Is(err, ErrCloseUncertain) {
				t.Fatalf("readAuthFile = %v after a clean Close", err)
			}
			if err.Error() != text || strings.Contains(err.Error(), f.dir) {
				t.Fatalf("error text %q, want %q", err.Error(), text)
			}
		})
	}
}

// TestOIDCBodyCloseUncertainSticky runs the fetcher against a MODELED body
// whose Close fails. The fetcher stops by itself; the Engine retains that
// body and is unavailable for good; no request replaces it.
func TestOIDCBodyCloseUncertainSticky(t *testing.T) {
	f := newModeledJWKSFixture(t)
	f.refresh(t)
	scoped := f.authenticate(t, f.alice)
	modeled := errors.New("modeled body close failure")
	keys := validJWKS(t)
	var bodies []*modelBody
	var mu sync.Mutex
	modelJWKSDo(t, func(*http.Client, *http.Request) (*http.Response, error) {
		b := &modelBody{data: bytes.NewReader(keys), closeErr: modeled}
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		return modelResponse(http.StatusOK, b), nil
	})
	changed := f.e.Changed()
	runJWKSUntilStopped(t, f.e)
	mu.Lock()
	n := len(bodies)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("%d requests, want 1: a request followed an uncertain Close", n)
	}
	err := f.e.Err()
	var held *bodyCloseError
	if !errors.As(err, &held) || held.body != io.ReadCloser(bodies[0]) || bodies[0].closes.Load() != 1 {
		t.Fatalf("Err = %v; want the one original body, closed once, retained", err)
	}
	select {
	case <-changed:
	default:
		t.Fatal("hold not notified")
	}
	requireHeld(t, f, err, scoped)
	mu.Lock()
	n = len(bodies)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("%d requests after the hold, want 1", n)
	}
}

// TestOIDCRunBlockedBody runs the original Run on the real clock with a
// MODELED body whose first Read, or whose first Close, waits for an explicit
// release, ignoring cancellation as a hung read or Close could. Scoped tokens
// stay available while it is pending. After cancellation Run stays pending,
// with the same original body and no other request, until that original
// returns. After a clean late return nothing is published, and a later fresh
// fetch succeeds. After a failed Close the Engine holds, for good, the
// original body and each original cause.
func TestOIDCRunBlockedBody(t *testing.T) {
	modeledClose := errors.New("modeled body close failure")
	modeledRead := &modeledError{"body read"}
	for _, tt := range []struct {
		name              string
		blockClose        bool
		readErr, closeErr error
	}{
		{"read", false, nil, nil},
		{"close", true, nil, nil},
		{"close uncertain", true, nil, modeledClose},
		{"read fails and close uncertain", false, modeledRead, modeledClose},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newOIDCEngineFixture(t)
			f.writeOIDC(t, 2, map[string]any{"jwks_file": nil, "jwks_uri": modeledJWKSURL})
			gate, entered := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(gate) }) }
			b := &modelBody{data: bytes.NewReader(validJWKS(t)), readErr: tt.readErr, closeErr: tt.closeErr}
			if tt.blockClose {
				b.closeBlock, b.closeEntered = gate, entered
			} else {
				b.block, b.entered = gate, entered
			}
			var calls atomic.Int32
			modelJWKSDo(t, func(*http.Client, *http.Request) (*http.Response, error) {
				if calls.Add(1) > 1 {
					return modelResponse(http.StatusOK, &modelBody{data: bytes.NewReader(validJWKS(t))}), nil
				}
				return modelResponse(http.StatusOK, b), nil
			})
			e, err := NewFileOIDC(f.auth, installation, f.oidc, modeledJWKSURL)
			if err != nil {
				t.Fatal(err)
			}
			// startRun's cleanup releases the original, cancels Run and waits
			// for that same Run without limit, even after a failed assertion,
			// before the seam resets and fixture removal registered earlier.
			cancel, done := startRun(t, e, release)
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("Run started no fetch within 10s")
			}
			if !e.Ready() {
				t.Fatal("Engine not ready while only a fetch is pending")
			}
			if _, err := e.Authenticate("Bearer " + f.alice); err != nil {
				t.Fatalf("scoped Authenticate while a fetch is pending: %v", err)
			}
			cancel()
			select {
			case <-done:
				t.Fatal("Run returned while the original body was pending")
			case <-time.After(500 * time.Millisecond):
			}
			begun := int32(0)
			if tt.blockClose {
				begun = 1
			}
			if b.closes.Load() != begun || b.closed.Load() != 0 || calls.Load() != 1 || e.Err() != nil {
				t.Fatalf("while pending: %d Close begun, %d returned, %d requests, Err %v; want %d, 0, 1, nil", b.closes.Load(), b.closed.Load(), calls.Load(), e.Err(), begun)
			}
			release()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("Run did not return within 10s of the original returning")
			}
			if b.closes.Load() != 1 || b.closed.Load() != 1 || calls.Load() != 1 {
				t.Fatalf("body closed %d times (%d returned), %d requests; want 1, 1, 1", b.closes.Load(), b.closed.Load(), calls.Load())
			}
			if tt.closeErr == nil {
				if e.Err() != nil {
					t.Fatalf("Err = %v after a clean Close", e.Err())
				}
				if e.oidcKeys.data != nil {
					t.Fatal("a fetch that returned after cancellation published its key set")
				}
				// The original has returned, so fresh work may follow.
				if err := e.oidcKeys.fetch(context.Background()); err != nil {
					t.Fatalf("fresh fetch after the late original: %v", err)
				}
				if e.oidcKeys.data == nil || calls.Load() != 2 {
					t.Fatalf("fresh fetch published nothing (%d requests)", calls.Load())
				}
				return
			}
			err = e.Err()
			if !errors.Is(err, ErrCloseUncertain) || !errors.Is(err, modeledClose) {
				t.Fatalf("Err = %v; want the original Close error held", err)
			}
			// Run was cancelled before the original returned, so the held
			// error also keeps that cause, and is still terminal.
			if !errors.Is(err, context.Canceled) || err.Error() != "close JWKS response" {
				t.Fatalf("Err = %v; want the uncertain Close with the cancellation cause", err)
			}
			var owner *bodyCloseError
			if !errors.As(err, &owner) || owner.body != io.ReadCloser(b) {
				t.Fatal("Err does not retain the original body")
			}
			if tt.readErr != nil {
				var cause *modeledError
				if !errors.Is(err, modeledRead) || !errors.As(err, &cause) || cause != modeledRead {
					t.Fatalf("Err = %v; the original read error is lost", err)
				}
			}
			if e.Ready() || e.oidcKeys.data != nil {
				t.Fatal("ready, or published keys, after an uncertain Close")
			}
			if got := e.refresh(); got != err {
				t.Fatalf("credential refresh = %v, want the held error", got)
			}
		})
	}
}

// TestOIDCJWKSLateOriginal fetches with the MODELED clock while the original
// body Read waits, without cancellation, and the clock passes the fetch
// window by one nanosecond. fetch stays pending with no other request until
// release; then the same original returns and closes once. After a clean
// Close the late key set is refused with the deadline cause and nothing is
// published or dated, and a fresh independent fetch then succeeds, dated
// when it completed. After a failed Close, with a clean read or a failed
// one, the error is the uncertain Close with its text, the original body,
// each original cause and the deadline cause, so runJWKS holds it.
func TestOIDCJWKSLateOriginal(t *testing.T) {
	modeledClose := errors.New("modeled body close failure")
	modeledRead := &modeledError{"body read"}
	for _, tt := range []struct {
		name              string
		readErr, closeErr error
	}{
		{"clean", nil, nil},
		{"close uncertain", nil, modeledClose},
		{"read fails and close uncertain", modeledRead, modeledClose},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newModeledJWKSFixture(t)
			f.refresh(t)
			gate := make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(gate) }) }
			b := &modelBody{data: bytes.NewReader(validJWKS(t)), readErr: tt.readErr, closeErr: tt.closeErr, block: gate, entered: make(chan struct{})}
			var calls atomic.Int32
			modelJWKSDo(t, func(*http.Client, *http.Request) (*http.Response, error) {
				if calls.Add(1) > 1 {
					return modelResponse(http.StatusOK, &modelBody{data: bytes.NewReader(validJWKS(t))}), nil
				}
				return modelResponse(http.StatusOK, b), nil
			})
			var err error
			// startOriginal's cleanup, registered before the fetch starts,
			// releases the original read and waits for that same fetch
			// without limit before the seam resets and fixture removal
			// registered earlier. The fetch ignores its context: nothing
			// cancels it, only the MODELED clock passes the window.
			_, returned := startOriginal(t, release, func(context.Context) {
				err = f.e.oidcKeys.fetch(context.Background())
			})
			select {
			case <-b.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("fetch did not read the body within 10s")
			}
			f.clock.add(jwksFetchTimeout + time.Nanosecond)
			select {
			case <-returned:
				t.Fatal("fetch returned while its original read was pending")
			case <-time.After(200 * time.Millisecond):
			}
			if b.closes.Load() != 0 || calls.Load() != 1 {
				t.Fatalf("while pending: %d closes, %d requests; want 0, 1", b.closes.Load(), calls.Load())
			}
			release()
			select {
			case <-returned:
			case <-time.After(10 * time.Second):
				t.Fatal("fetch did not return within 10s of release")
			}
			if b.closes.Load() != 1 || calls.Load() != 1 {
				t.Fatalf("%d closes, %d requests; want 1, 1", b.closes.Load(), calls.Load())
			}
			if f.e.oidcKeys.data != nil || !f.e.oidcKeys.fetched.IsZero() {
				t.Fatal("a late original published or dated a key set")
			}
			if tt.closeErr == nil {
				requireLate(t, f, err, context.DeadlineExceeded)
				done := f.clock.now()
				if err := f.e.oidcKeys.fetch(context.Background()); err != nil {
					t.Fatalf("fresh fetch after the late original: %v", err)
				}
				if calls.Load() != 2 || !f.e.oidcKeys.fetched.Equal(done) {
					t.Fatalf("fresh fetch: %d requests, dated %v; want 2, %v", calls.Load(), f.e.oidcKeys.fetched, done)
				}
				f.refresh(t)
				f.authenticate(t, f.idToken(t, nil))
				return
			}
			if err.Error() != "close JWKS response" || !errors.Is(err, ErrCloseUncertain) || !errors.Is(err, modeledClose) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				t.Fatalf("fetch = %v; want the uncertain Close with its Close and deadline causes", err)
			}
			var owner *bodyCloseError
			if !errors.As(err, &owner) || owner.body != io.ReadCloser(b) {
				t.Fatal("the error does not retain the original body")
			}
			if tt.readErr != nil {
				var cause *modeledError
				if !errors.Is(err, modeledRead) || !errors.As(err, &cause) || cause != modeledRead {
					t.Fatalf("fetch = %v; the original read error is lost", err)
				}
			}
		})
	}
}

// blockOIDCFile makes the first read, or the first Close (op), of an original
// OIDC, JWKS or CA file whose base name is name wait for release, ignoring
// cancellation as a hung mount could; the actual operation on that same
// descriptor then runs. Every Close is recorded when it begins and really
// closes its descriptor; a Close of name then returns fail, MODELED. reads
// counts reads of name.
func blockOIDCFile(t *testing.T, name, op string, fail error) (entered <-chan struct{}, release func(), rec *closeRecord, reads *atomic.Int32) {
	t.Helper()
	in, gate := make(chan struct{}), make(chan struct{})
	var enter, leave sync.Once
	release = func() { leave.Do(func() { close(gate) }) }
	wait := func() {
		first := false
		enter.Do(func() { first = true; close(in) })
		if first {
			<-gate
		}
	}
	rec, reads = new(closeRecord), new(atomic.Int32)
	read, closeFile := readOIDCFile, closeOIDCFile
	t.Cleanup(func() { readOIDCFile, closeOIDCFile = read, closeFile })
	readOIDCFile = func(f *os.File, max int64) ([]byte, error) {
		if filepath.Base(f.Name()) == name {
			reads.Add(1)
			if op == "read" {
				wait()
			}
		}
		return read(f, max)
	}
	closeOIDCFile = func(f *os.File) error {
		rec.mu.Lock()
		rec.files = append(rec.files, f)
		rec.mu.Unlock()
		match := filepath.Base(f.Name()) == name
		if match && op == "close" {
			wait()
		}
		if err := closeFile(f); err != nil {
			t.Errorf("native close: %v", err)
			return err
		}
		if match {
			return fail
		}
		return nil
	}
	return in, release, rec, reads
}

// TestOIDCRunBlockedFile runs the original Run on the real clock while the
// first read, or the first Close, of the original OIDC file, JWKS file or CA
// file waits for an explicit release. Scoped tokens stay available. After
// cancellation Run stays pending with that same file, without another read
// of it or a request in its place, until it returns. After a clean late
// return nothing is retained, and fresh reads and fetches then succeed. A
// MODELED failed Close holds the Engine for good with the original
// descriptor and Close error.
func TestOIDCRunBlockedFile(t *testing.T) {
	modeled := errors.New("modeled close failure")
	for _, tt := range []struct {
		name, op string
		fail     bool
	}{
		{"oidc.json", "read", false},
		{"keys.json", "read", false},
		{"ca.pem", "read", false},
		{"oidc.json", "close", false},
		{"keys.json", "close", false},
		{"ca.pem", "close", false},
		{"oidc.json", "close", true},
		{"keys.json", "close", true},
		{"ca.pem", "close", true},
	} {
		t.Run(fmt.Sprintf("%s/%s/fail=%v", tt.name, tt.op, tt.fail), func(t *testing.T) {
			var f *oidcEngineFixture
			var s *jwksServer
			url := ""
			if tt.name == "ca.pem" {
				f, s = newJWKSURIFixture(t, "/keys")
				url = s.URL + "/keys"
			} else {
				f = newOIDCEngineFixture(t)
			}
			var fail error
			if tt.fail {
				fail = modeled
			}
			entered, release, rec, reads := blockOIDCFile(t, tt.name, tt.op, fail)
			e, err := NewFileOIDC(f.auth, installation, f.oidc, url)
			if err != nil {
				t.Fatal(err)
			}
			// startRun's cleanup releases the original, cancels Run and waits
			// for that same Run without limit, even after a failed assertion,
			// before the seam resets and fixture removal registered earlier.
			cancel, done := startRun(t, e, release)
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatalf("Run did not reach the %s %s within 10s", tt.name, tt.op)
			}
			if _, err := e.Authenticate("Bearer " + f.alice); err != nil || !e.Ready() {
				t.Fatalf("scoped Authenticate while the %s %s is pending: %v", tt.name, tt.op, err)
			}
			cancel()
			select {
			case <-done:
				t.Fatalf("Run returned while the original %s %s was pending", tt.name, tt.op)
			case <-time.After(500 * time.Millisecond):
			}
			begun := 0
			if tt.op == "close" {
				begun = 1
			}
			if n := len(rec.named(tt.name)); n != begun || reads.Load() != 1 || e.Err() != nil {
				t.Fatalf("while pending: %d Close begun, %d reads, Err %v; want %d, 1, nil", n, reads.Load(), e.Err(), begun)
			}
			if s != nil && s.total() != 0 {
				t.Fatalf("%d requests while the CA file was pending, want 0", s.total())
			}
			release()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatalf("Run did not return within 10s of the %s %s returning", tt.name, tt.op)
			}
			files := rec.named(tt.name)
			if len(files) != 1 || reads.Load() != 1 {
				t.Fatalf("%d closes and %d reads of %s, want 1 and 1", len(files), reads.Load(), tt.name)
			}
			if tt.fail {
				err := e.Err()
				var owner *fileCloseError
				if !errors.Is(err, ErrCloseUncertain) || !errors.Is(err, modeled) || !errors.As(err, &owner) || owner.file != files[0] {
					t.Fatalf("Err = %v; want the original descriptor and Close error held", err)
				}
				if e.Ready() {
					t.Fatal("ready after an uncertain Close")
				}
				if got := e.refresh(); got != err {
					t.Fatalf("credential refresh = %v, want the held error", got)
				}
				e.refreshOIDC()
				if reads.Load() != 1 || len(rec.named(tt.name)) != 1 {
					t.Fatal("read again after an uncertain Close")
				}
				return
			}
			if e.Err() != nil {
				t.Fatalf("Err = %v after a clean Close", e.Err())
			}
			if e.oidcKeys != nil && e.oidcKeys.data != nil {
				t.Fatal("a fetch that returned after cancellation published its key set")
			}
			// The original has returned, so fresh work may follow.
			if e.oidcKeys != nil {
				if err := e.oidcKeys.fetch(context.Background()); err != nil {
					t.Fatalf("fresh fetch after the late original: %v", err)
				}
			}
			if err := e.refresh(); err != nil {
				t.Fatalf("fresh credential refresh: %v", err)
			}
			e.refreshOIDC()
			if _, err := e.Authenticate("Bearer " + f.idToken(t, nil)); err != nil {
				t.Fatalf("Authenticate after fresh reads: %v", err)
			}
		})
	}
}
