package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ExampleNewFileOIDC is in package auth so that the tests below can model a
// held Engine on the example's own path. A caller outside the package writes
// auth.NewFileOIDC.
func ExampleNewFileOIDC() {
	const installation = "0123456789abcdef0123456789abcdef"
	x := newExampleOIDC(installation)
	engine, err := NewFileOIDC(x.auth, installation, x.oidc, "")
	if err != nil {
		// A non-nil engine holds the original credential file.
		x.stop(engine, err)
	}
	changed := engine.Changed()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		engine.Run(ctx)
	}()
	// Run's first OIDC read publishes the OIDC snapshot.
	select {
	case <-changed:
	case <-time.After(10 * time.Second):
	}
	actor, err := engine.Authenticate("Bearer " + x.token)
	cancel()
	<-stopped
	// Run has returned. The files may go only if the Engine holds nothing.
	if held := engine.Err(); held != nil {
		x.stop(engine, held)
	}
	x.remove()
	fmt.Println(actor.Principal(), actor.Role(), err)
	// Output: alice operator <nil>
}

// exampleOIDC is the example's private directory: a credential file naming
// alice an operator, an OIDC file mapping alice-subject to alice, a JWKS file
// with one local issuer key, and an ID token that key signed. The credential
// file is written directly, not through readConfig, so that a test can model
// a failure of the constructor's own read.
type exampleOIDC struct {
	dir, auth, oidc string
	token           string
}

// exampleHold is the example's terminal panic value when its Engine holds an
// error: the Engine, its error, which retains the original file or response
// body, and the directory left in place for them. A real caller keeps these
// and stops; the example stops by panicking with all three rather than with
// the error alone, which would drop the Engine.
type exampleHold struct {
	engine *Engine
	err    error
	dir    string
}

// exampleRemoveAll removes the example's directory. Tests replace it to
// record removals.
var exampleRemoveAll = os.RemoveAll

func newExampleOIDC(installation string) *exampleOIDC {
	dir, err := os.MkdirTemp("", "skygo-auth-oidc-example-")
	if err != nil {
		panic(err)
	}
	x := &exampleOIDC{dir: dir, auth: filepath.Join(dir, "auth.json"), oidc: filepath.Join(dir, "oidc.json")}
	if err := x.write(installation); err != nil {
		x.remove()
		panic(err)
	}
	return x
}

func (x *exampleOIDC) write(installation string) error {
	// A local issuer key; the Engine is given only its public half.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	creds, err := encode(&config{Version: 1, Installation: installation, Generation: 1, Principals: []principal{{"legacy", Admin}, {"alice", Operator}}})
	if err != nil {
		return err
	}
	b64 := base64.RawURLEncoding.EncodeToString
	keys, err := json.Marshal(map[string]any{"keys": []map[string]string{{
		"kty": "EC", "crv": "P-256",
		"x": b64(key.X.FillBytes(make([]byte, 32))), "y": b64(key.Y.FillBytes(make([]byte, 32))),
	}}})
	if err != nil {
		return err
	}
	settings, err := json.Marshal(map[string]any{
		"version": 1, "installation": installation, "generation": 1,
		"issuer": "https://issuer.example", "audience": "skygo", "jwks_file": "keys.json",
		"identities": []map[string]string{{"subject": "alice-subject", "principal": "alice"}},
	})
	if err != nil {
		return err
	}
	for _, f := range []struct {
		path string
		data []byte
		perm os.FileMode
	}{{x.auth, creds, 0600}, {filepath.Join(x.dir, "keys.json"), keys, 0644}, {x.oidc, settings, 0600}} {
		if err := os.WriteFile(f.path, f.data, f.perm); err != nil {
			return err
		}
	}

	// An ID token as the issuer would sign it.
	now := time.Now().Unix()
	claims, err := json.Marshal(map[string]any{"iss": "https://issuer.example", "sub": "alice-subject", "aud": "skygo", "iat": now, "exp": now + 300})
	if err != nil {
		return err
	}
	input := b64([]byte(`{"alg":"ES256"}`)) + "." + b64(claims)
	digest := sha256.Sum256([]byte(input))
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		return err
	}
	x.token = input + "." + b64(append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...))
	return nil
}

// stop stops the example after a failure. An Engine returned with err, or
// holding it, retains an original file or response body, so the directory
// is kept and stop panics with an exampleHold; without an Engine nothing is
// held, so stop removes the directory and panics with err.
func (x *exampleOIDC) stop(engine *Engine, err error) {
	if engine != nil {
		panic(&exampleHold{engine: engine, err: err, dir: x.dir})
	}
	x.remove()
	panic(err)
}

func (x *exampleOIDC) remove() {
	if err := exampleRemoveAll(x.dir); err != nil {
		panic(err)
	}
}

// runExampleNewFileOIDC runs ExampleNewFileOIDC itself, recording each
// directory it removes, and recovers only an exampleHold.
func runExampleNewFileOIDC(t *testing.T) (removed []string, hold *exampleHold) {
	t.Helper()
	t.Cleanup(func() { exampleRemoveAll = os.RemoveAll })
	exampleRemoveAll = func(dir string) error {
		removed = append(removed, dir)
		return os.RemoveAll(dir)
	}
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		h, ok := r.(*exampleHold)
		if !ok {
			panic(r)
		}
		hold = h
	}()
	ExampleNewFileOIDC()
	return removed, hold
}

// TestExampleNewFileOIDCDisposition runs the example three ways. A clean run
// stops and joins Run, finds no held error and removes its directory exactly
// once. When the constructor's credential file Close, or Run's JWKS file
// Close, fails, the example removes nothing and stops with an exampleHold
// carrying the same Engine and its held error, which retains the original
// descriptor; on the Run path the example has joined Run first. The Close
// failures are MODELED: each native Close succeeded, so once the example has
// stopped the test may remove the held directory itself.
func TestExampleNewFileOIDCDisposition(t *testing.T) {
	modeled := errors.New("modeled close failure")
	t.Run("clean", func(t *testing.T) {
		creds := modelCloseFailure(t, nil)
		files := modelOIDCCloseFailure(t, "", nil)
		removed, hold := runExampleNewFileOIDC(t)
		if hold != nil {
			os.RemoveAll(hold.dir)
			t.Fatalf("a clean run held %v", hold.err)
		}
		if len(removed) != 1 {
			t.Fatalf("removed %d directories, want 1", len(removed))
		}
		if _, err := os.Lstat(removed[0]); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("directory after a clean run: %v", err)
		}
		if len(*creds) == 0 || len(files.named("keys.json")) == 0 {
			t.Fatal("the example read neither the credential file nor its keys")
		}
	})
	t.Run("constructor held", func(t *testing.T) {
		closed := modelCloseFailure(t, modeled)
		removed, hold := runExampleNewFileOIDC(t)
		if hold == nil {
			t.Fatal("no exampleHold after an uncertain constructor Close")
		}
		t.Cleanup(func() { os.RemoveAll(hold.dir) })
		if len(removed) != 0 {
			t.Fatalf("removed %d directories of a held Engine", len(removed))
		}
		var owner *fileCloseError
		if hold.engine == nil || hold.engine.Err() != hold.err || !errors.Is(hold.err, ErrCloseUncertain) || !errors.Is(hold.err, modeled) {
			t.Fatalf("hold = %v; want the partial Engine and its held error", hold.err)
		}
		if !errors.As(hold.err, &owner) || len(*closed) != 1 || owner.file != (*closed)[0] {
			t.Fatalf("held error does not retain the one original descriptor (%d closes)", len(*closed))
		}
		if _, err := os.Lstat(filepath.Join(hold.dir, "auth.json")); err != nil {
			t.Fatalf("held directory: %v", err)
		}
	})
	t.Run("run held", func(t *testing.T) {
		rec := modelOIDCCloseFailure(t, "keys.json", modeled)
		removed, hold := runExampleNewFileOIDC(t)
		if hold == nil {
			t.Fatal("no exampleHold after an uncertain JWKS file Close")
		}
		t.Cleanup(func() { os.RemoveAll(hold.dir) })
		if len(removed) != 0 {
			t.Fatalf("removed %d directories of a held Engine", len(removed))
		}
		if hold.engine == nil || hold.engine.Err() != hold.err || !errors.Is(hold.err, ErrCloseUncertain) || !errors.Is(hold.err, modeled) || hold.engine.Ready() {
			t.Fatalf("hold = %v; want the Engine and its held error", hold.err)
		}
		var owner *fileCloseError
		files := rec.named("keys.json")
		if !errors.As(hold.err, &owner) || len(files) != 1 || owner.file != files[0] {
			t.Fatalf("held error does not retain the one original descriptor (%d closes)", len(files))
		}
		if _, err := os.Lstat(filepath.Join(hold.dir, "keys.json")); err != nil {
			t.Fatalf("held directory: %v", err)
		}
	})
}
