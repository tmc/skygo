package auth

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type delegationFixture struct {
	directory, path, token string
	info                   TokenInfo
	clock                  atomic.Int64
	engine                 *Engine
	actor                  Actor
	initialCleanup         func() error
	causes                 []error
}

var heldDelegationFixtures struct {
	sync.Mutex
	frames []*delegationFixture
}

func newDelegationFixture(t *testing.T) *delegationFixture {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-auth-delegation-tests")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(parent, "fixture-")
	if err != nil {
		t.Fatal(err)
	}
	f := &delegationFixture{directory: directory, path: filepath.Join(directory, "auth.json")}
	heldDelegationFixtures.Lock()
	heldDelegationFixtures.frames = append(heldDelegationFixtures.frames, f)
	heldDelegationFixtures.Unlock()
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("retaining original auth fixture and causes: %s", directory)
			return
		}
		// Initialize's rollback callback owns only the initial publication. It
		// cannot remove later credential mutations; never retry that callback.
		// The private test namespace has no open Engine handles or background Run.
		if err := os.RemoveAll(directory); err != nil {
			f.causes = append(f.causes, err)
			t.Error(err)
		}
	})
	f.initialCleanup, err = Initialize(f.path, filepath.Join(directory, "initial.token"), installation)
	f.must(t, err)
	f.must(t, AddPrincipal(f.path, "alice", Operator))
	output := filepath.Join(directory, "alice.token")
	f.info, err = Issue(f.path, "alice", output, time.Hour)
	f.must(t, err)
	f.token = readToken(t, output)
	f.clock.Store(f.info.CreatedAt.UnixNano())
	f.engine, err = newFile(f.path, installation, f.now)
	f.must(t, err)
	f.actor = requireActor(t, f.engine, f.token)
	return f
}

func (f *delegationFixture) now() time.Time {
	return time.Unix(0, f.clock.Load()).UTC()
}

func (f *delegationFixture) must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		f.causes = append(f.causes, err)
		t.Fatal(err)
	}
}

func (f *delegationFixture) delegate(t *testing.T) Delegation {
	t.Helper()
	value, err := f.engine.Delegate(f.actor)
	f.must(t, err)
	return value
}

func TestDelegationExactIdentityAndRestart(t *testing.T) {
	f := newDelegationFixture(t)
	value := f.delegate(t)
	want := Delegation{Principal: "alice", TokenID: f.info.ID, Digest: tokenHash(f.token)}
	if value != want {
		t.Fatal("delegation changed exact credential identity")
	}
	f.must(t, f.engine.CheckDelegation(value))
	// The initial file principal is named legacy; its real file credential is
	// supported. Engine mode, not that principal's spelling, sets the boundary.
	initial := requireActor(t, f.engine, readToken(t, filepath.Join(f.directory, "initial.token")))
	initialValue, err := f.engine.Delegate(initial)
	f.must(t, err)
	if initialValue.Principal != "legacy" || initialValue.TokenID == "" {
		t.Fatal("initial file admin confused with legacy engine")
	}
	f.must(t, f.engine.CheckDelegation(initialValue))
	encoded, err := json.Marshal(value)
	f.must(t, err)
	if bytes.Contains(encoded, []byte(f.token)) || bytes.Contains(encoded, []byte("Bearer ")) {
		t.Fatal("delegation contains bearer authority")
	}
	var saved Delegation
	f.must(t, json.Unmarshal(encoded, &saved))
	if saved != value {
		t.Fatal("delegation value did not survive persistence round trip")
	}
	restarted, err := newFile(f.path, installation, f.now)
	f.must(t, err)
	// No Authenticate or Actor reconstruction on this fresh Engine. This saved
	// DTO permits only CheckDelegation, not ordinary Actor-based authority.
	f.must(t, restarted.CheckDelegation(saved))
	if _, err := restarted.Delegate(f.actor); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("foreign Engine Actor delegated: %v", err)
	}
	if _, err := restarted.Check(f.actor, Mutate); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("restart adopted old Actor: %v", err)
	}
	if _, err := f.engine.Delegate(Actor{}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("zero Actor delegated: %v", err)
	}
	for _, test := range []struct {
		name   string
		change func(*Delegation)
	}{
		{"zero", func(v *Delegation) { *v = Delegation{} }},
		{"principal", func(v *Delegation) { v.Principal = "bob" }},
		{"token id", func(v *Delegation) { v.TokenID = "00000000000000000000000000000000" }},
		{"digest", func(v *Delegation) { v.Digest[0] ^= 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := value
			test.change(&changed)
			if err := f.engine.CheckDelegation(changed); !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("changed delegation accepted: %v", err)
			}
		})
	}
}

func TestDelegationCurrentRole(t *testing.T) {
	f := newDelegationFixture(t)
	value := f.delegate(t)
	for _, role := range []Role{Admin, Reader, Operator} {
		f.must(t, SetRole(f.path, "alice", role))
		f.must(t, f.engine.refresh())
		err := f.engine.CheckDelegation(value)
		next, delegateErr := f.engine.Delegate(f.actor)
		if role == Reader {
			if !errors.Is(err, ErrPermissionDenied) || !errors.Is(delegateErr, ErrPermissionDenied) || next != (Delegation{}) {
				t.Fatalf("current Reader retained mutation authority: %v, %v", err, delegateErr)
			}
		} else {
			f.must(t, err)
			f.must(t, delegateErr)
			if next != value {
				t.Fatal("role update changed delegated credential identity")
			}
		}
	}
}

func TestDelegationCredentialRevocationAndReplacement(t *testing.T) {
	for _, test := range []string{"revoked", "changed digest", "changed credential principal", "deleted principal"} {
		t.Run(test, func(t *testing.T) {
			f := newDelegationFixture(t)
			value := f.delegate(t)
			switch test {
			case "revoked":
				f.must(t, Revoke(f.path, f.info.ID))
			case "changed digest":
				writeFixture(t, f.path, func(c *config) {
					c.Generation++
					for i := range c.Tokens {
						if c.Tokens[i].ID == f.info.ID {
							hash := []byte(c.Tokens[i].Hash)
							if hash[0] == 'a' {
								hash[0] = 'b'
							} else {
								hash[0] = 'a'
							}
							c.Tokens[i].Hash = string(hash)
						}
					}
				})
			case "changed credential principal":
				f.must(t, AddPrincipal(f.path, "bob", Operator))
				writeFixture(t, f.path, func(c *config) {
					c.Generation++
					for i := range c.Tokens {
						if c.Tokens[i].ID == f.info.ID {
							c.Tokens[i].Principal = "bob"
						}
					}
				})
			case "deleted principal":
				f.must(t, Revoke(f.path, f.info.ID))
				writeFixture(t, f.path, func(c *config) {
					c.Generation++
					for i, p := range c.Principals {
						if p.ID == "alice" {
							c.Principals = append(c.Principals[:i], c.Principals[i+1:]...)
							break
						}
					}
				})
			}
			f.must(t, f.engine.refresh())
			if err := f.engine.CheckDelegation(value); !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("old credential delegation survived replacement: %v", err)
			}
			if next, err := f.engine.Delegate(f.actor); !errors.Is(err, ErrUnauthenticated) || next != (Delegation{}) {
				t.Fatalf("old Actor delegated after replacement: %v", err)
			}
		})
	}
}

func TestDelegationFreshnessExpiryAndUnavailableRecovery(t *testing.T) {
	f := newDelegationFixture(t)
	value := f.delegate(t)
	f.clock.Add(int64(3 * time.Second))
	if err := f.engine.CheckDelegation(value); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("stale delegation: %v", err)
	}
	if next, err := f.engine.Delegate(f.actor); !errors.Is(err, ErrUnavailable) || next != (Delegation{}) {
		t.Fatalf("stale Actor delegated: %v", err)
	}
	f.must(t, f.engine.refresh())
	f.must(t, f.engine.CheckDelegation(value))
	for _, instant := range []time.Time{f.info.CreatedAt.Add(-time.Nanosecond), f.info.ExpiresAt} {
		f.clock.Store(instant.UnixNano())
		f.must(t, f.engine.refresh())
		if err := f.engine.CheckDelegation(value); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("unusable credential delegation: %v", err)
		}
		if next, err := f.engine.Delegate(f.actor); !errors.Is(err, ErrUnauthenticated) || next != (Delegation{}) {
			t.Fatalf("unusable credential Actor delegated: %v", err)
		}
	}
	f.clock.Store(f.info.CreatedAt.UnixNano())
	f.must(t, f.engine.refresh())
	original, err := os.ReadFile(f.path)
	f.must(t, err)
	for _, missing := range []bool{false, true} {
		if missing {
			f.must(t, os.Remove(f.path))
		} else {
			f.must(t, os.WriteFile(f.path, []byte("{"), 0600))
		}
		if err := f.engine.refresh(); err == nil {
			t.Fatal("unavailable credential snapshot accepted")
		} else {
			f.causes = append(f.causes, err)
		}
		if err := f.engine.CheckDelegation(value); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("unavailable delegation: %v", err)
		}
		if next, err := f.engine.Delegate(f.actor); !errors.Is(err, ErrUnavailable) || next != (Delegation{}) {
			t.Fatalf("unavailable Actor delegated: %v", err)
		}
		f.must(t, os.WriteFile(f.path, original, 0600))
		f.must(t, f.engine.refresh())
		f.must(t, f.engine.CheckDelegation(value))
	}
}

func TestDelegationLegacyUnsupported(t *testing.T) {
	f := newDelegationFixture(t)
	value := f.delegate(t)
	for _, token := range []string{"", "local-test-credential"} {
		e := NewLegacy(token)
		header := ""
		if token != "" {
			header = "Bearer " + token
		}
		a, err := e.Authenticate(header)
		f.must(t, err)
		if next, err := e.Delegate(a); !errors.Is(err, ErrUnauthenticated) || next != (Delegation{}) {
			t.Fatalf("legacy delegation accepted: %v", err)
		}
		if err := e.CheckDelegation(value); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("legacy engine checked file delegation: %v", err)
		}
		// Ordinary legacy behavior remains supported and unchanged.
		if _, err := e.Check(a, Mutate); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDelegationRotationKeepsCredentialIdentity(t *testing.T) {
	f := newDelegationFixture(t)
	old := f.delegate(t)
	output := filepath.Join(f.directory, "rotated.token")
	info, err := Issue(f.path, "alice", output, time.Hour)
	f.must(t, err)
	f.clock.Store(info.CreatedAt.UnixNano())
	f.must(t, f.engine.refresh())
	actor := requireActor(t, f.engine, readToken(t, output))
	rotated, err := f.engine.Delegate(actor)
	f.must(t, err)
	if rotated.Principal != old.Principal || rotated.TokenID != info.ID || rotated.TokenID == old.TokenID || rotated.Digest == old.Digest {
		t.Fatal("rotation conflated credentials or changed principal ownership")
	}
	f.must(t, f.engine.CheckDelegation(old))
	f.must(t, f.engine.CheckDelegation(rotated))
	f.must(t, Revoke(f.path, old.TokenID))
	f.must(t, f.engine.refresh())
	if err := f.engine.CheckDelegation(old); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revoked delegation adopted replacement: %v", err)
	}
	f.must(t, f.engine.CheckDelegation(rotated))
}
