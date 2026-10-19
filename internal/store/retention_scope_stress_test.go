//go:build darwin || linux

package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// This opt-in corpus exercises Store/auth integration. It neither starts a
// coordinator nor supplies backend execution or native cleanup evidence.
func TestRetentionScopeStress(t *testing.T) {
	root := os.Getenv("SKYGO_SCOPE_STRESS_DIR")
	if root == "" {
		t.Skip("set SKYGO_SCOPE_STRESS_DIR to an empty private directory")
	}
	scopeStressPrivate(t, root, true)
	evidence := os.Getenv("SKYGO_SCOPE_STRESS_EVIDENCE")
	if evidence == "" {
		t.Fatal("SKYGO_SCOPE_STRESS_EVIDENCE must name a new external JSONL file")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err = filepath.Abs(evidence)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(root, evidence)
	if err != nil || rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		t.Fatal("evidence must be outside the fixture directory")
	}
	file, err := os.OpenFile(evidence, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if file != nil {
			if err := file.Sync(); err != nil {
				t.Error(err)
			}
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	rec := &scopeStressRecorder{root: root, started: time.Now(), output: json.NewEncoder(file)}
	rec.guard(t)
	if free := scopeStressFree(t, root); free < 8<<30 {
		t.Fatalf("fixture free bytes=%d; need at least 8 GiB", free)
	}
	data := filepath.Join(root, "data")
	s, err := Open(data)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	owner := s.Owner()
	task := backend.Task{Argv: []string{"scope-corpus-never-executed"}}
	bootstrap := auth.NewLegacy("scope-corpus-bootstrap")
	actor, err := bootstrap.Authenticate("Bearer scope-corpus-bootstrap")
	if err == nil {
		actor, err = bootstrap.Check(actor, auth.Mutate)
	}
	if err != nil {
		t.Fatal(err)
	}
	rec.guard(t)
	legacy, err := s.Submit(actor, "legacy-scope", task, "remote", "scope-binding")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	authRoot := filepath.Join(root, "auth")
	scopeStressMkdir(t, authRoot)
	var running *scopeStressEngine
	defer func() {
		if running != nil {
			running.stop(t)
		}
	}()
	replace := func(config string) {
		if running != nil {
			running.stop(t)
		}
		running = scopeStressStartEngine(t, config, owner)
	}
	var batches []scopeStressBatch
	for batch := 0; batch < 9; batch++ {
		dir := filepath.Join(authRoot, fmt.Sprintf("batch-%02d", batch))
		scopeStressMkdir(t, dir)
		b := scopeStressBatch{config: filepath.Join(dir, "auth.json"), admin: filepath.Join(dir, "admin.token")}
		rec.guard(t)
		if _, err := auth.Initialize(b.config, b.admin, owner); err != nil {
			t.Fatal(err)
		}
		count := 127
		if batch == 8 {
			count = 8 // Seven admitted principals, then the 1025th scope.
		}
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("scope-%04d", batch*127+i)
			path := filepath.Join(dir, id+".token")
			rec.guard(t)
			if err := auth.AddPrincipal(b.config, id, auth.Operator); err != nil {
				t.Fatal(err)
			}
			rec.guard(t)
			info, err := auth.Issue(b.config, id, path, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			b.principals = append(b.principals, scopeStressPrincipal{id: id, token: path, tokenID: info.ID})
		}
		batches = append(batches, b)
		config, err := os.ReadFile(b.config)
		if err != nil {
			t.Fatal(err)
		}
		var metadata struct {
			Installation string            `json:"installation"`
			Generation   uint64            `json:"generation"`
			Principals   []json.RawMessage `json:"principals"`
			Tokens       []json.RawMessage `json:"tokens"`
		}
		if err := json.Unmarshal(config, &metadata); err != nil {
			t.Fatal(err)
		}
		if metadata.Installation != owner || metadata.Generation == 0 || len(metadata.Principals) != count+1 || len(metadata.Tokens) != count+1 || len(metadata.Principals) > 128 || len(metadata.Tokens) > 256 {
			t.Fatal("actual auth config violates its installed authority bounds")
		}
		if err := rec.output.Encode(map[string]any{"phase": "auth_config", "batch": batch, "config_sha256": fmt.Sprintf("%x", sha256.Sum256(config)), "installation": metadata.Installation, "generation": metadata.Generation, "principals": len(metadata.Principals), "tokens": len(metadata.Tokens)}); err != nil {
			t.Fatal(err)
		}
	}
	// Activation is stopped-state and uses the prepared installation-bound file.
	replace(batches[0].config)
	admin := func() auth.Actor { return running.actor(t, batches[running.batch].admin, auth.Mutate) }
	s, err = Open(data)
	if err != nil {
		t.Fatal(err)
	}
	rec.guard(t)
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	policy := DefaultRetentionPolicy()
	rec.guard(t)
	info, err := EnableRetention(data, policy)
	if err != nil {
		t.Fatal(err)
	}
	// Only Store request phases use the fixed clock; auth and disk use real time.
	clock := info.AcceptUntil.Add(-policy.Window)
	open := func() {
		var err error
		s, err = Open(data)
		if err != nil {
			t.Fatal(err)
		}
		s.now = func() time.Time { return clock }
		if s.Owner() != owner || !s.Scoped() {
			t.Fatal("reopen changed owner or scoped mode")
		}
	}
	open()
	expected := map[string]bool{"legacy": true}
	scopeStressScopes(t, s, expected)
	rec.sample(t, s, admin(), "activated", owner, "", "")
	var jobs []backend.Job
	var first backend.Job
	for batch, b := range batches {
		replace(b.config)
		running.batch = batch
		for i, principal := range b.principals {
			if batch == 8 && i == 7 {
				break
			}
			actor := running.actor(t, principal.token, auth.Mutate)
			if actor.Principal() != principal.id || actor.Role() != auth.Operator {
				t.Fatal("credential did not authenticate its configured principal")
			}
			rec.guard(t)
			j, err := s.SubmitRetained(actor, info.Domain, "scope-request", task, "remote", "scope-binding", backend.RemoteOutput, 0)
			if err != nil {
				t.Fatalf("principal %s: %v", principal.id, err)
			}
			if len(jobs) == 0 {
				first = j
			}
			jobs = append(jobs, j)
			expected[principal.id] = true
			rec.sample(t, s, admin(), "accepted", owner, principal.id, principal.tokenID)
		}
		scopeStressScopes(t, s, expected)
	}
	if len(expected) != 1024 || len(jobs) != 1023 {
		t.Fatal("did not reach the default lifetime scope maximum")
	}
	refuse := func(domain RequestDomain, phase string) {
		replace(batches[8].config)
		running.batch = 8
		principal := batches[8].principals[7]
		actor := running.actor(t, principal.token, auth.Mutate)
		before := routingState(t, s.db)
		rec.guard(t)
		_, err := s.SubmitRetained(actor, domain, "refused-scope", task, "remote", "scope-binding", backend.RemoteOutput, 0)
		var capacity *CapacityError
		if !errors.As(err, &capacity) || capacity.Domain != PrincipalScopesQuota || capacity.Used != 1024 || capacity.Limit != 1024 {
			t.Fatalf("scope refusal=%v (%+v)", err, capacity)
		}
		if !bytes.Equal(before, routingState(t, s.db)) {
			t.Fatal("refused scope changed durable rows, counters or sequences")
		}
		if err := rec.output.Encode(map[string]any{"phase": "quota_error", "at": phase, "domain": capacity.Domain, "used": capacity.Used, "limit": capacity.Limit, "principal": principal.id, "token_id": principal.tokenID}); err != nil {
			t.Fatal(err)
		}
		scopeStressScopes(t, s, expected)
		rec.sample(t, s, admin(), phase, owner, principal.id, principal.tokenID)
	}
	refuse(info.Domain, "full_refusal")
	// Reauthenticate with the original real config; no stale or foreign Actor.
	replace(batches[0].config)
	firstActor := running.actor(t, batches[0].principals[0].token, auth.Mutate)
	before := routingState(t, s.db)
	rec.guard(t)
	replay, err := s.SubmitRetained(firstActor, info.Domain, "scope-request", first.Task, "", "", backend.UnspecifiedOutput, 0)
	if err != nil || !reflect.DeepEqual(replay, first) || !bytes.Equal(before, routingState(t, s.db)) {
		t.Fatalf("full-scope replay changed binding/state: %v", err)
	}
	// These modern remote jobs never started. Actual cancellation and Whole mark
	// require no stopped/native proof, callback, output receipt or manual closure.
	clock = clock.Add(time.Second)
	for _, j := range jobs {
		rec.guard(t)
		_, err := s.UpdateFor(admin(), j.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil })
		if err != nil {
			t.Fatal(err)
		}
		rec.guard(t)
		canceled, start, err := s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, clock)
		if err != nil || start || canceled.Status.State != backend.Canceled {
			t.Fatalf("never-started cancellation: start=%t err=%v", start, err)
		}
	}
	max, err := s.LastSequenceFor(admin())
	if err != nil {
		t.Fatal(err)
	}
	selection := PruneSelection{Before: clock.Add(time.Second), Mode: Whole, Max: max, Limit: 100}
	var marked uint32
	for page := 0; page < 20; page++ {
		rec.guard(t)
		p, err := s.Prune(admin(), selection, true, nil, nil, clock)
		if err != nil {
			t.Fatal(err)
		}
		marked += p.Marked
		rec.sample(t, s, admin(), "whole_prune", owner, "", "")
		if !p.More {
			break
		}
		if p.After <= selection.After {
			t.Fatal("prune made no cursor progress")
		}
		selection.After = p.After
	}
	if marked != 1023 {
		t.Fatalf("marked=%d; want all 1023 never-started operator jobs", marked)
	}
	pruned, err := s.GetFor(admin(), first.ID)
	var expired *ExpiryError
	if !errors.As(err, &expired) || expired.Reason != ResultExpired || expired.JobID != first.ID || !reflect.DeepEqual(pruned, backend.Job{}) {
		t.Fatalf("pruned job read: job=%+v error=%v; want exact result expiry and no job", pruned, err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		jobs := tx.Bucket(jobsBucket)
		if jobs == nil {
			return errors.New("canonical jobs bucket missing")
		}
		if jobs.Get([]byte(first.ID)) != nil {
			return errors.New("pruned job remains in canonical jobs bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	firstActor = running.actor(t, batches[0].principals[0].token, auth.Read)
	if _, found, err := s.LookupRetained(firstActor, info.Domain, "scope-request", first.Task); !found || !scopeStressExpiry(err, ResultExpired) {
		t.Fatalf("pruned request did not retain exact expiry: found=%t err=%v", found, err)
	}
	if _, found, err := s.NextCleanup(0); err != nil || found {
		t.Fatalf("never-started remote prune invented native/output cleanup: %t %v", found, err)
	}
	scopeStressScopes(t, s, expected)
	refuse(info.Domain, "after_prune_refusal")
	// Seal the actual request/job set before changing only Store's phase clock.
	sealed, err := s.RetentionInfo(admin())
	if err != nil {
		t.Fatal(err)
	}
	remaining := scopeStressUsed(t, sealed, RequestRecordsQuota)
	jobsRemaining := scopeStressUsed(t, sealed, JobsQuota)
	if remaining != 1024 || jobsRemaining != 1 {
		t.Fatal("unexpected sealed scope corpus", remaining, jobsRemaining)
	}
	// Every nonempty call must remove a request; RequestsZero needs at most
	// R such calls. R+1 is defensive and does not extend the wall/disk guards.
	callLimit := remaining + 1
	clock = info.RetryUntil.Add(time.Second)
	for pass := uint64(0); pass < callLimit; pass++ {
		rec.guard(t)
		if err := s.MaintainRetention(clock); err != nil {
			t.Fatal(err)
		}
		current := rec.sample(t, s, admin(), "request_retirement", owner, "", "")
		next := scopeStressUsed(t, current, RequestRecordsQuota)
		if next > remaining || next == remaining || remaining-next > 50 {
			t.Fatal("closed scope retirement made invalid progress", remaining, next)
		}
		remaining = next
		if next == 0 {
			break
		}
	}
	current, err := s.RetentionInfo(admin())
	if err != nil || scopeStressUsed(t, current, RequestRecordsQuota) != 0 || current.Domain.Epoch == info.Domain.Epoch {
		t.Fatalf("bounded request retirement did not finish: %v", err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		var metadata retainedJob
		if err := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(legacy.ID), &metadata); err != nil {
			return err
		}
		if !metadata.LegacyRequestRetired || metadata.LegacyRequestID != nil {
			return errors.New("closed bootstrap identity was not retired")
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	open()
	scopeStressScopes(t, s, expected)
	if _, err := s.GetFor(admin(), legacy.ID); err != nil {
		t.Fatalf("phase maintenance removed protected legacy job: %v", err)
	}
	refuse(current.Domain, "retired_reopened_refusal")
	if err := s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
	rec.sample(t, s, admin(), "final", owner, "", "")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	running.stop(t)
	running = nil
	if err := rec.output.Encode(map[string]any{"phase": "complete", "passed": true, "scopes": 1024, "accepted_operator_jobs": 1023, "guarded_steps": rec.steps, "seconds": time.Since(rec.started).Seconds()}); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	file = nil
}

type scopeStressPrincipal struct{ id, token, tokenID string }
type scopeStressBatch struct {
	config, admin string
	principals    []scopeStressPrincipal
}
type scopeStressEngine struct {
	engine *auth.Engine
	cancel context.CancelFunc
	done   <-chan struct{}
	batch  int
}

func scopeStressStartEngine(t *testing.T, path, owner string) *scopeStressEngine {
	t.Helper()
	e, err := auth.NewFile(path, owner)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); e.Run(ctx) }()
	return &scopeStressEngine{engine: e, cancel: cancel, done: done}
}
func (e *scopeStressEngine) stop(t *testing.T) {
	t.Helper()
	e.cancel()
	select {
	case <-e.done:
	case <-time.After(5 * time.Second):
		t.Fatal("authentication refresher did not join; no replacement allowed")
	}
}
func (e *scopeStressEngine) actor(t *testing.T, path string, action auth.Action) auth.Actor {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	a, err := e.engine.Authenticate("Bearer " + strings.TrimSpace(string(data)))
	if err == nil {
		a, err = e.engine.Check(a, action)
	}
	if err != nil {
		t.Fatalf("fresh credential check: %v", err)
	}
	return a
}

type scopeStressRecorder struct {
	root    string
	started time.Time
	steps   int
	output  *json.Encoder
}

func (r *scopeStressRecorder) guard(t *testing.T) {
	t.Helper()
	r.steps++
	if r.steps > 10000 || time.Since(r.started) > 15*time.Minute {
		t.Fatal("scope corpus exceeded guarded-step or wall budget")
	}
	if scopeStressFree(t, r.root) < 2<<30 {
		t.Fatal("scope corpus reached the 2 GiB free-space floor")
	}
}
func (r *scopeStressRecorder) sample(t *testing.T, s *Store, actor auth.Actor, phase, owner, principal, tokenID string) RetentionInfo {
	t.Helper()
	r.guard(t)
	info, err := s.RetentionInfo(actor)
	if err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(s.db.Path())
	if err != nil {
		t.Fatal(err)
	}
	native, ok := stat.Sys().(*syscall.Stat_t)
	if !ok || native.Blocks < 0 || uint64(native.Blocks) > math.MaxUint64/512 || stat.Size() < 0 || stat.Size() >= 320<<20 {
		t.Fatal("invalid allocation sample or database reached 320 MiB HOLD")
	}
	event := map[string]any{"phase": phase, "owner": owner, "principal": principal, "token_id": tokenID, "usage": info.Usage, "database_file_bytes": stat.Size(), "database_allocated_bytes": uint64(native.Blocks) * 512, "filesystem_free_bytes": scopeStressFree(t, r.root), "bolt_stats": s.db.Stats(), "guarded_steps": r.steps, "seconds": time.Since(r.started).Seconds()}
	if err := r.output.Encode(event); err != nil {
		t.Fatal(err)
	}
	return info
}
func scopeStressUsed(t *testing.T, info RetentionInfo, domain QuotaDomain) uint64 {
	t.Helper()
	for _, u := range info.Usage {
		if u.Domain == domain {
			return u.Used
		}
	}
	t.Fatalf("missing quota domain %d", domain)
	return 0
}
func scopeStressScopes(t *testing.T, s *Store, expected map[string]bool) {
	t.Helper()
	if err := s.db.View(func(tx *bolt.Tx) error {
		seen := 0
		if err := retentionTable(tx, retainedScopesBucket).ForEach(func(k, v []byte) error {
			if v == nil || !expected[string(k)] {
				return errors.New("unexpected permanent scope row")
			}
			seen++
			return nil
		}); err != nil {
			return err
		}
		if seen != len(expected) {
			return errors.New("authenticated scopes missing from permanent index")
		}
		u, err := usage(tx)
		if err != nil || u[PrincipalScopesQuota] != uint64(seen) {
			return errors.New("scope quota disagrees with actual permanent rows")
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
}
func scopeStressExpiry(err error, reason ExpiryReason) bool {
	var expired *ExpiryError
	return errors.As(err, &expired) && expired.Reason == reason
}
func scopeStressFree(t *testing.T, path string) uint64 {
	t.Helper()
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil || stat.Bsize <= 0 {
		t.Fatalf("statfs: %v", err)
	}
	if uint64(stat.Bavail) > math.MaxUint64/uint64(stat.Bsize) {
		t.Fatal("statfs free byte multiplication overflow")
	}
	return uint64(stat.Bavail) * uint64(stat.Bsize)
}
func scopeStressMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	scopeStressPrivate(t, path, true)
}
func scopeStressPrivate(t *testing.T, path string, empty bool) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 {
		t.Fatalf("fixture must be an existing private 0700 directory: %s", path)
	}
	if empty {
		entries, err := os.ReadDir(path)
		if err != nil || len(entries) != 0 {
			t.Fatalf("fixture directory must be empty: %s", path)
		}
	}
}
