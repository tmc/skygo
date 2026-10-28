package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// The first two tests drive one item of Store.Prune's real planner
// (newRetentionMutation, artifactLoad/clone, the mode's selection/mark steps,
// prepareArtifactChange, commit/apply) inside real Store transactions; the
// remote test composes the same seam with a real object-delete mark (see its
// comment). In each the only MODELED input is one
// raw prospective plan row whose task carries a "workers" member. backend.Task
// has no such field, so only the decode guard can see it; the paired null twin
// differs from it in that one value. Workspace/clock receipts come from the
// existing regression fixtures and are MODELED, not native.
//
// The planner has no provider callback or I/O boundary: prepareArtifactChange
// only reads tx and the plan. A full logical Store snapshot before and after
// is therefore the no-effects oracle; no callback witness is used.

const plannerCorruptError = "invalid artifact metadata"

// plannerForecast is the result of one planned artifact forecast.
type plannerForecast struct {
	plan     *retentionMutation
	prepared artifactPrepared
	planned  []byte // the planned row before any model replacement
	err      error  // prepareArtifactChange's result
}

// plannerBystander returns every logical Store entry whose path or value
// names job id: its job row, principal and index rows, retained job metadata
// (its retention accounting record) and any remote attempt row. It requires
// the job row and the job_metadata record to be present.
func plannerBystander(t *testing.T, s *Store, id string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for k, v := range plannerStoreSnapshot(t, s) {
		if strings.Contains(k, id) || strings.Contains(v, id) {
			out[k] = v
		}
	}
	for _, k := range []string{"jobs/" + id, "retention/job_metadata/" + id} {
		if _, ok := out[k]; !ok {
			t.Fatalf("bystander has no %s entry", k)
		}
	}
	return out
}

// plannerSubmitBystander submits a distinct ordinary retained job through
// the public Store API and returns it with its logical baseline.
func plannerSubmitBystander(t *testing.T, s *Store) (backend.Job, map[string]string) {
	t.Helper()
	info, err := s.RetentionInfo(testActor())
	if err != nil {
		t.Fatal(err)
	}
	j, err := s.SubmitRetained(testActor(), info.Domain, "planner-bystander", workersPlain(), "remote", "test-binding", backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	return j, plannerBystander(t, s, j.ID)
}

// plannerBystanderSame requires the bystander's entries and public job to be
// unchanged.
func plannerBystanderSame(t *testing.T, s *Store, j backend.Job, want map[string]string) {
	t.Helper()
	if got := plannerBystander(t, s, j.ID); !maps.Equal(got, want) {
		t.Fatalf("bystander %s changed", j.ID)
	}
	got, err := s.Get(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	plannerSame(t, "bystander", got, j)
}

// plannerSnapshot returns every key, value and bucket sequence in tx.
func plannerSnapshot(tx *bolt.Tx) (map[string]string, error) {
	out := make(map[string]string)
	var walk func(prefix string, b *bolt.Bucket) error
	walk = func(prefix string, b *bolt.Bucket) error {
		out[prefix+"#sequence"] = fmt.Sprint(b.Sequence())
		return b.ForEach(func(k, v []byte) error {
			name := prefix + "/" + string(k)
			if v == nil {
				if c := b.Bucket(k); c != nil {
					return walk(name, c)
				}
			}
			out[name] = string(v)
			return nil
		})
	}
	err := tx.ForEach(func(name []byte, b *bolt.Bucket) error { return walk(string(name), b) })
	return out, err
}

func plannerStoreSnapshot(t *testing.T, s *Store) map[string]string {
	t.Helper()
	var out map[string]string
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		out, err = plannerSnapshot(tx)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// plannerWrites returns the prepared artifact writes keyed by table and key.
func plannerWrites(p artifactPrepared) map[string]string {
	out := map[string]string{"usage": string(p.usage)}
	for _, w := range p.writes {
		v := "delete"
		if w.value != nil {
			v = "put " + string(w.value)
		}
		out[w.table+"/"+w.key] = v
	}
	return out
}

// plannerModelJob returns the stored job row raw with a workers member
// inserted at the start of its task object.
func plannerModelJob(t *testing.T, raw []byte, workers string) []byte {
	t.Helper()
	marker := []byte(`"task":{`)
	if bytes.Count(raw, marker) != 1 {
		t.Fatalf("job row has %d task objects", bytes.Count(raw, marker))
	}
	return bytes.Replace(raw, marker, []byte(`"task":{"workers":`+workers+`,`), 1)
}

// plannerModelHeader returns the taskless header row raw with a duplicate
// task member first, holding workers, and "task":null last. The last member
// wins in json.Unmarshal, so the decoded header keeps Task == nil.
func plannerModelHeader(t *testing.T, raw []byte, workers string) []byte {
	t.Helper()
	if len(raw) < 2 || raw[0] != '{' || raw[len(raw)-1] != '}' || bytes.Contains(raw, []byte(`"task"`)) {
		t.Fatalf("unexpected planned header %s", raw)
	}
	var b bytes.Buffer
	b.WriteString(`{"task":{"argv":["true"],"workers":` + workers + `},`)
	b.Write(raw[1 : len(raw)-1])
	b.WriteString(`,"task":null}`)
	return b.Bytes()
}

// plannerRefused requires the exact mapped refusal: artifactCorrupt, not the
// guard's own errWorkerRoute chain, and no prepared writes.
func plannerRefused(t *testing.T, f plannerForecast) {
	t.Helper()
	if f.err == nil || f.err.Error() != plannerCorruptError || errors.Is(f.err, errWorkerRoute) {
		t.Fatalf("prepare = %v, want exactly %q", f.err, plannerCorruptError)
	}
	if len(f.prepared.writes) != 0 || f.prepared.usage != nil {
		t.Fatalf("refusal prepared writes %v", plannerWrites(f.prepared))
	}
}

// plannerSame requires two marshaled values to be equal.
func plannerSame(t *testing.T, what string, got, want any) {
	t.Helper()
	g, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	w, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(g, w) {
		t.Fatalf("%s decoded to %s, want %s", what, g, w)
	}
}

// A pinned job's row read through the plan (artifactValidateCorePlanned's
// jobRow via plan.coreGet -> decodeStoredJob) maps a workers refusal to
// artifactCorrupt in the real ResultOnly planner item.
func TestArtifactPlannedJobPinWorkersRefused(t *testing.T) {
	s, job, input := artifactRegressionWorkspace(t, true, true)
	ref := artifactRegressionPublish(t, s, job)
	other, otherBefore := plannerSubmitBystander(t, s)
	now := s.now()
	attempt := job.CurrentAttemptID()
	sel := PruneSelection{Before: now.Add(time.Hour), Mode: ResultOnly, Attempt: 1}

	// forecast follows Store.Prune's ResultOnly item. row, if non-nil, is the
	// MODELED prospective job row placed in the plan with corePut.
	forecast := func(tx *bolt.Tx, row []byte) (out plannerForecast, err error) {
		if out.plan, err = newRetentionMutation(tx); err != nil {
			return out, err
		}
		initial, err := artifactLoad(tx)
		if err != nil {
			return out, err
		}
		trial := initial.clone()
		x, err := artifactPruneSubject(tx, job.ID, sel.Mode)
		if err != nil {
			return out, err
		}
		reason, err := artifactResultSelection(tx, trial, x, sel, map[string]bool{}, now)
		if err != nil {
			return out, err
		}
		if reason != "" {
			return out, fmt.Errorf("result selection skipped: %s", reason)
		}
		out.planned = bytes.Clone(out.plan.coreGet(jobsBucket, []byte(job.ID)))
		if row != nil {
			out.plan.corePut(jobsBucket, []byte(job.ID), row)
		}
		n, err := out.plan.changedBytes()
		if err != nil {
			return out, err
		}
		out.prepared, out.err = prepareArtifactChange(tx, initial, trial, out.plan, maxChangeBytes-n)
		return out, nil
	}
	view := func(row []byte) plannerForecast {
		t.Helper()
		var f plannerForecast
		if err := s.db.View(func(tx *bolt.Tx) error {
			var err error
			f, err = forecast(tx, row)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return f
	}

	var pin artifactPin
	var orig []byte
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		var ok bool
		if pin, ok = st.JobInputs[job.ID]; !ok || pin.Ref != input {
			return errors.New("fixture job has no input pin")
		}
		if _, ok := st.Results[attempt]; !ok {
			return errors.New("fixture has no published result")
		}
		orig = bytes.Clone(tx.Bucket(jobsBucket).Get([]byte(job.ID)))
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
	nullRow := plannerModelJob(t, orig, "null")
	badRow := plannerModelJob(t, orig, `{"count":2,"mode":"tcp"}`)
	var want, got, plain backend.Job
	if err := decodeStoredJob(orig, &want); err != nil {
		t.Fatal(err)
	}
	if err := decodeStoredJob(nullRow, &got); err != nil {
		t.Fatalf("null twin refused: %v", err)
	}
	plannerSame(t, "null twin", got, want)
	if err := json.Unmarshal(badRow, &plain); err != nil {
		t.Fatal(err)
	}
	plannerSame(t, "worker row without guard", plain, want)
	if err := decodeStoredJob(badRow, &plain); !errors.Is(err, errWorkerRoute) {
		t.Fatalf("guard on worker row: %v", err)
	}

	before := plannerStoreSnapshot(t, s)
	object := artifactRegressionObject(t, s, input.ID)

	bad := view(badRow)
	if !bytes.Equal(bad.planned, orig) {
		t.Fatal("plan did not start from the stored job row")
	}
	plannerRefused(t, bad)

	null := view(nullRow)
	if null.err != nil {
		t.Fatalf("null twin: %v", null.err)
	}
	if len(null.prepared.writes) == 0 {
		t.Fatal("null twin prepared no result expiry")
	}
	if got := plannerStoreSnapshot(t, s); !maps.Equal(got, before) {
		t.Fatal("forecasts changed the Store")
	}
	if got := artifactRegressionObject(t, s, input.ID); got != object {
		t.Fatal("forecasts changed the pinned input")
	}
	plannerBystanderSame(t, s, other, otherBefore)

	// The real plan, without the model row, is committed exactly as Prune does.
	if err := s.db.Update(func(tx *bolt.Tx) error {
		f, err := forecast(tx, nil)
		if err != nil {
			return err
		}
		if f.err != nil {
			return f.err
		}
		if !maps.Equal(plannerWrites(f.prepared), plannerWrites(null.prepared)) {
			return errors.New("null twin and real plan prepared different writes")
		}
		if !bytes.Equal(f.plan.coreGet(jobsBucket, []byte(job.ID)), orig) {
			return errors.New("committed plan holds a different job row")
		}
		if err := f.plan.commit(); err != nil {
			return err
		}
		return f.prepared.apply(tx)
	}); err != nil {
		t.Fatal(err)
	}
	if got := plannerStoreSnapshot(t, s); maps.Equal(got, before) {
		t.Fatal("committed plan changed nothing")
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		if _, ok := st.Results[attempt]; ok {
			return errors.New("result relation survived expiry")
		}
		if e, ok := st.Expired[attempt]; !ok || e.ObjectID != ref.ID || e.Collected {
			return fmt.Errorf("expiry witness %+v", e)
		}
		if _, ok := st.Grants[ref.ID]; ok {
			return errors.New("expired result kept its grant")
		}
		if st.JobInputs[job.ID] != pin {
			return errors.New("expiry changed the job input pin")
		}
		if !bytes.Equal(tx.Bucket(jobsBucket).Get([]byte(job.ID)), orig) {
			return errors.New("expiry changed the job row")
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
	if got := artifactRegressionObject(t, s, input.ID); got != object {
		t.Fatal("expiry changed the pinned input")
	}
	result, err := s.ArtifactResultFor(testActor(), job.ID, attempt)
	if err != nil || result.State != "expired" || result.Reference != nil {
		t.Fatalf("expired result: %+v %v", result, err)
	}
	plannerBystanderSame(t, s, other, otherBefore)
}

// A taskless manual header read through the plan (artifactValidateCorePlanned's
// headerRow via plan.get -> decodeCleanupHeader) maps a workers refusal to
// artifactCorrupt in the real MetadataOnly planner item, after the job row has
// been planned away and its pin transferred to the header.
func TestArtifactPlannedManualHeaderWorkersRefused(t *testing.T) {
	s, job, input := artifactRegressionWorkspace(t, true, false)
	artifactRegressionComplete(t, s, job, false)
	other, otherBefore := plannerSubmitBystander(t, s)
	now := s.now()
	sel := PruneSelection{Before: now.Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true}
	key := retentionKey{true, string(cleanupHeadersBucket), job.ID}

	// forecast follows Store.Prune's MetadataOnly item. row, if non-nil, is the
	// MODELED prospective header placed in the plan after the real transfer
	// read the planned header; retentionMutation has no raw retention put.
	forecast := func(tx *bolt.Tx, row []byte) (out plannerForecast, err error) {
		plan, err := newRetentionMutation(tx)
		if err != nil {
			return out, err
		}
		initial, err := artifactLoad(tx)
		if err != nil {
			return out, err
		}
		trial := initial.clone()
		x, err := artifactPruneSubject(tx, job.ID, sel.Mode)
		if err != nil {
			return out, err
		}
		reason, refs := classifyQualified(tx, x, sel, map[string]bool{}, nil, artifactWholeReady(trial, x.Job))
		if reason != "" {
			return out, fmt.Errorf("classify skipped: %s", reason)
		}
		h, ops, err := makeCleanup(tx, x, sel, refs, nil, now, false)
		if err != nil {
			return out, err
		}
		out.plan = plan.fork()
		if err := markPrune(out.plan, x, sel, h, ops, refs); err != nil {
			return out, err
		}
		if err := out.plan.validate(); err != nil {
			return out, err
		}
		n, err := out.plan.changedBytes()
		if err != nil {
			return out, err
		}
		if err := artifactTransferInputPlanned(trial, out.plan, job.ID); err != nil {
			return out, err
		}
		if out.plan.coreGet(jobsBucket, []byte(job.ID)) != nil {
			return out, errors.New("job row is still planned")
		}
		out.planned = bytes.Clone(out.plan.get(key))
		if row != nil {
			out.plan.changes[key] = bytes.Clone(row)
		}
		out.prepared, out.err = prepareArtifactChange(tx, initial, trial, out.plan, maxChangeBytes-n)
		return out, nil
	}
	view := func(row []byte) plannerForecast {
		t.Helper()
		var f plannerForecast
		if err := s.db.View(func(tx *bolt.Tx) error {
			var err error
			f, err = forecast(tx, row)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return f
	}

	var pin artifactPin
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		var ok bool
		if pin, ok = st.JobInputs[job.ID]; !ok || pin.Ref != input {
			return errors.New("fixture job has no input pin")
		}
		if retentionTable(tx, cleanupHeadersBucket).Get([]byte(job.ID)) != nil {
			return errors.New("fixture already has a header")
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
	before := plannerStoreSnapshot(t, s)
	object := artifactRegressionObject(t, s, input.ID)

	actual := view(nil)
	if actual.err != nil {
		t.Fatalf("real plan: %v", actual.err)
	}
	if len(actual.prepared.writes) == 0 {
		t.Fatal("real plan prepared no transfer")
	}
	var want, got, plain cleanupHeader
	if err := decodeCleanupHeader(actual.planned, &want); err != nil {
		t.Fatal(err)
	}
	if want.ID != job.ID || want.JobID != job.ID || !want.NeedsManual || want.Task != nil || want.TaskDigest != pin.TaskDigest {
		t.Fatalf("planned header %+v", want)
	}
	nullRow := plannerModelHeader(t, actual.planned, "null")
	badRow := plannerModelHeader(t, actual.planned, `{"count":2,"mode":"tcp"}`)
	if err := decodeCleanupHeader(nullRow, &got); err != nil {
		t.Fatalf("null twin refused: %v", err)
	}
	plannerSame(t, "null twin", got, want)
	if err := json.Unmarshal(badRow, &plain); err != nil {
		t.Fatal(err)
	}
	if plain.Task != nil {
		t.Fatal("worker header decoded a task")
	}
	plannerSame(t, "worker header without guard", plain, want)
	if err := decodeCleanupHeader(badRow, &plain); !errors.Is(err, errWorkerRoute) {
		t.Fatalf("guard on worker header: %v", err)
	}

	bad := view(badRow)
	if !bytes.Equal(bad.planned, actual.planned) {
		t.Fatal("planned header changed between forecasts")
	}
	plannerRefused(t, bad)

	null := view(nullRow)
	if null.err != nil {
		t.Fatalf("null twin: %v", null.err)
	}
	if !maps.Equal(plannerWrites(null.prepared), plannerWrites(actual.prepared)) {
		t.Fatal("null twin and real plan prepared different writes")
	}
	if got := plannerStoreSnapshot(t, s); !maps.Equal(got, before) {
		t.Fatal("forecasts changed the Store")
	}
	if got := artifactRegressionObject(t, s, input.ID); got != object {
		t.Fatal("forecasts changed the pinned input")
	}
	plannerBystanderSame(t, s, other, otherBefore)

	// The real plan, without the model row, is committed exactly as Prune does.
	if err := s.db.Update(func(tx *bolt.Tx) error {
		f, err := forecast(tx, nil)
		if err != nil {
			return err
		}
		if f.err != nil {
			return f.err
		}
		if !maps.Equal(plannerWrites(f.prepared), plannerWrites(actual.prepared)) {
			return errors.New("committed plan differs from its forecast")
		}
		if !bytes.Equal(f.plan.get(key), actual.planned) {
			return errors.New("committed plan holds a different header")
		}
		if err := f.plan.commit(); err != nil {
			return err
		}
		return f.prepared.apply(tx)
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		if _, ok := st.JobInputs[job.ID]; ok {
			return errors.New("job input pin survived transfer")
		}
		if st.ManualInputs[job.ID] != pin {
			return errors.New("manual input differs from the job pin")
		}
		if tx.Bucket(jobsBucket).Get([]byte(job.ID)) != nil {
			return errors.New("job row survived MetadataOnly")
		}
		var h cleanupHeader
		if err := loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(job.ID), &h); err != nil {
			return err
		}
		if !h.NeedsManual || h.Task != nil || h.TaskDigest != pin.TaskDigest {
			return fmt.Errorf("committed header %+v", h)
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
	if got := artifactRegressionObject(t, s, input.ID); got != object {
		t.Fatal("transfer changed the retained input")
	}
	plannerBystanderSame(t, s, other, otherBefore)
}

// A remote job's row read through the plan (remoteValidateCore's planned
// branch: plan.coreGet(jobsBucket) -> decodeStoredJob) maps a workers refusal
// to artifactCorrupt. The remote jobs are mount-only, so no JobInputs pin,
// result, completion or workspace names them and artifactValidateCorePlanned
// never decodes their rows; remoteValidateCore is the first and only reader.
//
// The artifact change is the real private object-delete mark
// (artifactMarkObjectDelete) of an unpinned uploaded object, forecast by the
// real prepareArtifactChange with a concrete retentionMutation. No production
// caller pairs that mark with a retention plan; Prune pairs the plan with
// result expiry, which a mount-only remote job cannot reach. The pairing is
// the composition under test, not a claimed production route.
func TestArtifactPlannedRemoteJobWorkersRefused(t *testing.T) {
	fx := newRemoteTransactionFixture(t)
	info := fx.enableRetention(t)
	object := artifactTestComplete(t, fx.s, fx.actor)
	task := backend.Task{Argv: []string{"true"}, Image: "modeled-image", DataMounts: []backend.DataMount{{Reference: "dataset", Destination: "/mnt/skygo/dataset", ReadOnly: true}}}
	submit := func(id string) backend.Job {
		t.Helper()
		fx.authenticate(t)
		j, err := fx.s.SubmitData(fx.actor, &info.Domain, id, task, "remote", "modeled-profile-v1", fx.authority, fx.policy, fx.now)
		if err != nil {
			t.Fatal(err)
		}
		return j
	}
	job := submit("planned-remote")
	other := submit("planned-bystander")
	key := RemoteKey{job.ID, job.CurrentAttemptID()}
	otherKey := RemoteKey{other.ID, other.CurrentAttemptID()}
	accepted := remoteTransactionGet(t, fx.s, key)
	otherAccepted := remoteTransactionGet(t, fx.s, otherKey)
	for _, r := range []RemoteAttempt{accepted, otherAccepted} {
		if r.Authority != fx.authority {
			t.Fatalf("remote authority %+v, want %+v", r.Authority, fx.authority)
		}
	}
	if accepted.Key != key || otherAccepted.Key != otherKey {
		t.Fatalf("remote keys %+v %+v", accepted.Key, otherAccepted.Key)
	}
	otherBefore := plannerBystander(t, fx.s, other.ID)
	operation := strings.Repeat("e", 32)

	// at is the earliest time the real mark accepts: the object's deletion
	// floor or its grant expiry, whichever is later.
	var at time.Time
	var orig []byte
	if err := fx.s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		if len(st.JobInputs) != 0 || len(st.ManualInputs) != 0 || len(st.Results) != 0 || len(st.Completions) != 0 || len(st.Workspaces) != 0 {
			return errors.New("fixture has job-bound artifact state")
		}
		for _, k := range []RemoteKey{key, otherKey} {
			if r, ok := st.Remote[k.AttemptID]; !ok || r.Job != k.JobID || r.Authority == nil || r.Authority.Digest != fx.authority.Digest {
				return fmt.Errorf("fixture remote record %s", k.AttemptID)
			}
		}
		o, ok := st.Objects[object.ID]
		if !ok || o.State != "complete" || st.pinned(object.ID) {
			return errors.New("fixture object is not a complete unpinned upload")
		}
		at = o.DeleteAfter
		if g, ok := st.Grants[object.ID]; ok && g.Expires.After(at) {
			at = g.Expires
		}
		orig = bytes.Clone(tx.Bucket(jobsBucket).Get([]byte(job.ID)))
		if err := remoteValidateCore(tx, st, nil); err != nil {
			return err
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}

	nullRow := plannerModelJob(t, orig, "null")
	badRow := plannerModelJob(t, orig, `{"count":2,"mode":"tcp"}`)
	var want, got, plain backend.Job
	if err := decodeStoredJob(orig, &want); err != nil {
		t.Fatal(err)
	}
	if err := decodeStoredJob(nullRow, &got); err != nil {
		t.Fatalf("null twin refused: %v", err)
	}
	plannerSame(t, "null twin", got, want)
	if err := json.Unmarshal(badRow, &plain); err != nil {
		t.Fatal(err)
	}
	plannerSame(t, "worker row without guard", plain, want)
	if err := decodeStoredJob(badRow, &plain); !errors.Is(err, errWorkerRoute) {
		t.Fatalf("guard on worker row: %v", err)
	}

	// forecast marks the object for deletion in a trial state and prepares it
	// against a concrete plan. row, if non-nil, is the MODELED prospective
	// remote job row placed with corePut. pins and remote are the two planned
	// validators run directly on the same trial and plan.
	type remoteForecast struct {
		plannerForecast
		pins, remote error
	}
	forecast := func(tx *bolt.Tx, row []byte) (out remoteForecast, err error) {
		if out.plan, err = newRetentionMutation(tx); err != nil {
			return out, err
		}
		initial, err := artifactLoad(tx)
		if err != nil {
			return out, err
		}
		trial := initial.clone()
		if err := artifactMarkObjectDelete(trial, object.ID, operation, at); err != nil {
			return out, err
		}
		out.planned = bytes.Clone(out.plan.coreGet(jobsBucket, []byte(job.ID)))
		if row != nil {
			out.plan.corePut(jobsBucket, []byte(job.ID), row)
		}
		out.pins = artifactValidateCorePlanned(tx, trial, out.plan)
		out.remote = remoteValidateCore(tx, trial, out.plan)
		n, err := out.plan.changedBytes()
		if err != nil {
			return out, err
		}
		out.prepared, out.err = prepareArtifactChange(tx, initial, trial, out.plan, maxChangeBytes-n)
		return out, nil
	}
	view := func(row []byte) remoteForecast {
		t.Helper()
		var f remoteForecast
		if err := fx.s.db.View(func(tx *bolt.Tx) error {
			var err error
			f, err = forecast(tx, row)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return f
	}

	before := plannerStoreSnapshot(t, fx.s)

	bad := view(badRow)
	if !bytes.Equal(bad.planned, orig) {
		t.Fatal("plan did not start from the stored job row")
	}
	if bad.pins != nil {
		t.Fatalf("pin validation read the remote row: %v", bad.pins)
	}
	if bad.remote == nil || bad.remote.Error() != plannerCorruptError || errors.Is(bad.remote, errWorkerRoute) {
		t.Fatalf("remoteValidateCore = %v, want exactly %q", bad.remote, plannerCorruptError)
	}
	plannerRefused(t, bad.plannerForecast)

	null := view(nullRow)
	if null.pins != nil || null.remote != nil || null.err != nil {
		t.Fatalf("null twin: pins=%v remote=%v prepare=%v", null.pins, null.remote, null.err)
	}
	if len(null.prepared.writes) == 0 {
		t.Fatal("null twin prepared no delete mark")
	}
	if got := plannerStoreSnapshot(t, fx.s); !maps.Equal(got, before) {
		t.Fatal("forecasts changed the Store")
	}
	plannerBystanderSame(t, fx.s, other, otherBefore)
	remoteTransactionUnchanged(t, fx.s, key, accepted)
	remoteTransactionUnchanged(t, fx.s, otherKey, otherAccepted)

	// The real plan, without the model row, is committed with the mark.
	if err := fx.s.db.Update(func(tx *bolt.Tx) error {
		f, err := forecast(tx, nil)
		if err != nil {
			return err
		}
		if f.pins != nil || f.remote != nil || f.err != nil {
			return errors.Join(f.pins, f.remote, f.err)
		}
		if !maps.Equal(plannerWrites(f.prepared), plannerWrites(null.prepared)) {
			return errors.New("null twin and real plan prepared different writes")
		}
		if !bytes.Equal(f.plan.coreGet(jobsBucket, []byte(job.ID)), orig) {
			return errors.New("committed plan holds a different job row")
		}
		if err := f.plan.commit(); err != nil {
			return err
		}
		return f.prepared.apply(tx)
	}); err != nil {
		t.Fatal(err)
	}
	if got := plannerStoreSnapshot(t, fx.s); maps.Equal(got, before) {
		t.Fatal("committed plan changed nothing")
	}
	if err := fx.s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		o := st.Objects[object.ID]
		if o.State != "deleting" {
			return fmt.Errorf("object state %q", o.State)
		}
		if d := st.Deletes[operation]; d != (artifactDelete{Owner: o.Owner, Kind: "object", ObjectID: object.ID, File: o.File, Revision: 1}) {
			return fmt.Errorf("delete intent %+v", d)
		}
		if _, ok := st.Grants[object.ID]; ok {
			return errors.New("marked object kept its grant")
		}
		if !bytes.Equal(tx.Bucket(jobsBucket).Get([]byte(job.ID)), orig) {
			return errors.New("mark changed the remote job row")
		}
		if err := remoteValidateCore(tx, st, nil); err != nil {
			return err
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
	remoteTransactionUnchanged(t, fx.s, key, accepted)
	remoteTransactionUnchanged(t, fx.s, otherKey, otherAccepted)
	plannerBystanderSame(t, fx.s, other, otherBefore)
}
