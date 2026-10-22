package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
	"io"
	"reflect"
	"slices"
	"sort"
	"time"
)

type remoteRecord struct {
	Job       string           `json:"j"`
	Revision  uint64           `json:"r"`
	State     uint8            `json:"s"`
	Creation  uint8            `json:"c,omitempty"` // immutable original return and activation provenance
	Authority *remoteAuthority `json:"a,omitempty"`
	Resources remoteResources  `json:"k"`
	Active    *remoteActive    `json:"v,omitempty"`
	Done      *remoteDecision  `json:"d,omitempty"`
	Ack       uint8            `json:"x,omitempty"`
}
type remoteAuthority struct {
	Principal string   `json:"p"`
	TokenID   string   `json:"t"`
	Digest    [32]byte `json:"h"`
	Policy    [5]int64 `json:"q"`
}
type remoteResources struct {
	Pod    *remoteResource `json:"p,omitempty"`
	Input  *remoteResource `json:"i,omitempty"`
	Output *remoteResource `json:"o,omitempty"`
}
type remoteResource struct {
	Name string `json:"n"`
	UID  string `json:"u"`
}
type remoteActive struct {
	Phase            uint8               `json:"p"`
	Budgets          [4]int64            `json:"b"`
	Began            time.Time           `json:"t"`
	StageDeadline    time.Time           `json:"s"`
	ResourceDeadline time.Time           `json:"r"`
	TaskDeadline     *time.Time          `json:"k,omitempty"`
	ReleaseAt        *time.Time          `json:"c,omitempty"`
	Input            *remoteLease        `json:"i,omitempty"`
	Output           *remoteLease        `json:"o,omitempty"`
	Running          *remoteStartReceipt `json:"u,omitempty"`
	Stop             *remoteTaskStop     `json:"e,omitempty"`
	NoLaunch         *remoteNoLaunch     `json:"n,omitempty"`
	Collector        *remoteExit         `json:"f,omitempty"`
	ReleaseRevision  uint64              `json:"v,omitempty"`
	ClosedReason     uint8               `json:"z,omitempty"`
	ResultID         string              `json:"d,omitempty"`
}
type remoteLease struct {
	Hash    [32]byte  `json:"h"`
	Epoch   uint64    `json:"e"`
	State   uint8     `json:"s"`
	Expires time.Time `json:"x"`
	Renewed time.Time `json:"r"`
}
type remoteStartReceipt struct {
	ID      string    `json:"i"`
	Started time.Time `json:"s"`
}
type remoteExit struct {
	ID       string    `json:"i"`
	Code     int32     `json:"x"`
	Started  time.Time `json:"s"`
	Finished time.Time `json:"f"`
}
type remoteTaskStop struct {
	Stage *remoteExit `json:"s,omitempty"`
	Setup *remoteExit `json:"u,omitempty"`
	Task  remoteExit  `json:"t"`
}
type remoteBlocked struct {
	Name   string `json:"n"`
	Reason string `json:"r"`
}
type remoteNoLaunch struct {
	Failed           uint8          `json:"f"`
	Stage            *remoteExit    `json:"s"`
	Setup            *remoteExit    `json:"u,omitempty"`
	SetupBlocked     *remoteBlocked `json:"b,omitempty"`
	TaskBlocked      remoteBlocked  `json:"t"`
	CollectorBlocked *remoteBlocked `json:"c,omitempty"`
}
type remoteDecision struct {
	Digest       [32]byte        `json:"h"`
	Outcome      uint8           `json:"o"`
	OriginalExit *int32          `json:"x,omitempty"`
	Stop         *remoteTaskStop `json:"s,omitempty"`
	NoLaunch     *remoteNoLaunch `json:"n,omitempty"`
	Collector    *remoteExit     `json:"c,omitempty"`
	ResultID     string          `json:"r,omitempty"`
	// Input is the original Active.Input lease, retained only when its complete
	// transfer was consumed (State 4). It grants no admission.
	Input *remoteLease `json:"i,omitempty"`
}

func remoteCheckTime(t time.Time) bool {
	return !t.IsZero() && t.Location() == time.UTC && t.Year() >= 1 && t.Year() <= 9999
}
func remoteEncode(r remoteRecord) ([]byte, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(r); err != nil {
		return nil, err
	}
	raw := bytes.TrimSuffix(b.Bytes(), []byte("\n"))
	if len(raw) > 4096 {
		return nil, artifactCorrupt()
	}
	return raw, nil
}
func remoteDecode(raw []byte) (remoteRecord, error) {
	var r remoteRecord
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if len(raw) > 4096 || d.Decode(&r) != nil || d.Decode(new(any)) != io.EOF {
		return r, artifactCorrupt()
	}
	canonical, err := remoteEncode(r)
	if err != nil || !bytes.Equal(raw, canonical) {
		return r, artifactCorrupt()
	}
	return r, nil
}
func remoteCopy[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
func remoteCloneStop(s *remoteTaskStop) *remoteTaskStop {
	if s == nil {
		return nil
	}
	v := *s
	v.Stage = remoteCopy(s.Stage)
	v.Setup = remoteCopy(s.Setup)
	return &v
}
func remoteCloneNoLaunch(n *remoteNoLaunch) *remoteNoLaunch {
	if n == nil {
		return nil
	}
	v := *n
	v.Stage = remoteCopy(n.Stage)
	v.Setup = remoteCopy(n.Setup)
	v.SetupBlocked = remoteCopy(n.SetupBlocked)
	v.CollectorBlocked = remoteCopy(n.CollectorBlocked)
	return &v
}
func remoteClone(r remoteRecord) remoteRecord {
	r.Authority = remoteCopy(r.Authority)
	r.Resources.Pod = remoteCopy(r.Resources.Pod)
	r.Resources.Input = remoteCopy(r.Resources.Input)
	r.Resources.Output = remoteCopy(r.Resources.Output)
	if r.Active != nil {
		a := *r.Active
		a.TaskDeadline = remoteCopy(a.TaskDeadline)
		a.ReleaseAt = remoteCopy(a.ReleaseAt)
		a.Input = remoteCopy(a.Input)
		a.Output = remoteCopy(a.Output)
		a.Running = remoteCopy(a.Running)
		a.Stop = remoteCloneStop(a.Stop)
		a.NoLaunch = remoteCloneNoLaunch(a.NoLaunch)
		a.Collector = remoteCopy(a.Collector)
		r.Active = &a
	}
	if r.Done != nil {
		d := *r.Done
		d.OriginalExit = remoteCopy(d.OriginalExit)
		d.Stop = remoteCloneStop(d.Stop)
		d.NoLaunch = remoteCloneNoLaunch(d.NoLaunch)
		d.Collector = remoteCopy(d.Collector)
		d.Input = remoteCopy(d.Input)
		r.Done = &d
	}
	return r
}
func remotePolicy(a *remoteAuthority) backend.DataPolicy {
	return backend.DataPolicy{StageSeconds: a.Policy[0], CollectionSeconds: a.Policy[1], DefaultTaskSeconds: a.Policy[2], MaxTaskSeconds: a.Policy[3], ShutdownSeconds: a.Policy[4]}
}
func remoteBudgets(a *remoteActive) backend.DataBudgets {
	return backend.DataBudgets{StageSeconds: a.Budgets[0], TaskSeconds: a.Budgets[1], CollectionSeconds: a.Budgets[2], ShutdownSeconds: a.Budgets[3]}
}
func remoteResourceDTO(r *remoteResource) backend.DataResource {
	if r == nil {
		return backend.DataResource{}
	}
	return backend.DataResource{Name: r.Name, UID: r.UID}
}
func remoteCreated(r remoteRecord) backend.DataCreated {
	p := remoteResourceDTO(r.Resources.Pod)
	return backend.DataCreated{Instance: p.UID, Pod: p, InputSecret: remoteResourceDTO(r.Resources.Input), OutputSecret: remoteResourceDTO(r.Resources.Output)}
}
func remoteResourceRecord(r backend.DataResource) *remoteResource {
	if r == (backend.DataResource{}) {
		return nil
	}
	return &remoteResource{r.Name, r.UID}
}
func remoteCreatedRecord(r backend.DataCreated) remoteResources {
	return remoteResources{remoteResourceRecord(r.Pod), remoteResourceRecord(r.InputSecret), remoteResourceRecord(r.OutputSecret)}
}
func remoteExitDTO(e *remoteExit) *backend.ContainerExit {
	if e == nil {
		return nil
	}
	return &backend.ContainerExit{ContainerID: e.ID, ExitCode: e.Code, StartedAt: e.Started, FinishedAt: e.Finished}
}
func remoteExitRecord(e *backend.ContainerExit) *remoteExit {
	if e == nil {
		return nil
	}
	return &remoteExit{e.ContainerID, e.ExitCode, e.StartedAt.UTC(), e.FinishedAt.UTC()}
}
func remoteStopDTO(s *remoteTaskStop) *backend.DataTaskStop {
	if s == nil {
		return nil
	}
	return &backend.DataTaskStop{Stage: remoteExitDTO(s.Stage), Setup: remoteExitDTO(s.Setup), Task: *remoteExitDTO(&s.Task)}
}
func remoteStopRecord(s *backend.DataTaskStop) *remoteTaskStop {
	if s == nil {
		return nil
	}
	return &remoteTaskStop{Stage: remoteExitRecord(s.Stage), Setup: remoteExitRecord(s.Setup), Task: *remoteExitRecord(&s.Task)}
}
func remoteBlockedDTO(b *remoteBlocked) *backend.DataNotStarted {
	if b == nil {
		return nil
	}
	return &backend.DataNotStarted{Name: b.Name, WaitingReason: b.Reason}
}
func remoteBlockedRecord(b *backend.DataNotStarted) *remoteBlocked {
	if b == nil {
		return nil
	}
	return &remoteBlocked{b.Name, b.WaitingReason}
}
func remoteNoLaunchDTO(n *remoteNoLaunch) *backend.DataNoLaunch {
	if n == nil {
		return nil
	}
	return &backend.DataNoLaunch{FailedInit: backend.DataInitPhase(n.Failed), Stage: remoteExitDTO(n.Stage), Setup: remoteExitDTO(n.Setup), SetupBlocked: remoteBlockedDTO(n.SetupBlocked), TaskBlocked: remoteBlockedDTO(&n.TaskBlocked), CollectorBlocked: remoteBlockedDTO(n.CollectorBlocked)}
}
func remoteBlockedValue(b *backend.DataNotStarted) remoteBlocked {
	v := remoteBlockedRecord(b)
	if v == nil {
		return remoteBlocked{}
	}
	return *v
}
func remoteNoLaunchRecord(n *backend.DataNoLaunch) *remoteNoLaunch {
	if n == nil {
		return nil
	}
	return &remoteNoLaunch{Failed: uint8(n.FailedInit), Stage: remoteExitRecord(n.Stage), Setup: remoteExitRecord(n.Setup), SetupBlocked: remoteBlockedRecord(n.SetupBlocked), TaskBlocked: remoteBlockedValue(n.TaskBlocked), CollectorBlocked: remoteBlockedRecord(n.CollectorBlocked)}
}
func remoteScopeDTO(l *remoteLease) *RemoteScope {
	if l == nil {
		return nil
	}
	return &RemoteScope{l.Hash, l.Epoch, l.State, l.Expires, l.Renewed}
}
func remoteGet(st *artifactState, key RemoteKey) (remoteRecord, error) {
	r, ok := st.Remote[key.AttemptID]
	if !ok || r.Job != key.JobID {
		return r, ErrNotFound
	}
	return remoteClone(r), nil
}
func remoteOwner(st *artifactState, r remoteRecord) (string, error) {
	first, ok := st.Remote[r.Job]
	if !ok || first.Authority == nil {
		return "", artifactCorrupt()
	}
	return first.Authority.Principal, nil
}
func remoteRecordOwner(st *artifactState, id string, r remoteRecord) (string, error) {
	if !artifactHex(id, 32) || !artifactHex(r.Job, 32) || r.Revision == 0 || r.State < 1 || r.State > 4 || r.Creation > 3 || r.Ack > 7 {
		return "", artifactCorrupt()
	}
	first, ok := st.Remote[r.Job]
	if !ok || first.Job != r.Job || first.Authority == nil || id == r.Job != (r.Authority != nil) {
		return "", artifactCorrupt()
	}
	a := first.Authority
	if !validPrincipal([]byte(a.Principal)) || !artifactHex(a.TokenID, 32) || a.Digest == ([32]byte{}) || remotePolicy(a).Validate() != nil {
		return "", artifactCorrupt()
	}
	if r.State == 1 {
		if r.Active != nil || r.Done != nil || r.Creation != 0 || r.Ack != 0 || r.Resources != (remoteResources{}) {
			return "", artifactCorrupt()
		}
	} else if r.State == 2 {
		if r.Active == nil || r.Done != nil {
			return "", artifactCorrupt()
		}
		x := r.Active
		if r.Creation == 0 && r.Resources != (remoteResources{}) {
			return "", artifactCorrupt()
		}
		if x.Phase < 1 || x.Phase > 6 || x.ClosedReason > 6 || !remoteCheckTime(x.Began) || !remoteCheckTime(x.StageDeadline) || !remoteCheckTime(x.ResourceDeadline) || !x.StageDeadline.After(x.Began) || !x.ResourceDeadline.After(x.StageDeadline) {
			return "", artifactCorrupt()
		}
		if _, e := remoteBudgets(x).TotalSeconds(); e != nil {
			return "", artifactCorrupt()
		}
		for _, l := range []*remoteLease{x.Input, x.Output} {
			if l != nil && ((l.State == 1 && r.Creation != 0) || (l.State == 2 && r.Creation != 3)) {
				return "", artifactCorrupt()
			}
			if l != nil && (l.Hash == ([32]byte{}) || l.Epoch == 0 || l.State < 1 || l.State > 4 || !remoteCheckTime(l.Renewed) || !remoteCheckTime(l.Expires) || !l.Expires.Equal(l.Renewed.Add(120*time.Second))) {
				return "", artifactCorrupt()
			}
		}
		if x.Output != nil && x.Output.State == 4 || x.Input != nil && x.Input.State == 4 && r.Creation != 3 {
			return "", artifactCorrupt()
		}
		if x.Input != nil && x.Output != nil && x.Input.Hash == x.Output.Hash {
			return "", artifactCorrupt()
		}
		if x.Running != nil && (x.Stop != nil || x.NoLaunch != nil) || x.Stop != nil && x.NoLaunch != nil || x.ClosedReason != 0 && ((x.Input != nil && x.Input.State != 3 && x.Input.State != 4) || (x.Output != nil && x.Output.State != 3)) {
			return "", artifactCorrupt()
		}
		if x.TaskDeadline != nil && !remoteCheckTime(*x.TaskDeadline) || x.ReleaseAt != nil && !remoteCheckTime(*x.ReleaseAt) {
			return "", artifactCorrupt()
		}
		if (x.ReleaseRevision != 0) != (x.ReleaseAt != nil) || x.ReleaseRevision > r.Revision {
			return "", artifactCorrupt()
		}
		if x.ReleaseAt != nil {
			if r.Creation != 3 || x.Stop == nil || x.Stop.Task.Code != 0 || x.Output == nil || x.ReleaseAt.Before(x.Began) || x.ReleaseAt.Before(x.Stop.Task.Finished) || !x.ReleaseAt.Before(x.ResourceDeadline) {
				return "", artifactCorrupt()
			}
			if _, err := remoteCollectionDeadline(x); err != nil {
				return "", artifactCorrupt()
			}
		}
		if x.ResultID != "" && !artifactHex(x.ResultID, 32) {
			return "", artifactCorrupt()
		}
		if r.Ack != 0 && (r.Creation != 2 || r.Resources.Pod != nil || r.Ack&1 != 0 || x.Stop != nil || x.NoLaunch != nil || x.Running != nil || x.Collector != nil || x.ClosedReason == 0) {
			return "", artifactCorrupt()
		}
	} else if r.Active != nil || r.Done == nil || r.Done.Digest == ([32]byte{}) || r.Done.Outcome < 1 || r.Done.Outcome >= uint8(DataUncertain) {
		return "", artifactCorrupt()
	} else if l := r.Done.Input; l != nil && (r.Creation != 3 || r.Resources.Input == nil || l.Hash == ([32]byte{}) || l.Epoch == 0 || l.State != 4 || !remoteCheckTime(l.Renewed) || !remoteCheckTime(l.Expires) || !l.Expires.Equal(l.Renewed.Add(120*time.Second))) {
		return "", artifactCorrupt()
	}
	if r.State == 4 && !remoteQueuedDecision(r) && r.Ack != remoteRequiredAck(r) {
		return "", artifactCorrupt()
	}
	if r.State >= 3 {
		for _, read := range st.Reads {
			if read.RemoteAttemptID == id && (!read.Closed || r.State == 4) {
				return "", artifactCorrupt()
			}
		}
	}
	return a.Principal, nil
}
func remoteRequiredAck(r remoteRecord) uint8 {
	n := uint8(1)
	if r.Resources.Input != nil {
		n |= 2
	}
	if r.Resources.Output != nil {
		n |= 4
	}
	return n
}
func remoteOutputID(r remoteRecord) string {
	if r.Active != nil {
		return r.Active.ResultID
	}
	if r.Done != nil {
		return r.Done.ResultID
	}
	return ""
}
func remoteSnapshot(st *artifactState, id string, r remoteRecord) (RemoteAttempt, error) {
	first, ok := st.Remote[r.Job]
	if !ok || first.Authority == nil {
		return RemoteAttempt{}, artifactCorrupt()
	}
	a := first.Authority
	out := RemoteAttempt{Key: RemoteKey{r.Job, id}, Revision: r.Revision, State: r.State, Creation: r.Creation, Authority: auth.Delegation{Principal: a.Principal, TokenID: a.TokenID, Digest: a.Digest}, Policy: remotePolicy(a), Resources: remoteCreated(r), Ack: r.Ack}
	if r.Active != nil {
		x := r.Active
		deadline, e := remoteCollectionDeadline(x)
		if e != nil {
			return out, e
		}
		out.Active = &RemoteActive{Phase: backend.DataPhase(x.Phase), Budgets: remoteBudgets(x), Began: x.Began, StageDeadline: x.StageDeadline, ResourceDeadline: x.ResourceDeadline, TaskDeadline: remoteCopy(x.TaskDeadline), CollectionDeadline: deadline, Input: remoteScopeDTO(x.Input), Output: remoteScopeDTO(x.Output), Stop: remoteStopDTO(x.Stop), NoLaunch: remoteNoLaunchDTO(x.NoLaunch), Collector: remoteExitDTO(x.Collector), ReleaseRevision: x.ReleaseRevision, ClosedReason: RemoteCloseReason(x.ClosedReason), ResultID: x.ResultID}
		if x.Running != nil {
			out.Active.Running = &backend.ContainerStart{ContainerID: x.Running.ID, StartedAt: x.Running.Started}
		}
	}
	if r.Done != nil {
		d := r.Done
		out.Done = &RemoteDecision{Digest: d.Digest, Outcome: DataOutcome(d.Outcome), OriginalExit: remoteCopy(d.OriginalExit), Stop: remoteStopDTO(d.Stop), NoLaunch: remoteNoLaunchDTO(d.NoLaunch), Collector: remoteExitDTO(d.Collector), ResultID: d.ResultID}
	}
	return out, nil
}
func remoteRef(tx *bolt.Tx, key RemoteKey, r remoteRecord) (backend.Reference, error) {
	j, e := get(tx, key.JobID)
	if e != nil {
		return backend.Reference{}, e
	}
	if !slices.Contains(executionIDs(j), key.AttemptID) {
		return backend.Reference{}, ErrArtifactConflict
	}
	return backend.Reference{ID: key.AttemptID, Binding: j.TargetBinding, Instance: remoteCreated(r).Instance, Task: j.Task}, nil
}
func remoteValidateCore(tx *bolt.Tx, st *artifactState, plan *retentionMutation) error {
	for id, r := range st.Remote {
		owner, e := remoteRecordOwner(st, id, r)
		if e != nil {
			return e
		}
		var j backend.Job
		if plan == nil {
			j, e = get(tx, r.Job)
		} else {
			raw := plan.coreGet(jobsBucket, []byte(r.Job))
			if raw == nil {
				return ErrArtifactConflict
			}
			e = json.Unmarshal(raw, &j)
		}
		if e != nil || !slices.Contains(executionIDs(j), id) || !validBinding(j.TargetBinding) {
			return artifactCorrupt()
		}
		principal := "legacy"
		if isScoped(tx) {
			if plan == nil {
				principal = string(tx.Bucket(jobPrincipalsBucket).Get([]byte(r.Job)))
			} else {
				principal = string(plan.coreGet(jobPrincipalsBucket, []byte(r.Job)))
			}
		}
		if owner != principal || backend.ValidateDataTask(j.Task, nil) != nil {
			return artifactCorrupt()
		}
		if e = backend.ValidateDataCreated(j.Task, remoteCreated(r), r.Creation != 3); e != nil {
			return artifactCorrupt()
		}
		if remoteQueuedDecision(r) {
			if !remoteNeverStarted(j, id) || r.Done.Digest != remoteQueuedDigest(j) {
				return artifactCorrupt()
			}
			continue
		}
		if r.Done != nil {
			d := r.Done
			if (d.Outcome == uint8(DataMainExit) || d.Outcome == uint8(DataCollectionFailure)) != (d.OriginalExit != nil) || d.OriginalExit != nil && (*d.OriginalExit < 0 || *d.OriginalExit > 255) || d.Outcome == uint8(DataCollectionFailure) && *d.OriginalExit != 0 {
				return artifactCorrupt()
			}
			if r.State == 3 {
				c := RemoteCompletion{Outcome: DataOutcome(d.Outcome), Stop: remoteStopDTO(d.Stop), NoLaunch: remoteNoLaunchDTO(d.NoLaunch), Collector: remoteExitDTO(d.Collector)}
				_, original, e := remoteCompletionStatus(j, r, c)
				if e != nil || !reflect.DeepEqual(original, d.OriginalExit) {
					return artifactCorrupt()
				}
			}
			if (d.ResultID != "") != (len(j.Task.Outputs) != 0) {
				return artifactCorrupt()
			}
		}
		if r.State == 4 {
			required := uint8(1)
			if j.Task.Workdir != nil {
				required |= 2
			}
			if len(j.Task.Outputs) != 0 {
				required |= 4
			}
			if r.Ack != required {
				return artifactCorrupt()
			}
		}
		if r.Active == nil {
			continue
		}
		a := r.Active
		expected, e := remotePolicy(st.Remote[r.Job].Authority).Budgets(j.Task.TimeoutSeconds)
		if e != nil || expected != remoteBudgets(a) {
			return artifactCorrupt()
		}
		total, _ := expected.TotalSeconds()
		stage, e := backend.DataDeadline(a.Began, expected.StageSeconds)
		if e != nil || !stage.Equal(a.StageDeadline) {
			return artifactCorrupt()
		}
		end, e := backend.DataDeadline(a.Began, total)
		if e != nil || !end.Equal(a.ResourceDeadline) {
			return artifactCorrupt()
		}
		if (a.Input != nil) != (j.Task.Workdir != nil) || (a.Output != nil) != (len(j.Task.Outputs) != 0) {
			return artifactCorrupt()
		}
		if (a.ResultID != "") != (len(j.Task.Outputs) != 0) {
			return artifactCorrupt()
		}
		var started *time.Time
		if a.Running != nil {
			started = &a.Running.Started
		}
		if a.Stop != nil {
			started = &a.Stop.Task.Started
		}
		if (a.TaskDeadline != nil) != (started != nil) {
			return artifactCorrupt()
		}
		if started != nil {
			deadline, e := backend.DataDeadline(*started, expected.TaskSeconds)
			if e != nil || !deadline.Equal(*a.TaskDeadline) || started.Before(a.Began) || !started.Before(a.StageDeadline) {
				return artifactCorrupt()
			}
		}
		if (a.ReleaseRevision != 0) != (a.ReleaseAt != nil) || a.ReleaseRevision > r.Revision {
			return artifactCorrupt()
		}
		if a.ReleaseAt != nil {
			if r.Creation != 3 || a.Stop == nil || a.Stop.Task.Code != 0 || a.Output == nil || a.ReleaseAt.Before(a.Began) || a.ReleaseAt.Before(a.Stop.Task.Finished) || !a.ReleaseAt.Before(a.ResourceDeadline) {
				return artifactCorrupt()
			}
			if _, e := remoteCollectionDeadline(a); e != nil {
				return artifactCorrupt()
			}
		}
		if a.Running != nil || a.Stop != nil || a.NoLaunch != nil || a.Collector != nil {
			ref := backend.Reference{ID: id, Binding: j.TargetBinding, Instance: remoteCreated(r).Instance, Task: j.Task}
			obs := backend.DataObservation{Status: backend.Status{State: backend.Starting, Instance: ref.Instance}, Phase: backend.DataPhase(a.Phase), TaskStop: remoteStopDTO(a.Stop), NoLaunch: remoteNoLaunchDTO(a.NoLaunch), Collector: remoteExitDTO(a.Collector)}
			if a.Running != nil {
				obs.Status.State = backend.Running
				obs.TaskStart = &backend.ContainerStart{ContainerID: a.Running.ID, StartedAt: a.Running.Started}
			}
			if a.Stop != nil {
				code := a.Stop.Task.Code
				obs.Status = backend.Status{State: backend.Failed, Instance: ref.Instance, ExitCode: &code}
				if code == 0 {
					obs.Status.State = backend.Succeeded
				}
			}
			if a.NoLaunch != nil {
				obs.Status = backend.Status{State: backend.Failed, Instance: ref.Instance}
			}
			if backend.ValidateDataObservation(ref, obs) != nil || !remoteObservationTimes(a, obs, nil) {
				return artifactCorrupt()
			}
		}
	}
	return nil
}
func remoteBump(r *remoteRecord) error {
	n, e := artifactNext(r.Revision)
	if e == nil {
		r.Revision = n
	}
	return e
}
func remoteClose(r *remoteRecord, reason RemoteCloseReason) {
	if r.Active == nil {
		return
	}
	r.Active.ClosedReason = uint8(reason)
	r.Active.Phase = uint8(backend.DataHeld)
	for _, l := range []*remoteLease{r.Active.Input, r.Active.Output} {
		if l != nil && l.State != 4 {
			l.State = 3
		}
	}
}
func remoteGrantRecord(st *artifactState, g RemoteGrant, now time.Time) (remoteRecord, *remoteLease, error) {
	r, e := remoteGet(st, g.Key)
	if e != nil {
		return r, nil, ErrNotFound
	}
	if r.Creation != 3 || r.State != 2 || r.Active == nil || r.Active.ClosedReason != 0 || r.Active.Phase == uint8(backend.DataHeld) || !remoteCheckTime(now) || !now.Before(r.Active.ResourceDeadline) {
		return r, nil, ErrArtifactExpired
	}
	var l *remoteLease
	switch g.Kind {
	case RemoteInput:
		l = r.Active.Input
	case RemoteOutput:
		l = r.Active.Output
	default:
		return r, nil, ErrNotFound
	}
	if l == nil || l.State != 2 || l.Epoch != g.Epoch || !l.Expires.Equal(g.Expires) || !now.Before(l.Expires) {
		return r, nil, ErrArtifactExpired
	}
	if r.Active.TaskDeadline != nil && r.Active.Stop == nil && !now.Before(*r.Active.TaskDeadline) {
		return r, nil, ErrArtifactExpired
	}
	if r.Active.Stop == nil && r.Active.Running == nil && !now.Before(r.Active.StageDeadline) {
		return r, nil, ErrArtifactExpired
	}
	if g.Kind == RemoteOutput {
		deadline, e := remoteCollectionDeadline(r.Active)
		if e != nil || r.Active.Stop == nil || r.Active.Stop.Task.Code != 0 || r.Active.ReleaseRevision == 0 || deadline == nil || !now.Before(*deadline) {
			return r, nil, ErrArtifactConflict
		}
	}
	return r, l, nil
}

// GetRemoteAttempt returns one trusted detached route/ownership snapshot.
func (s *Store) GetRemoteAttempt(key RemoteKey) (out RemoteAttempt, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		r, e := remoteGet(st, key)
		if e != nil {
			return e
		}
		out, e = remoteSnapshot(st, key.AttemptID, r)
		return e
	})
	return
}

// NextRemoteAttempt selects one bounded row in key order, wrapping once.
// Selection conveys no native work authority or original-owner receipt.
func (s *Store) NextRemoteAttempt(after string) (out RemoteAttempt, found bool, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		ids := make([]string, 0, len(st.Remote))
		for id := range st.Remote {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		if len(ids) == 0 {
			return nil
		}
		i := sort.SearchStrings(ids, after)
		for i < len(ids) && ids[i] <= after {
			i++
		}
		if i == len(ids) {
			i = 0
		}
		out, e = remoteSnapshot(st, ids[i], st.Remote[ids[i]])
		found = e == nil
		return e
	})
	return
}

func remoteQueuedBefore(j backend.Job) bool {
	if j.Status.State != backend.Queued || !j.CancelRequested || j.Status.Instance != "" || j.Status.ExitCode != nil || j.Status.Termination != backend.UnknownTermination || j.NextAttemptAt != nil || j.CurrentAttemptID() != j.ID || len(j.Attempts) > 1 {
		return false
	}
	if len(j.Attempts) == 1 {
		a := j.Attempts[0]
		if a.ID != j.ID || a.StartingAt != nil || a.FinishedAt != nil || a.Status.State != backend.Queued || a.Status.Instance != "" || a.Status.ExitCode != nil || a.Status.Termination != backend.UnknownTermination {
			return false
		}
	}
	return true
}
func remoteNeverStarted(j backend.Job, id string) bool {
	if id != j.ID || j.CurrentAttemptID() != j.ID || j.Status.State != backend.Canceled || j.Status.Reason != "canceled before execution" || !j.CancelRequested || j.Status.Instance != "" || j.Status.ExitCode != nil || j.Status.Termination != backend.UnknownTermination || j.NextAttemptAt != nil || len(j.Attempts) > 1 {
		return false
	}
	if len(j.Attempts) == 1 {
		a := j.Attempts[0]
		if a.ID != id || a.StartingAt != nil || a.FinishedAt == nil || a.Status.State != backend.Canceled || a.Status.Reason != "canceled before execution" || a.Status.Instance != "" || a.Status.ExitCode != nil || a.Status.Termination != backend.UnknownTermination {
			return false
		}
	}
	return true
}
func remoteQueuedDecision(r remoteRecord) bool {
	return r.State == 4 && r.Creation == 0 && r.Active == nil && r.Resources == (remoteResources{}) && r.Ack == 0 && r.Authority != nil && r.Done != nil && r.Done.Outcome == uint8(DataCanceled) && r.Done.OriginalExit == nil && r.Done.Stop == nil && r.Done.NoLaunch == nil && r.Done.Collector == nil && r.Done.ResultID == ""
}
func remoteQueuedDigest(j backend.Job) [32]byte {
	task, _ := backend.TaskBytes(j.Task) // caller has already validated this canonical accepted task.
	raw, _ := json.Marshal(struct {
		Purpose, Job, Binding string
		Task                  json.RawMessage
	}{"skygo.remote-queued-cancel.v1", j.ID, j.TargetBinding, task})
	return sha256.Sum256(raw)
}

// remoteDeleteDeferred is the same pre-dispatch and post-return fence. A late
// original File receipt may be marked for custody while its attempt still owns
// the completion frontier; neither selection nor a supplied receipt bypasses it.
func remoteDeleteDeferred(st *artifactState, d artifactDelete) bool {
	if u, ok := st.Uploads[d.ObjectID]; ok && u.RemoteAttemptID != "" {
		r, ok := st.Remote[u.RemoteAttemptID]
		return ok && r.State == 2
	}
	if o, ok := st.Objects[d.ObjectID]; ok && o.Origin == "remote_result" {
		r, ok := st.Remote[o.Source]
		return ok && r.State == 2
	}
	return false
}
func remoteLinkedArtifacts(st *artifactState, job string) bool {
	attempts := make(map[string]bool)
	outputs := make(map[string]bool)
	for id, r := range st.Remote {
		if r.Job == job {
			attempts[id] = true
			if id := remoteOutputID(r); id != "" {
				outputs[id] = true
			}
		}
	}
	for _, u := range st.Uploads {
		if attempts[u.RemoteAttemptID] {
			return true
		}
	}
	for _, r := range st.Reads {
		if attempts[r.RemoteAttemptID] {
			return true
		}
	}
	for _, o := range st.Objects {
		if o.Origin == "remote_result" && attempts[o.Source] {
			return true
		}
	}
	for _, d := range st.Deletes {
		if outputs[d.ObjectID] {
			return true
		}
	}
	return false
}

// remoteDetachClosedReads removes only a completed scope's metadata linkage.
// Every original Close receipt and its ordinary charge stays unchanged. The
// existing original positive RemoveData result is retained across each quantum.
func remoteDetachClosedReads(st *artifactState, attempt string) (int, bool, error) {
	ids := make([]string, 0)
	for id, r := range st.Reads {
		if r.RemoteAttemptID == attempt {
			if !r.Closed {
				return 0, false, ErrArtifactConflict
			}
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	n := len(ids)
	if n > 64 {
		n = 64
	}
	for _, id := range ids[:n] {
		r := st.Reads[id]
		r.RemoteAttemptID = ""
		st.Reads[id] = r
	}
	return n, len(ids) > n, nil
}

// remoteCollectionDeadline derives the fixed budget from the original Store
// release transaction, never from a worker's termination timestamp.
func remoteCollectionDeadline(a *remoteActive) (*time.Time, error) {
	if a.ReleaseAt == nil {
		return nil, nil
	}
	end, err := backend.DataDeadline(*a.ReleaseAt, a.Budgets[2])
	if err != nil {
		return nil, err
	}
	return &end, nil
}

// remoteObservationTimes checks trusted native receipt chronology. Reload can
// recheck the lower bound; only the receiving transaction knows the upper bound.
func remoteObservationTimes(a *remoteActive, o backend.DataObservation, now *time.Time) bool {
	within := func(t time.Time) bool {
		return !t.Before(a.Began) && (now == nil || !t.After(*now))
	}
	exit := func(x *backend.ContainerExit) bool {
		return x == nil || within(x.StartedAt) && within(x.FinishedAt)
	}
	if o.TaskStart != nil && (!within(o.TaskStart.StartedAt) || !o.TaskStart.StartedAt.Before(a.StageDeadline)) {
		return false
	}
	if o.TaskStop != nil && (!exit(o.TaskStop.Stage) || !exit(o.TaskStop.Setup) || !exit(&o.TaskStop.Task) || !o.TaskStop.Task.StartedAt.Before(a.StageDeadline)) {
		return false
	}
	if o.NoLaunch != nil && (!exit(o.NoLaunch.Stage) || !exit(o.NoLaunch.Setup)) {
		return false
	}
	return exit(o.Collector)
}
