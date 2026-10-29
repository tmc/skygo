package store

import (
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

const (
	workerBarrierGroupLimit  = 4096
	workerBarrierMemberLimit = 8192
)

type workerBarrierGroupRecord struct {
	Version   uint8                 `json:"version"`
	Decision  WorkerBarrierDecision `json:"decision"`
	DecidedAt time.Time             `json:"decided_at"`
}

type workerBarrierMemberRecord struct {
	Version           uint8                           `json:"version"`
	Service           workerPodRecord                 `json:"service"`
	Start             workerEffectRecord              `json:"start"`
	Recover           workerEffectRecord              `json:"recover"`
	StartToken        string                          `json:"start_token"`
	StartAck          bool                            `json:"start_ack"`
	StartRevision     string                          `json:"start_revision"`
	Observation       *workerBarrierObservationRecord `json:"observation"`
	Stop              backend.WorkerStop              `json:"stop"`
	StopAt            time.Time                       `json:"stop_at"`
	RemoveHadPod      bool                            `json:"remove_had_pod"`
	RecoverHadPod     bool                            `json:"recover_had_pod"`
	PodRemoveSent     bool                            `json:"pod_remove_sent"`
	ServiceRemoveSent bool                            `json:"service_remove_sent"`
	ServiceRemoved    bool                            `json:"service_removed"`
	CreateRefused     bool                            `json:"create_refused"`
	Resolve           workerResolveRecord             `json:"resolve"`
	Vanished          bool                            `json:"vanished"`
	LocalUnsettled    bool                            `json:"local_unsettled"`
}

type workerBarrierObservationRecord struct {
	Revision       string    `json:"revision"`
	Gated          bool      `json:"gated"`
	Node           string    `json:"node"`
	Prepared       bool      `json:"prepared"`
	Started        bool      `json:"started"`
	Deleting       bool      `json:"deleting"`
	RendezvousHost string    `json:"rendezvous_host"`
	RendezvousPort uint16    `json:"rendezvous_port"`
	Routed         bool      `json:"routed"`
	At             time.Time `json:"at"`
}

type workerResolveRecord struct {
	Class     WorkerResolveClass `json:"class"`
	Refused   bool               `json:"refused"`
	At        time.Time          `json:"at"`
	Unsettled bool               `json:"unsettled,omitempty"`
}

func workerBarrierSpecValid(s backend.WorkerSpec) bool {
	n, err := backend.NormalizeWorkers(&s)
	return err == nil && n != nil && *n == s
}

func workerRecordGroupLimit(g workerGroupRecord) int {
	if g.Barrier != nil {
		return workerBarrierGroupLimit
	}
	return workerGroupLimit
}

func workerRecordReserve(g workerGroupRecord) uint64 {
	if g.Barrier == nil {
		return workerGroupReserve(g.Spec.Count)
	}
	return workerBarrierGroupLimit + uint64(len(workerGroupKey(g.Job))) + uint64(g.Spec.Count)*(workerBarrierMemberLimit+uint64(len(workerMemberKey(g.Job, 0))))
}

func validWorkerBarrierGroupRecord(g workerBarrierGroupRecord) error {
	if g.Version != 1 || g.Decision > WorkerBarrierAbort || !workerTime(g.DecidedAt) || (g.Decision == WorkerBarrierUndecided) != g.DecidedAt.IsZero() {
		return errors.New("invalid worker barrier decision")
	}
	return nil
}

func workerBarrierResource(p workerPodRecord) bool {
	if p == (workerPodRecord{}) {
		return true
	}
	if len(p.Name) == 0 || len(p.Name) > 63 || len(p.UID) == 0 || len(p.UID) > 128 || !workerDNS(p.Name, 63) {
		return false
	}
	for _, c := range []byte(p.UID) {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}

func workerDNS(s string, limit int) bool {
	if len(s) == 0 || len(s) > limit {
		return false
	}
	for _, part := range strings.Split(s, ".") {
		if len(part) == 0 || len(part) > 63 || part[0] == '-' || part[len(part)-1] == '-' {
			return false
		}
		for _, c := range []byte(part) {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

func validWorkerBarrierMember(m workerMemberRecord) error {
	b := m.Barrier
	if b.Version != 1 || !workerBarrierResource(m.Pod) || !workerBarrierResource(b.Service) || !workerTime(b.StopAt) || b.Stop > backend.WorkerTerminated || (b.Stop == backend.WorkerStopUnproven) != b.StopAt.IsZero() {
		return errors.New("invalid worker barrier member")
	}
	if b.Service.UID != "" && m.Create.State != WorkerRecorded && m.Create.State != WorkerUnknown {
		return errors.New("worker service without create result")
	}
	if b.Stop != backend.WorkerStopUnproven && m.Pod.UID == "" {
		return errors.New("worker stop without original pod")
	}
	if b.StartToken != "" && (!artifactHex(b.StartToken, 64) || m.Pod.UID == "") || b.StartAck && b.Start.State != WorkerRecorded || b.StartRevision != workerText(b.StartRevision, workerRevisionLimit) || b.StartRevision != "" && !b.StartAck {
		return errors.New("invalid worker start receipt")
	}
	if b.Start.State != WorkerNone && b.StartToken == "" {
		return errors.New("worker start without token")
	}
	if b.RemoveHadPod && (m.Pod.UID == "" || m.Remove.State == WorkerNone) || m.Remove.State != WorkerNone && !b.RemoveHadPod && (b.Service.UID == "" || m.Pod.UID != "" && !m.Removed) {
		return errors.New("invalid original worker remove reference")
	}
	if b.RecoverHadPod && (m.Pod.UID == "" || !b.RemoveHadPod || b.Recover.State == WorkerNone) || b.Recover.State != WorkerNone && !b.RecoverHadPod && m.Pod.UID != "" && !m.Removed {
		return errors.New("invalid original worker recover reference")
	}
	if b.Recover.State != WorkerNone && (m.Pod.UID == "" && !b.CreateRefused || m.Remove.State != WorkerUnknown || b.RecoverHadPod && !b.PodRemoveSent || b.Service.UID != "" && !b.ServiceRemoveSent || workerBarrierRecoveryEvidence(m) == backend.WorkerStopUnproven) {
		return errors.New("worker recovery without original dispatch and stop evidence")
	}
	if b.PodRemoveSent && (m.Pod.UID == "" || m.Remove.State == WorkerNone || !b.RemoveHadPod) || b.ServiceRemoveSent && (b.Service.UID == "" || m.Remove.State == WorkerNone) || b.ServiceRemoved && (b.Service.UID == "" || m.Remove.State == WorkerNone && b.Recover.State == WorkerNone) {
		return errors.New("invalid worker per-kind removal")
	}
	if b.CreateRefused && m.Create.State != WorkerUnknown && m.Create.State != WorkerRecorded {
		return errors.New("worker refusal without create result")
	}
	if m.Create.State == WorkerRecorded && m.Pod.UID == "" && !b.CreateRefused {
		return errors.New("worker create without pod or refusal")
	}
	if m.Observed != nil && m.Observed.ExitCode != nil && b.Stop != backend.WorkerTerminated {
		return errors.New("worker exit without positive termination")
	}
	r := b.Resolve
	if r.Class > WorkerResolveMismatch || !workerTime(r.At) || (r.Class == WorkerResolveNone) != r.At.IsZero() || r.Refused && r.Class == WorkerResolveNone || r.Class != WorkerResolveNone && m.Create.State != WorkerUnknown || r.Unsettled && (r.Class == WorkerResolveNone || !b.LocalUnsettled) {
		return errors.New("invalid worker resolve result")
	}
	if o := b.Observation; o != nil {
		if m.Pod.UID == "" || o.Revision != workerText(o.Revision, workerRevisionLimit) || o.Node != "" && !workerDNS(o.Node, workerNodeLimit) || o.RendezvousHost != "" && !workerDNS(o.RendezvousHost, 253) || o.RendezvousPort != 0 && o.RendezvousPort != 29500 || !workerTime(o.At) || o.At.IsZero() {
			return errors.New("invalid worker barrier observation")
		}
		if o.Prepared && b.Stop != backend.WorkerStopUnproven {
			return errors.New("prepared stopped worker")
		}
		if b.Stop == backend.WorkerNeverBound && (!o.Deleting || o.Node != "" || o.Prepared) {
			return errors.New("invalid never-bound worker observation")
		}
		if o.Routed && (b.Service.UID == "" || o.RendezvousHost == "" || o.RendezvousPort != 29500) {
			return errors.New("invalid worker rendezvous route")
		}
	}
	return nil
}

func workerBarrierObservation(m workerMemberRecord) *backend.WorkerBarrier {
	b := m.Barrier
	if b == nil || b.Observation == nil {
		return nil
	}
	o := b.Observation
	out := &backend.WorkerBarrier{Pod: m.pod(), Revision: o.Revision, Gated: o.Gated, Node: o.Node, Prepared: o.Prepared, Started: o.Started, Deleting: o.Deleting, Stop: b.Stop}
	if b.Service.UID != "" {
		out.Rendezvous = backend.WorkerRendezvous{Service: backend.WorkerResource{Name: b.Service.Name, UID: b.Service.UID}, Host: o.RendezvousHost, Port: o.RendezvousPort, Routed: o.Routed}
	}
	if x := m.Observed; x != nil && x.ExitCode != nil {
		out.Exit = &backend.ContainerExit{ContainerID: x.Container, StartedAt: x.Started, FinishedAt: x.Finished, ExitCode: *x.ExitCode}
	}
	return out
}

func workerBarrierMemberView(m workerMemberRecord) *WorkerBarrierMember {
	b := m.Barrier
	if b == nil {
		return nil
	}
	out := &WorkerBarrierMember{Version: b.Version, Service: backend.WorkerResource{Name: b.Service.Name, UID: b.Service.UID}, Start: workerEffectView(b.Start), Recover: workerEffectView(b.Recover), StartAck: b.StartAck, StartRevision: b.StartRevision, Observation: workerBarrierObservation(m), Stop: b.Stop, StopAt: b.StopAt, RemoveHadPod: b.RemoveHadPod, RecoverHadPod: b.RecoverHadPod, PodRemoveSent: b.PodRemoveSent, ServiceRemoveSent: b.ServiceRemoveSent, ServiceRemoved: b.ServiceRemoved, CreateRefused: b.CreateRefused, Resolve: WorkerResolveOutcome{Class: b.Resolve.Class, Refused: b.Resolve.Refused, At: b.Resolve.At, Unsettled: b.Resolve.Unsettled}, Vanished: b.Vanished, LocalUnsettled: b.LocalUnsettled}
	if b.Observation != nil {
		out.ObservedAt = b.Observation.At
	}
	token, _ := hex.DecodeString(b.StartToken)
	copy(out.StartToken[:], token)
	return out
}

func validateWorkerBarrierGroup(tx *bolt.Tx, g workerGroupRecord, ms []workerMemberRecord) error {
	if schemaNumber(tx) < workerBarrierSchema {
		return errors.New("worker barrier requires schema 15")
	}
	j, err := get(tx, g.Job)
	if err != nil {
		return err
	}
	want, err := workerBarrierDemand(j.Task, g.Spec)
	if err != nil || want != g.Demand {
		return errors.New("worker barrier demand differs from task")
	}
	if j.Status.State == backend.Unknown || j.Status.State == backend.Retrying {
		return errors.New("invalid worker barrier job status")
	}
	if g.Barrier.Decision == WorkerBarrierAbort && !j.Status.State.Terminal() {
		// A local-owner hold freezes aggregation, but recording Abort still
		// prevents any future start. It must remain readable in that state.
		latched := false
		for _, m := range ms {
			latched = latched || m.Barrier != nil && m.Barrier.LocalUnsettled
		}
		if !latched {
			return errors.New("nonterminal aborted worker barrier")
		}
	}
	ref := workerGroupIdentity(j, g)
	creates, unknown := 0, 0
	for rank, m := range ms {
		b := m.Barrier
		if m.Job != g.Job || int(m.Rank) != rank || b == nil {
			return errors.New("mixed worker barrier records")
		}
		if err := validWorkerMember(m); err != nil {
			return err
		}
		if g.Spec.Mode == backend.WorkerTCP && rank == 0 && m.Create.State == WorkerRecorded && !b.CreateRefused && b.Service.UID == "" {
			return errors.New("complete TCP create missing service")
		}
		if b.Service != (workerPodRecord{}) && (g.Spec.Mode != backend.WorkerTCP || rank != 0) {
			return errors.New("unexpected worker service owner")
		}
		if m.Create.State != WorkerNone {
			creates++
		}
		if m.Create.State == WorkerUnknown {
			unknown++
		}
		if m.Pod.UID != "" {
			r := backend.WorkerReference{Group: ref, Rank: m.Rank, Pod: m.pod()}
			if g.Spec.Mode == backend.WorkerTCP && ms[0].Barrier != nil {
				s := ms[0].Barrier.Service
				r.Service = backend.WorkerResource{Name: s.Name, UID: s.UID}
			}
			if err := backend.ValidateWorkerReference(r, false); err != nil {
				return err
			}
			if b.StartToken != "" {
				tok, err := backend.WorkerStartToken(r)
				if err != nil || hex.EncodeToString(tok[:]) != b.StartToken {
					return errors.New("invalid worker start token binding")
				}
			}
		}
		if g.Barrier.Decision != WorkerBarrierStart && (b.StartToken != "" || b.Start.State != WorkerNone) {
			return errors.New("worker start before decision")
		}
		if g.Barrier.Decision == WorkerBarrierStart && (m.Create.State != WorkerRecorded || !m.ReleaseAck) {
			return errors.New("worker start decision before complete release")
		}
		if g.Barrier.Decision == WorkerBarrierStart && b.StartToken == "" {
			return errors.New("worker decision missing rank token")
		}
		if b.Observation != nil && b.Observation.Started && b.StartToken == "" {
			return errors.New("worker token observed before decision")
		}
	}
	switch g.Phase {
	case WorkerGroupQueued, WorkerGroupAdmitted:
		if creates != 0 || j.Status.State != backend.Queued {
			return errors.New("invalid queued worker barrier")
		}
	case WorkerGroupActive:
		if creates == 0 || unknown != 0 || j.Status.State == backend.Queued {
			return errors.New("invalid active worker barrier")
		}
	case WorkerGroupHeld:
		if unknown == 0 || j.Status.State == backend.Queued {
			return errors.New("invalid held worker barrier")
		}
	case WorkerGroupDone:
		if !j.Status.State.Terminal() || j.Status.State == backend.Unknown {
			return errors.New("nonterminal settled worker barrier")
		}
		for _, m := range ms {
			if !workerBarrierMemberSettled(m) {
				return errors.New("unsettled worker barrier member")
			}
		}
	}
	if j.Status.State == backend.Succeeded {
		if g.Barrier.Decision != WorkerBarrierStart {
			return errors.New("worker success without start decision")
		}
		for _, m := range ms {
			if m.Barrier.Start.State == WorkerNone || m.Observed == nil || m.Observed.ExitCode == nil || *m.Observed.ExitCode != 0 {
				return errors.New("worker success without each main exit")
			}
		}
	}
	return nil
}
