package kube

import (
	"slices"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// classifyKind is the single current state of one participant status.
type classifyKind uint8

const (
	classifyWaiting classifyKind = iota + 1
	classifyRunning
	classifyTerminated
)

// classifyPart is one qualified participant status.
type classifyPart struct {
	kind   classifyKind
	status *corev1.ContainerStatus
}

func (p classifyPart) blocked() bool {
	return p.kind == classifyWaiting && p.status.State.Waiting.Reason == "PodInitializing"
}

func (p classifyPart) exit() backend.ContainerExit {
	x := p.status.State.Terminated
	return backend.ContainerExit{ContainerID: x.ContainerID, ExitCode: x.ExitCode,
		StartedAt: x.StartedAt.Time.UTC(), FinishedAt: x.FinishedAt.Time.UTC()}
}

func (p classifyPart) startedAt() time.Time {
	if p.kind == classifyRunning {
		return p.status.State.Running.StartedAt.Time.UTC()
	}
	return p.status.State.Terminated.StartedAt.Time.UTC()
}

// classifyData classifies one snapshot of the original data Pod. The caller
// has already verified its UID, name, ownership and spec profile, and compares
// two observations before trusting a terminal or no-launch result. Anything
// outside the exact expected participant shape is Held, never terminal.
func classifyData(p *corev1.Pod, t backend.Task, budgets backend.DataBudgets) backend.DataObservation {
	uid := string(p.UID)
	held := func(reason string) backend.DataObservation {
		return backend.DataObservation{Status: backend.Status{State: backend.Unknown, Instance: uid, Reason: reason}, Phase: backend.DataHeld}
	}
	starting := func(phase backend.DataPhase) backend.DataObservation {
		return backend.DataObservation{Status: backend.Status{State: backend.Starting, Instance: uid}, Phase: phase}
	}
	switch {
	case p.DeletionTimestamp != nil:
		return held("data pod is deleting")
	case p.Status.Phase == corev1.PodUnknown:
		return held("data pod state unknown")
	case p.Status.Reason != "":
		return held("data pod stopped by kubelet")
	case len(p.Status.EphemeralContainerStatuses) != 0:
		return held("data pod has ephemeral containers")
	}
	if len(p.Status.InitContainerStatuses) == 0 && len(p.Status.ContainerStatuses) == 0 {
		if p.Status.Phase != "" && p.Status.Phase != corev1.PodPending {
			return held("data pod lacks container statuses")
		}
		return starting(backend.DataStarting)
	}

	var initNames, mainNames []string
	if t.Workdir != nil {
		initNames = append(initNames, "stage")
	}
	if len(t.Setup) != 0 {
		initNames = append(initNames, "setup")
	}
	mainNames = append(mainNames, "task")
	if len(t.Outputs) != 0 {
		mainNames = append(mainNames, "collector")
	}
	parts := make(map[string]classifyPart)
	if !classifyStatuses(parts, p.Status.InitContainerStatuses, initNames) ||
		!classifyStatuses(parts, p.Status.ContainerStatuses, mainNames) {
		return held("data pod container statuses differ")
	}
	ids := make(map[string]bool)
	for _, part := range parts {
		if id := part.status.ContainerID; id != "" {
			if ids[id] {
				return held("data containers share an identity")
			}
			ids[id] = true
		}
	}

	stage, hasStage := parts["stage"]
	setup, hasSetup := parts["setup"]
	task := parts["task"]
	collector, hasCollector := parts["collector"]
	// laterBlocked reports whether every participant after the named init is
	// an explicit PodInitializing wait.
	laterBlocked := func(afterStage bool) bool {
		return (!afterStage || !hasSetup || setup.blocked()) && task.blocked() && (!hasCollector || collector.blocked())
	}
	noLaunch := func(reason string) backend.DataObservation {
		n := &backend.DataNoLaunch{TaskBlocked: &backend.DataNotStarted{Name: "task", WaitingReason: "PodInitializing"}}
		if hasCollector {
			n.CollectorBlocked = &backend.DataNotStarted{Name: "collector", WaitingReason: "PodInitializing"}
		}
		return backend.DataObservation{Status: backend.Status{State: backend.Failed, Instance: uid, Reason: reason}, NoLaunch: n, Phase: backend.DataSettled}
	}

	var stageExit, setupExit *backend.ContainerExit
	var initEnd time.Time
	if hasStage {
		if stage.kind != classifyTerminated {
			if !laterBlocked(true) {
				return held("data participants started before stage")
			}
			return starting(backend.DataStage)
		}
		x := stage.exit()
		if !classifyWithin(x.FinishedAt, x.StartedAt, budgets.StageSeconds) {
			return held("data stage exceeded its budget")
		}
		if x.ExitCode != 0 {
			if !laterBlocked(true) {
				return held("data participants started after stage failure")
			}
			o := noLaunch("data stage failed")
			o.NoLaunch.FailedInit, o.NoLaunch.Stage = backend.StageFailure, &x
			if hasSetup {
				o.NoLaunch.SetupBlocked = &backend.DataNotStarted{Name: "setup", WaitingReason: "PodInitializing"}
			}
			return o
		}
		stageExit, initEnd = &x, x.FinishedAt
	}
	if hasSetup {
		if !hasStage {
			return held("data setup lacks stage")
		}
		if setup.kind != classifyWaiting && setup.startedAt().Before(initEnd) {
			return held("data setup started before stage finished")
		}
		if setup.kind != classifyTerminated {
			if !laterBlocked(false) {
				return held("data participants started before setup")
			}
			return starting(backend.DataStage)
		}
		x := setup.exit()
		if !classifyWithin(x.FinishedAt, stageExit.StartedAt, budgets.StageSeconds) {
			return held("data stage exceeded its budget")
		}
		if x.ExitCode != 0 {
			if !laterBlocked(false) {
				return held("data participants started after setup failure")
			}
			o := noLaunch("data setup failed")
			o.NoLaunch.FailedInit, o.NoLaunch.Stage, o.NoLaunch.Setup = backend.SetupFailure, stageExit, &x
			return o
		}
		setupExit, initEnd = &x, x.FinishedAt
	}

	if hasCollector && collector.kind != classifyWaiting && collector.startedAt().Before(initEnd) {
		return held("data collector started before init finished")
	}
	switch task.kind {
	case classifyWaiting:
		if hasCollector && collector.kind == classifyTerminated {
			return held("data collector exited before task")
		}
		return starting(backend.DataStarting)
	case classifyRunning:
		if hasCollector && collector.kind == classifyTerminated {
			return held("data collector exited before task")
		}
		start := task.startedAt()
		if start.Before(initEnd) {
			return held("data task started before init finished")
		}
		return backend.DataObservation{
			Status:    backend.Status{State: backend.Running, Instance: uid},
			TaskStart: &backend.ContainerStart{ContainerID: task.status.ContainerID, StartedAt: start},
			Phase:     backend.DataTask,
		}
	}
	x := task.exit()
	if x.StartedAt.Before(initEnd) {
		return held("data task started before init finished")
	}
	if !classifyWithin(x.FinishedAt, x.StartedAt, budgets.TaskSeconds) {
		return held("data task exceeded its budget")
	}
	code := x.ExitCode
	o := backend.DataObservation{
		Status:   backend.Status{State: backend.Succeeded, Instance: uid, ExitCode: &code},
		TaskStop: &backend.DataTaskStop{Stage: stageExit, Setup: setupExit, Task: x},
		Phase:    backend.DataSettled,
	}
	if code != 0 {
		o.Status.State = backend.Failed
	}
	if hasCollector && collector.kind != classifyTerminated {
		o.Phase = backend.DataCollect
		return o
	}
	if hasCollector {
		// Collection begins at the trusted release, which Kubernetes does not
		// record; Store and Control enforce its deadline from that time. The
		// classifier reports the native exit facts and checks only chronology.
		c := collector.exit()
		if c.FinishedAt.Before(x.FinishedAt) {
			return held("data collector finished before task")
		}
		o.Collector = &c
	}
	o.Status.Termination = backend.StoppedWithExitCode
	return o
}

// classifyStatuses adds the qualified statuses of list to parts. The list must
// hold exactly one status for each of names, in any order.
func classifyStatuses(parts map[string]classifyPart, list []corev1.ContainerStatus, names []string) bool {
	if len(list) != len(names) {
		return false
	}
	for i := range list {
		s := &list[i]
		if _, dup := parts[s.Name]; dup || !slices.Contains(names, s.Name) {
			return false
		}
		kind := classifyState(s)
		if kind == 0 {
			return false
		}
		parts[s.Name] = classifyPart{kind: kind, status: s}
	}
	return true
}

// classifyState returns the kind of a current never-restarted status with
// exactly one qualified state, or zero.
func classifyState(s *corev1.ContainerStatus) classifyKind {
	last := s.LastTerminationState
	if s.RestartCount != 0 || last.Waiting != nil || last.Running != nil || last.Terminated != nil {
		return 0
	}
	w, r, x := s.State.Waiting, s.State.Running, s.State.Terminated
	switch {
	case w != nil && r == nil && x == nil:
		if s.ContainerID != "" {
			return 0
		}
		return classifyWaiting
	case w == nil && r != nil && x == nil:
		if !classifyID(s.ContainerID) || !classifyTime(r.StartedAt) {
			return 0
		}
		return classifyRunning
	case w == nil && r == nil && x != nil:
		if !classifyID(x.ContainerID) || x.ContainerID != s.ContainerID || x.ExitCode < 0 || x.ExitCode > 255 ||
			!classifyTime(x.StartedAt) || !classifyTime(x.FinishedAt) || x.FinishedAt.Time.Before(x.StartedAt.Time) {
			return 0
		}
		return classifyTerminated
	}
	return 0
}

// classifyID reports whether id is a bounded printable "runtime://id" value.
func classifyID(id string) bool {
	if len(id) > 128 {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] < 0x20 || id[i] > 0x7e {
			return false
		}
	}
	runtime, rest, ok := strings.Cut(id, "://")
	return ok && runtime != "" && rest != ""
}

// classifyTime reports whether t is positive and within the supported range.
func classifyTime(t metav1.Time) bool {
	return t.Time.Unix() > 0 && t.Time.UTC().Year() <= 9999
}

// classifyWithin reports whether end is no later than start plus seconds.
func classifyWithin(end, start time.Time, seconds int64) bool {
	deadline, err := backend.DataDeadline(start, seconds)
	return err == nil && !end.After(deadline)
}
