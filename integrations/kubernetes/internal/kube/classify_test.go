package kube

import (
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const classifyUID = "0f0e0d0c-0b0a-4908-8706-050403020100"

var classifyBase = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func classifyAt(seconds int64) metav1.Time {
	return metav1.Time{Time: classifyBase.Add(time.Duration(seconds) * time.Second)}
}

func waitingStatus(name, reason string) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}}}
}

func runningStatus(name, id string, start int64) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, ContainerID: id,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: classifyAt(start)}}}
}

func exitedStatus(name, id string, code int32, start, finish int64) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, ContainerID: id, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
		ContainerID: id, ExitCode: code, StartedAt: classifyAt(start), FinishedAt: classifyAt(finish)}}}
}

func dataPod(init, main []corev1.ContainerStatus) *corev1.Pod {
	p := &corev1.Pod{}
	p.UID = classifyUID
	p.Status.Phase = corev1.PodRunning
	p.Status.InitContainerStatuses = init
	p.Status.ContainerStatuses = main
	return p
}

func classifyTask(setup, outputs bool) backend.Task {
	t := backend.Task{Argv: []string{"run"}, Image: "example.com/task:1",
		Workdir: &artifact.Ref{ID: "0123456789abcdef0123456789abcdef",
			SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", Size: 10}}
	if setup {
		t.Setup = []string{"make", "deps"}
	}
	if outputs {
		t.Outputs = []string{"out"}
	}
	return t
}

func TestClassifyData(t *testing.T) {
	var (
		plain  = classifyTask(false, false)
		setup  = classifyTask(true, false)
		output = classifyTask(false, true)
		full   = classifyTask(true, true)
		mount  = backend.Task{Argv: []string{"run"}, Image: "example.com/task:1",
			DataMounts: []backend.DataMount{{Reference: "data", Destination: "/mnt/skygo/data", ReadOnly: true}}}
	)
	budgets := backend.DataBudgets{StageSeconds: 600, TaskSeconds: 3600, CollectionSeconds: 600, ShutdownSeconds: 120}
	const (
		stageID = "containerd://stage"
		setupID = "containerd://setup"
		taskID  = "containerd://task"
		collID  = "containerd://collector"
		podInit = "PodInitializing"
	)
	stageOK := exitedStatus("stage", stageID, 0, 10, 20)
	setupOK := exitedStatus("setup", setupID, 0, 21, 30)
	blockedTask := waitingStatus("task", podInit)
	blockedColl := waitingStatus("collector", podInit)
	inits := []corev1.ContainerStatus{stageOK, setupOK}
	type list = []corev1.ContainerStatus

	tests := []struct {
		name  string
		task  backend.Task
		pod   *corev1.Pod
		phase backend.DataPhase
		state backend.State
		check func(*testing.T, backend.DataObservation)
	}{
		{name: "pending empty statuses", task: full,
			pod:   func() *corev1.Pod { p := dataPod(nil, nil); p.Status.Phase = corev1.PodPending; return p }(),
			phase: backend.DataStarting, state: backend.Starting},
		{name: "failed empty statuses", task: full,
			pod:   func() *corev1.Pod { p := dataPod(nil, nil); p.Status.Phase = corev1.PodFailed; return p }(),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "stage waiting", task: full,
			pod:   dataPod(list{waitingStatus("stage", podInit), waitingStatus("setup", podInit)}, list{blockedTask, blockedColl}),
			phase: backend.DataStage, state: backend.Starting},
		{name: "stage running", task: full,
			pod:   dataPod(list{runningStatus("stage", stageID, 10), waitingStatus("setup", podInit)}, list{blockedColl, blockedTask}),
			phase: backend.DataStage, state: backend.Starting},
		{name: "stage running task not blocked", task: plain,
			pod:   dataPod(list{runningStatus("stage", stageID, 10)}, list{waitingStatus("task", "ContainerCreating")}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "stage failed no setup no outputs", task: plain,
			pod:   dataPod(list{exitedStatus("stage", stageID, 3, 10, 20)}, list{blockedTask}),
			phase: backend.DataSettled, state: backend.Failed,
			check: func(t *testing.T, o backend.DataObservation) {
				n := o.NoLaunch
				if n == nil || n.FailedInit != backend.StageFailure || n.Stage.ExitCode != 3 || n.SetupBlocked != nil || n.CollectorBlocked != nil {
					t.Fatalf("NoLaunch = %+v", n)
				}
			}},
		{name: "stage failed with setup and outputs", task: full,
			pod:   dataPod(list{exitedStatus("stage", stageID, 1, 10, 20), waitingStatus("setup", podInit)}, list{blockedTask, blockedColl}),
			phase: backend.DataSettled, state: backend.Failed,
			check: func(t *testing.T, o backend.DataObservation) {
				n := o.NoLaunch
				if n == nil || n.SetupBlocked == nil || n.CollectorBlocked == nil || n.TaskBlocked == nil || n.Setup != nil {
					t.Fatalf("NoLaunch = %+v", n)
				}
				if !n.Stage.FinishedAt.Equal(classifyBase.Add(20*time.Second)) || n.Stage.FinishedAt.Location() != time.UTC {
					t.Fatalf("stage finished = %v", n.Stage.FinishedAt)
				}
			}},
		{name: "stage failed setup not blocked", task: setup,
			pod:   dataPod(list{exitedStatus("stage", stageID, 1, 10, 20), waitingStatus("setup", "ContainerCreating")}, list{blockedTask}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "stage failed collector not blocked", task: output,
			pod:   dataPod(list{exitedStatus("stage", stageID, 1, 10, 20)}, list{blockedTask, runningStatus("collector", collID, 25)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "setup running", task: full,
			pod:   dataPod(list{stageOK, runningStatus("setup", setupID, 21)}, list{blockedTask, blockedColl}),
			phase: backend.DataStage, state: backend.Starting},
		{name: "setup running task started", task: setup,
			pod:   dataPod(list{stageOK, runningStatus("setup", setupID, 21)}, list{runningStatus("task", taskID, 22)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "setup failed", task: full,
			pod:   dataPod(list{stageOK, exitedStatus("setup", setupID, 2, 21, 30)}, list{blockedTask, blockedColl}),
			phase: backend.DataSettled, state: backend.Failed,
			check: func(t *testing.T, o backend.DataObservation) {
				n := o.NoLaunch
				if n == nil || n.FailedInit != backend.SetupFailure || n.Setup == nil || n.Setup.ExitCode != 2 || n.SetupBlocked != nil {
					t.Fatalf("NoLaunch = %+v", n)
				}
			}},
		{name: "setup failed task not blocked", task: setup,
			pod:   dataPod(list{stageOK, exitedStatus("setup", setupID, 2, 21, 30)}, list{waitingStatus("task", "ErrImagePull")}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "setup started before stage finished", task: setup,
			pod:   dataPod(list{stageOK, exitedStatus("setup", setupID, 2, 15, 30)}, list{blockedTask}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "setup shares stage identity", task: setup,
			pod:   dataPod(list{stageOK, exitedStatus("setup", stageID, 2, 21, 30)}, list{blockedTask}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "init restarted", task: plain,
			pod: func() *corev1.Pod {
				s := stageOK
				s.RestartCount = 1
				return dataPod(list{s}, list{waitingStatus("task", "")})
			}(),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "init last state", task: plain,
			pod: func() *corev1.Pod {
				s := stageOK
				s.LastTerminationState.Terminated = &corev1.ContainerStateTerminated{ExitCode: 1}
				return dataPod(list{s}, list{waitingStatus("task", "")})
			}(),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "two states", task: plain,
			pod: func() *corev1.Pod {
				s := stageOK
				s.State.Waiting = &corev1.ContainerStateWaiting{}
				return dataPod(list{s}, list{waitingStatus("task", "")})
			}(),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "no state", task: plain,
			pod:   dataPod(list{stageOK}, list{{Name: "task"}}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "waiting with container id", task: plain,
			pod: func() *corev1.Pod {
				s := waitingStatus("task", "")
				s.ContainerID = taskID
				return dataPod(list{stageOK}, list{s})
			}(),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "task waiting", task: full,
			pod:   dataPod(inits, list{waitingStatus("task", "ContainerCreating"), blockedColl}),
			phase: backend.DataStarting, state: backend.Starting},
		{name: "task waiting collector exited", task: output,
			pod:   dataPod(list{stageOK}, list{waitingStatus("task", ""), exitedStatus("collector", collID, 0, 25, 26)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "task running", task: full,
			pod:   dataPod(inits, list{runningStatus("task", taskID, 31), runningStatus("collector", collID, 31)}),
			phase: backend.DataTask, state: backend.Running,
			check: func(t *testing.T, o backend.DataObservation) {
				if o.TaskStart == nil || o.TaskStart.ContainerID != taskID || !o.TaskStart.StartedAt.Equal(classifyBase.Add(31*time.Second)) {
					t.Fatalf("TaskStart = %+v", o.TaskStart)
				}
			}},
		{name: "task running collector exited early", task: full,
			pod:   dataPod(inits, list{runningStatus("task", taskID, 31), exitedStatus("collector", collID, 0, 31, 32)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "task running before init finished", task: setup,
			pod:   dataPod(inits, list{runningStatus("task", taskID, 29)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "collector running before init finished", task: full,
			pod:   dataPod(inits, list{runningStatus("task", taskID, 31), runningStatus("collector", collID, 25)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "task succeeded no outputs", task: setup,
			pod:   dataPod(inits, list{exitedStatus("task", taskID, 0, 31, 40)}),
			phase: backend.DataSettled, state: backend.Succeeded,
			check: func(t *testing.T, o backend.DataObservation) {
				s := o.TaskStop
				if s == nil || s.Stage == nil || s.Setup == nil || s.Task.ExitCode != 0 || o.Status.Termination != backend.StoppedWithExitCode || *o.Status.ExitCode != 0 {
					t.Fatalf("observation = %+v", o)
				}
			}},
		{name: "task failed no outputs", task: plain,
			pod:   dataPod(list{stageOK}, list{exitedStatus("task", taskID, 7, 21, 40)}),
			phase: backend.DataSettled, state: backend.Failed,
			check: func(t *testing.T, o backend.DataObservation) {
				if o.TaskStop == nil || o.TaskStop.Setup != nil || *o.Status.ExitCode != 7 || o.Status.Termination != backend.StoppedWithExitCode {
					t.Fatalf("observation = %+v", o)
				}
			}},
		{name: "task exited collector running", task: full,
			pod:   dataPod(inits, list{exitedStatus("task", taskID, 0, 31, 40), runningStatus("collector", collID, 31)}),
			phase: backend.DataCollect, state: backend.Succeeded,
			check: func(t *testing.T, o backend.DataObservation) {
				if o.Collector != nil || o.Status.Termination != backend.UnknownTermination {
					t.Fatalf("observation = %+v", o)
				}
			}},
		{name: "task failed collector waiting", task: output,
			pod:   dataPod(list{stageOK}, list{exitedStatus("task", taskID, 9, 21, 40), waitingStatus("collector", "")}),
			phase: backend.DataCollect, state: backend.Failed},
		{name: "task exited collector exited", task: full,
			pod:   dataPod(inits, list{exitedStatus("collector", collID, 0, 31, 45), exitedStatus("task", taskID, 0, 31, 40)}),
			phase: backend.DataSettled, state: backend.Succeeded,
			check: func(t *testing.T, o backend.DataObservation) {
				if o.Collector == nil || o.Collector.ContainerID != collID || o.Status.Termination != backend.StoppedWithExitCode {
					t.Fatalf("observation = %+v", o)
				}
			}},
		{name: "task failed collector exited", task: full,
			pod:   dataPod(inits, list{exitedStatus("task", taskID, 4, 31, 40), exitedStatus("collector", collID, 1, 31, 41)}),
			phase: backend.DataSettled, state: backend.Failed},
		{name: "collector finished before task", task: full,
			pod:   dataPod(inits, list{exitedStatus("task", taskID, 0, 31, 40), exitedStatus("collector", collID, 0, 31, 39)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "collector started before init finished", task: full,
			pod:   dataPod(inits, list{exitedStatus("task", taskID, 0, 31, 40), exitedStatus("collector", collID, 0, 25, 45)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "deletion timestamp", task: plain,
			pod: func() *corev1.Pod {
				p := dataPod(list{stageOK}, list{exitedStatus("task", taskID, 0, 21, 40)})
				now := classifyAt(50)
				p.DeletionTimestamp = &now
				return p
			}(),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "evicted", task: plain,
			pod: func() *corev1.Pod {
				p := dataPod(list{stageOK}, list{exitedStatus("task", taskID, 137, 21, 40)})
				p.Status.Phase, p.Status.Reason = corev1.PodFailed, "Evicted"
				return p
			}(),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "deadline exceeded", task: plain,
			pod: func() *corev1.Pod {
				p := dataPod(nil, nil)
				p.Status.Phase, p.Status.Reason = corev1.PodFailed, "DeadlineExceeded"
				return p
			}(),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "pod unknown", task: plain,
			pod: func() *corev1.Pod {
				p := dataPod(list{stageOK}, list{runningStatus("task", taskID, 21)})
				p.Status.Phase = corev1.PodUnknown
				return p
			}(),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "ephemeral status", task: plain,
			pod: func() *corev1.Pod {
				p := dataPod(list{stageOK}, list{runningStatus("task", taskID, 21)})
				p.Status.EphemeralContainerStatuses = list{runningStatus("debug", "containerd://debug", 22)}
				return p
			}(),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "extra main status", task: plain,
			pod:   dataPod(list{stageOK}, list{runningStatus("task", taskID, 21), runningStatus("sidecar", "containerd://side", 21)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "missing collector status", task: output,
			pod:   dataPod(list{stageOK}, list{runningStatus("task", taskID, 21)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "missing init status", task: setup,
			pod:   dataPod(list{stageOK}, list{runningStatus("task", taskID, 21)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "duplicate status name", task: output,
			pod:   dataPod(list{stageOK}, list{runningStatus("task", taskID, 21), runningStatus("task", collID, 21)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "init status in main list", task: plain,
			pod:   dataPod(nil, list{stageOK, runningStatus("task", taskID, 21)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "mismatched container id", task: plain,
			pod: func() *corev1.Pod {
				s := exitedStatus("task", taskID, 0, 21, 40)
				s.ContainerID = "containerd://other"
				return dataPod(list{stageOK}, list{s})
			}(),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "unqualified container id", task: plain,
			pod:   dataPod(list{stageOK}, list{exitedStatus("task", "abc123", 0, 21, 40)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "unqualified running id", task: plain,
			pod:   dataPod(list{stageOK}, list{runningStatus("task", "containerd://", 21)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "unprintable container id", task: plain,
			pod:   dataPod(list{stageOK}, list{runningStatus("task", "containerd://a\nb", 21)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "running without start", task: plain,
			pod: func() *corev1.Pod {
				s := runningStatus("task", taskID, 21)
				s.State.Running.StartedAt = metav1.Time{}
				return dataPod(list{stageOK}, list{s})
			}(),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "exit code out of range", task: plain,
			pod:   dataPod(list{stageOK}, list{exitedStatus("task", taskID, 256, 21, 40)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "finished before started", task: plain,
			pod:   dataPod(list{stageOK}, list{exitedStatus("task", taskID, 0, 40, 30)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "task over budget", task: plain,
			pod:   dataPod(list{stageOK}, list{exitedStatus("task", taskID, 0, 21, 21+3601)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "task at budget", task: plain,
			pod:   dataPod(list{stageOK}, list{exitedStatus("task", taskID, 0, 21, 21+3600)}),
			phase: backend.DataSettled, state: backend.Succeeded},
		// Collection deadlines start at the durable release, unknown here;
		// a late collector exit is reported as the native fact, not a timeout.
		{name: "collector past collection seconds after task", task: output,
			pod:   dataPod(list{stageOK}, list{exitedStatus("task", taskID, 0, 21, 40), exitedStatus("collector", collID, 0, 21, 40+601)}),
			phase: backend.DataSettled, state: backend.Succeeded},
		{name: "output collector finished before task", task: output,
			pod:   dataPod(list{stageOK}, list{exitedStatus("task", taskID, 0, 21, 40), exitedStatus("collector", collID, 0, 21, 39)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "stage over budget", task: plain,
			pod:   dataPod(list{exitedStatus("stage", stageID, 0, 10, 10+601)}, list{waitingStatus("task", "")}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "setup over stage budget", task: setup,
			pod:   dataPod(list{stageOK, exitedStatus("setup", setupID, 0, 21, 10+601)}, list{blockedTask}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "task started before init finished", task: setup,
			pod:   dataPod(inits, list{exitedStatus("task", taskID, 0, 25, 40)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "task shares setup identity", task: setup,
			pod:   dataPod(inits, list{exitedStatus("task", setupID, 0, 31, 40)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "collector shares task identity", task: output,
			pod:   dataPod(list{stageOK}, list{exitedStatus("task", taskID, 0, 21, 40), exitedStatus("collector", taskID, 0, 21, 41)}),
			phase: backend.DataHeld, state: backend.Unknown},
		{name: "mount-only waiting", task: mount,
			pod:   dataPod(nil, list{waitingStatus("task", "ContainerCreating")}),
			phase: backend.DataStarting, state: backend.Starting},
		{name: "mount-only running", task: mount,
			pod:   dataPod(nil, list{runningStatus("task", taskID, 1)}),
			phase: backend.DataTask, state: backend.Running},
		{name: "mount-only succeeded", task: mount,
			pod: func() *corev1.Pod {
				p := dataPod(nil, list{exitedStatus("task", taskID, 0, 1, 40)})
				p.Status.Phase = corev1.PodSucceeded
				return p
			}(),
			phase: backend.DataSettled, state: backend.Succeeded,
			check: func(t *testing.T, o backend.DataObservation) {
				if o.TaskStop == nil || o.TaskStop.Stage != nil || o.TaskStop.Setup != nil {
					t.Fatalf("TaskStop = %+v", o.TaskStop)
				}
			}},
		{name: "mount-only failed", task: mount,
			pod:   dataPod(nil, list{exitedStatus("task", taskID, 1, 1, 40)}),
			phase: backend.DataSettled, state: backend.Failed},
		{name: "mount-only unexpected init", task: mount,
			pod:   dataPod(list{stageOK}, list{runningStatus("task", taskID, 21)}),
			phase: backend.DataHeld, state: backend.Unknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyData(tt.pod, tt.task, budgets)
			ref := backend.Reference{ID: "0123456789abcdef0123456789abcdef", Task: tt.task, Binding: "b", Instance: classifyUID}
			if err := backend.ValidateDataObservation(ref, got); err != nil {
				t.Fatalf("ValidateDataObservation: %v (observation %+v)", err, got)
			}
			if got.Phase != tt.phase || got.Status.State != tt.state {
				t.Fatalf("got phase %d state %v (reason %q), want phase %d state %v", got.Phase, got.Status.State, got.Status.Reason, tt.phase, tt.state)
			}
			if got.Status.Instance != classifyUID {
				t.Fatalf("Instance = %q", got.Status.Instance)
			}
			if got.Phase == backend.DataHeld && got.Status.Reason == "" {
				t.Fatal("held observation lacks a reason")
			}
			if tt.check != nil {
				tt.check(t, got)
			}
		})
	}
}
