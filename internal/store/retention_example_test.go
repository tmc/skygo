package store_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// Each example owns a private directory. EnableRetention opens and closes the
// database before it is reopened for admission.
func exampleRetentionDirectory(policy store.RetentionPolicy) (string, store.RetentionInfo, func()) {
	dir, err := os.MkdirTemp("", "skygo-retention-example-")
	if err != nil {
		panic(err)
	}
	info, err := store.EnableRetention(dir, policy)
	if err != nil {
		os.RemoveAll(dir)
		panic(err)
	}
	return dir, info, func() { os.RemoveAll(dir) }
}

func ExampleEnableRetention() {
	dir, enabled, remove := exampleRetentionDirectory(store.DefaultRetentionPolicy())
	defer remove()
	inspected, err := store.InspectRetention(dir) // The installation is stopped.
	if err != nil {
		panic(err)
	}
	s, err := store.Open(dir)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	policy, active, err := s.RetentionPolicy()
	if err != nil {
		panic(err)
	}
	fmt.Println(active, policy.Window == 30*24*time.Hour, inspected.Domain == enabled.Domain)
	bytes, files, err := s.OutputReservations()
	fmt.Println(s.Directory() == dir, bytes, files, err)
	_, err = s.ReusableBytes() // Freelist bytes are not a hard filesystem limit.
	fmt.Println(err)
	// Output:
	// true true true
	// true 0 0 <nil>
	// <nil>
}

func ExampleCompact() {
	dir, enabled, remove := exampleRetentionDirectory(store.DefaultRetentionPolicy())
	defer remove()
	// Compact neither replaces nor deletes the stopped installation.
	err := store.Compact(dir, filepath.Join(dir, "compact.db"))
	if err != nil {
		panic(err)
	}
	original, err := store.InspectRetention(dir)
	fmt.Println(original.Domain == enabled.Domain, err)
	// Output: true <nil>
}

func ExampleStore_SubmitRetained() {
	policy := store.DefaultRetentionPolicy()
	policy.Jobs = 1
	dir, info, remove := exampleRetentionDirectory(policy)
	defer remove()
	s, err := store.Open(dir)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	// Remote output has no local output reservation. No execution is launched.
	task := backend.Task{Argv: []string{"trainer"}}
	job, err := s.SubmitRetained(testActor(), info.Domain, "training", task, "remote", "binding", backend.RemoteOutput, 0)
	if err != nil {
		panic(err)
	}
	replayed, found, err := s.LookupRetained(testActor(), info.Domain, "training", task)
	fmt.Println(found, replayed.ID == job.ID, err)
	_, err = s.SubmitRetained(testActor(), info.Domain, "another", task, "remote", "binding", backend.RemoteOutput, 0)
	var capacity *store.CapacityError
	if !errors.As(err, &capacity) {
		panic(err)
	}
	fmt.Println(capacity.Domain == store.JobsQuota, capacity.Used, capacity.Limit)
	fmt.Println(capacity.Error())
	current, err := s.RetentionInfo(testActor())
	if err != nil {
		panic(err)
	}
	for _, usage := range current.Usage {
		if usage.Domain == store.JobsQuota {
			fmt.Println(usage.Used, usage.Limit)
		}
	}
	page, err := s.ListFor(testActor(), 0, job.Sequence, 10)
	fmt.Println(len(page.Jobs), page.More, err)
	// Output:
	// true true <nil>
	// true 1 1
	// retention capacity reached
	// 1 1
	// 1 false <nil>
}

func ExampleStore_Prune() {
	dir, info, remove := exampleRetentionDirectory(store.DefaultRetentionPolicy())
	defer remove()
	s, err := store.Open(dir)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	task := backend.Task{Argv: []string{"trainer"}}
	job, err := s.SubmitRetained(testActor(), info.Domain, "training", task, "remote", "binding", backend.RemoteOutput, 0)
	if err != nil {
		panic(err)
	}
	// A coordinator's terminal observation is not a native stopped receipt.
	_, err = s.Update(job.ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil })
	if err != nil {
		panic(err)
	}
	now := job.CreatedAt.Add(time.Minute)
	sel := store.PruneSelection{Before: now, Mode: store.MetadataOnly, AcknowledgeUnverifiedTerminal: true}
	subjects, err := s.PruneSubjects(testActor(), sel)
	if err != nil {
		panic(err)
	}
	preview, err := s.Prune(testActor(), sel, false, nil, nil, now)
	fmt.Println(len(subjects), preview.Selected, preview.Marked, err)
	fmt.Println(subjects[0].Job.ID == job.ID)
	_, found, err := s.LookupRetained(testActor(), info.Domain, "training", task)
	fmt.Println(found, err) // Dry-run leaves the accepted result intact.
	marked, err := s.Prune(testActor(), sel, true, nil, nil, now)
	fmt.Println(marked.Marked, err)
	for _, transfer := range marked.Transfers {
		if transfer.Domain == store.JobsQuota {
			fmt.Println(transfer.Added, transfer.Released)
		}
	}
	_, _, err = s.LookupRetained(testActor(), info.Domain, "training", task)
	var expired *store.ExpiryError
	if !errors.As(err, &expired) {
		panic(err)
	}
	fmt.Println(expired.Reason == store.ResultExpired, expired.Error())
	var logExpiry *store.ExpiryError
	fmt.Println(errors.As(s.CheckLogDomain(testActor(), job.ID, job.ID), &logExpiry))
	obligations, err := s.ListObligations(testActor(), 0, 0, 10)
	if err != nil || len(obligations.Items) != 1 {
		panic(fmt.Errorf("obligations: %v", err))
	}
	fmt.Println(obligations.Items[0].NeedsManual)
	_, dispatchable, err := s.NextCleanup(0)
	fmt.Println(dispatchable, err) // Manual reconciliation does not grant a callback.
	// Output:
	// 1 1 0 <nil>
	// true
	// true <nil>
	// 1 <nil>
	// 0 1
	// true retention result or request expired
	// true
	// true
	// false <nil>
}

func ExampleStore_OutputReference() {
	dir, info, remove := exampleRetentionDirectory(store.DefaultRetentionPolicy())
	defer remove()
	s, err := store.Open(dir)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	p := info.Policy
	b, err := local.NewRetained(filepath.Join(dir, "logs"), local.RetentionConfig{Owner: s.Owner(), OutputUnit: p.OutputUnit, Volume: p.Volume, OutputBytes: p.OutputBytes, OutputDomains: p.OutputDomains, OutputFiles: p.OutputFiles})
	if err != nil {
		panic(err)
	}
	defer b.Close()
	binding, err := backend.LocalBinding(s.Owner())
	if err != nil {
		panic(err)
	}
	if err = s.BindOutputStorage("local", binding, backend.LocalOutput, b.OutputReservation()); err != nil {
		panic(err)
	}
	job, err := s.SubmitRetained(testActor(), info.Domain, "training", backend.Task{Argv: []string{"trainer"}}, "local", binding, backend.LocalOutput, b.OutputReservation())
	if err != nil {
		panic(err)
	}
	// Persist start intent and reserve output before handing identity to the
	// backend. This does not call Backend.Start or execute the command.
	job, start, err := s.BeginAttempt(job.ID, job.ID, b.OutputReservation(), job.CreatedAt)
	if err != nil || !start {
		panic(fmt.Errorf("begin attempt: %v", err))
	}
	ref, err := s.OutputReference(job.ID, job.ID)
	if err != nil {
		panic(err)
	}
	fmt.Println(b.BindOutput(ref)) // The real coordinator binds before Start.
	refs, next, err := s.ListOutputReferences("", 10)
	fmt.Println(len(refs), next == "", refs[0] == ref, err)
	fmt.Println(ref.ClosureID == "", ref.PayloadReleased)
	// A committed output reservation is neither writer closure nor stopped proof.

	// Output:
	// <nil>
	// 1 true true <nil>
	// true false
}

func ExampleStore_FinishCleanup_outputOnly() {
	dir, info, remove := exampleRetentionDirectory(store.DefaultRetentionPolicy())
	defer remove()
	s, err := store.Open(dir)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			panic(err)
		}
	}()
	p := info.Policy
	b, err := local.NewRetained(filepath.Join(dir, "logs"), local.RetentionConfig{Owner: s.Owner(), OutputUnit: p.OutputUnit, Volume: p.Volume, OutputBytes: p.OutputBytes, OutputDomains: p.OutputDomains, OutputFiles: p.OutputFiles})
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := b.Close(); err != nil {
			panic(err)
		}
	}()
	binding, err := backend.LocalBinding(s.Owner())
	if err != nil {
		panic(err)
	}
	// This path is inside the newly created private directory. No example code
	// creates it; an ENOENT from fork/exec is required, so no program can run.
	absent := filepath.Join(dir, "absent-program")
	if _, err = os.Lstat(absent); !errors.Is(err, fs.ErrNotExist) {
		panic(fmt.Errorf("absent executable: %v", err))
	}
	task := backend.Task{Argv: []string{absent}}
	job, err := s.SubmitRetained(testActor(), info.Domain, "failed-start", task, "local", binding, backend.LocalOutput, b.OutputReservation())
	if err != nil {
		panic(err)
	}
	job, start, err := s.BeginAttempt(job.ID, job.ID, b.OutputReservation(), job.CreatedAt)
	if err != nil || !start {
		panic(fmt.Errorf("begin attempt: %v", err))
	}
	ref, err := s.OutputReference(job.ID, job.ID)
	if err != nil {
		panic(err)
	}
	if err = b.BindOutput(ref); err != nil {
		panic(err)
	}
	ctx := context.Background()
	startErr := b.Start(ctx, job.ID, task)
	var pathErr *os.PathError
	if !errors.Is(startErr, fs.ErrNotExist) || !errors.As(startErr, &pathErr) || pathErr.Op != "fork/exec" || pathErr.Path != absent {
		panic(fmt.Errorf("require failed exec of private absent path: %v", startErr))
	}
	status, err := b.Observe(ctx, backend.Reference{ID: job.ID, Task: task, Binding: binding})
	if err != nil || status.State != backend.Failed || status.Termination != backend.UnknownTermination || status.ExitCode != nil || status.Instance != "" {
		panic(fmt.Errorf("failed-start observation: %v", err))
	}
	job, err = s.ObserveAttempt(job.ID, job.ID, status, job.CreatedAt.Add(time.Second))
	if err != nil {
		panic(err)
	}
	// The real failed-start lifecycle has closed pipes/log and released command
	// payload. That is output closure only, never native stopped proof.
	callbacks := b.Retention()
	receipt, err := callbacks.CheckOutputClosed(ctx, ref)
	if err != nil || receipt.ClosureID == "" || !receipt.PayloadReleased {
		panic(fmt.Errorf("output closure: %v", err))
	}
	fmt.Println(status.State, status.Termination == backend.UnknownTermination, receipt.PayloadReleased)
	now := job.UpdatedAt.Add(time.Minute)
	selection := store.PruneSelection{Before: now, Mode: store.OutputOnly}
	subjects, err := s.PruneSubjects(testActor(), selection)
	if err != nil || len(subjects) != 1 || subjects[0].Job.ID != job.ID {
		panic(fmt.Errorf("prune subjects: %v", err))
	}
	ref.ClosureID, ref.PayloadReleased = receipt.ClosureID, receipt.PayloadReleased
	if err = s.RecordOutputClosure(ref, subjects[0].Digest); err != nil {
		panic(err)
	}
	marked, err := s.Prune(testActor(), selection, true, nil, nil, now)
	if err != nil || marked.Marked != 1 {
		panic(fmt.Errorf("mark output: %v", err))
	}
	record, found, err := s.NextCleanup(0)
	if err != nil || !found || record.Operation != store.OutputCleanup || record.Stopped || record.Output != ref {
		panic(fmt.Errorf("output cleanup record: %v", err))
	}
	fmt.Println(marked.Marked, record.Operation == store.OutputCleanup, record.Stopped)
	// Cancellation is a real adapter failure. Its durable record and reservation
	// remain available for the same identity/digest on retry.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	removeErr := callbacks.RemoveOutput(canceled, record.Output)
	if !errors.Is(removeErr, context.Canceled) {
		panic(fmt.Errorf("canceled output removal: %v", removeErr))
	}
	if err = s.FailCleanup(record.ID, record.Digest, removeErr.Error(), now.Add(time.Second)); err != nil {
		panic(err)
	}
	retry, found, err := s.NextCleanup(0)
	if err != nil || !found || retry.ID != record.ID || retry.Digest != record.Digest {
		panic(fmt.Errorf("retry cleanup record: %v", err))
	}
	reserved, _, err := s.OutputReservations()
	if err != nil {
		panic(err)
	}
	usage, err := b.OutputUsage(ctx)
	if err != nil || usage.Files != 1 {
		panic(fmt.Errorf("failed removal retained log: %v", err))
	}
	fmt.Println(reserved == b.OutputReservation(), retry.Operation == store.OutputCleanup)
	if err = callbacks.RemoveOutput(ctx, retry.Output); err != nil {
		panic(err)
	}
	if err = s.FinishCleanup(retry.ID, retry.Operation, retry.Digest, now.Add(2*time.Second)); err != nil {
		panic(err)
	}
	reserved, files, err := s.OutputReservations()
	fmt.Println(reserved, files, err)
	usage, err = b.OutputUsage(ctx)
	if err != nil || usage.Files != 0 || usage.LogicalBytes != 0 || usage.AllocatedBytes != 0 {
		panic(fmt.Errorf("removed output inventory: %v", err))
	}
	if _, err = os.Lstat(filepath.Join(dir, "logs", job.ID+".log")); !errors.Is(err, fs.ErrNotExist) {
		panic(fmt.Errorf("removed log: %v", err))
	}
	var expired *store.ExpiryError
	if !errors.As(s.CheckLogDomain(testActor(), job.ID, job.ID), &expired) {
		panic("missing output expiry")
	}
	current, err := s.Get(job.ID)
	fmt.Println(expired.Reason == store.LogsExpired, reflect.DeepEqual(current, job), err)
	_, pending, err := s.NextCleanup(0)
	fmt.Println(pending, err)
	// Output:
	// failed true true
	// 1 true false
	// true true
	// 0 0 <nil>
	// true true <nil>
	// false <nil>
}
