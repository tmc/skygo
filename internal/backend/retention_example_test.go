package backend_test

import (
	"context"
	"fmt"
	"sync"

	"github.com/tmc/skygo/internal/backend"
)

func ExampleOutputStorage() {
	var target backend.Target
	fmt.Println(target.OutputStorage == backend.UnspecifiedOutput)
	target.OutputStorage = backend.RemoteOutput
	fmt.Println(target.OutputReservation)
	// Output:
	// true
	// 0
}

func ExampleOutputReference() {
	id := "0123456789abcdef0123456789abcdef"
	ref := backend.OutputReference{Owner: id, JobID: id, AttemptID: id, Generation: 1}
	fmt.Println(ref.JobID == ref.AttemptID, ref.Generation)
	// Output: true 1
}

func ExampleOutputReceipt() {
	receipt := backend.OutputReceipt{ClosureID: "closed-1"}
	fmt.Println(receipt.ClosureID, receipt.PayloadReleased)
	// Output: closed-1 false
}

func ExampleRetention() {
	// This in-memory example owns no native execution. A real adapter must
	// independently verify writer closure before issuing a receipt.
	id := "0123456789abcdef0123456789abcdef"
	closed := map[string]string{id: "binding"}
	output := map[string]bool{id: true}
	var mu sync.Mutex
	check := func(_ context.Context, ref backend.OutputReference) (backend.OutputReceipt, error) {
		binding, ok := closed[ref.AttemptID]
		if !ok || ref.Owner != id || ref.JobID != id || ref.Generation != 1 || binding != ref.Binding {
			return backend.OutputReceipt{}, backend.ErrIdentityMismatch
		}
		return backend.OutputReceipt{ClosureID: "closed-1"}, nil
	}
	retention := backend.Retention{
		CheckOutputClosed: func(ctx context.Context, ref backend.OutputReference) (backend.OutputReceipt, error) {
			mu.Lock()
			defer mu.Unlock()
			return check(ctx, ref)
		},
		RemoveOutput: func(ctx context.Context, ref backend.OutputReference) error {
			mu.Lock()
			defer mu.Unlock()
			receipt, err := check(ctx, ref)
			if err != nil {
				return err
			}
			if ref.ClosureID != receipt.ClosureID {
				return backend.ErrIdentityMismatch
			}
			delete(output, ref.AttemptID)
			return nil
		},
	}
	ref := backend.OutputReference{Owner: id, JobID: id, AttemptID: id, Binding: "binding", Generation: 1}
	receipt, err := retention.CheckOutputClosed(context.Background(), ref)
	fmt.Println(receipt.ClosureID, err, retention.RemoveExecution == nil)
	ref.ClosureID = receipt.ClosureID
	fmt.Println(retention.RemoveOutput(context.Background(), ref), len(output))
	// The immutable closure survives output removal, permitting an idempotent retry.
	fmt.Println(retention.RemoveOutput(context.Background(), ref), len(output))
	// Output:
	// closed-1 <nil> true
	// <nil> 0
	// <nil> 0
}
