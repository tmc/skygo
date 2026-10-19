package rpc_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
)

func TestRetentionDiscoveryReadOnlyAndScoped(t *testing.T) {
	f := scoped(t, &logBackend{})
	admin, err := f.engine.Authenticate("Bearer " + f.tokens["admin"])
	if err != nil {
		t.Fatal(err)
	}
	// The accepted audit queue has256 slots. Leave one free so an accidental
	// persistent-audit enqueue by any discovery below is observable afterward.
	for i := 0; i < 255; i++ {
		if err := f.control.RecordAccess(admin, "get", "", "allowed"); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(f.store.Directory(), "jobs.db")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	reader, err := f.client.GetRetention(ctx, authorized(f.tokens["reader"], &v1.GetRetentionRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if reader.Msg.Enabled || reader.Msg.Admin != nil || reader.Msg.Domain != nil {
		t.Fatalf("reader disclosure: %v", reader.Msg)
	}
	_, err = f.client.GetRetention(ctx, authorized(f.tokens["reader"], &v1.GetRetentionRequest{PageSize: 1}))
	code(t, err, connect.CodePermissionDenied)
	info, err := f.client.GetRetention(ctx, authorized(f.tokens["admin"], &v1.GetRetentionRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if info.Msg.Admin == nil || len(info.Msg.Admin.Usage) != 12 {
		t.Fatalf("admin projection: %v", info.Msg)
	}
	if info.Msg.Admin.ObservedAt != nil || info.Msg.Admin.DatabaseFileBytes != nil || info.Msg.Admin.OutputAllocatedBytes != nil {
		t.Fatal("discovery invented a physical sample without Run")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("quiescent discovery changed database bytes")
	}
	if err := f.control.RecordAccess(admin, "get", "", "allowed"); err != nil {
		t.Fatalf("discovery consumed durable audit capacity: %v", err)
	}
	if !f.control.AuditHealthy() {
		t.Fatal("discovery damaged audit health")
	}
}

func TestRetainedSubmissionInputAndTypedDomainError(t *testing.T) {
	f := scoped(t, &logBackend{})
	ctx := context.Background()
	message := &v1.SubmitRetainedJobRequest{RequestId: "stable", Task: &v1.Task{Argv: []string{"unused"}}}
	_, err := f.client.SubmitRetainedJob(ctx, authorized(f.tokens["reader"], message))
	code(t, err, connect.CodePermissionDenied)
	_, err = f.client.SubmitRetainedJob(ctx, authorized(f.tokens["alice"], message))
	code(t, err, connect.CodeFailedPrecondition)
	var typed *connect.Error
	if !errors.As(err, &typed) {
		t.Fatal(err)
	}
	found := false
	for _, detail := range typed.Details() {
		value, e := detail.Value()
		if e != nil {
			t.Fatal(e)
		}
		if expiry, ok := value.(*v1.RetentionExpiry); ok {
			if expiry.Reason != v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_DOMAIN_REQUIRED || expiry.JobId != "" {
				t.Fatalf("expiry=%v", expiry)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("missing typed request-domain detail over HTTP")
	}
	message.Domain = &v1.RequestDomain{Lineage: strings.Repeat("a", 32), Epoch: 0}
	_, err = f.client.SubmitRetainedJob(ctx, authorized(f.tokens["alice"], message))
	code(t, err, connect.CodeInvalidArgument)
	message.Domain.Epoch = 1
	_, err = f.client.SubmitRetainedJob(ctx, authorized(f.tokens["alice"], message))
	code(t, err, connect.CodeFailedPrecondition)
	actor, err := f.engine.Authenticate("Bearer " + f.tokens["admin"])
	if err != nil {
		t.Fatal(err)
	}
	jobs, _, err := f.control.List(actor, 100, "")
	if err != nil || len(jobs) != 0 {
		t.Fatalf("refused input accepted jobs=%v err=%v", jobs, err)
	}
}
