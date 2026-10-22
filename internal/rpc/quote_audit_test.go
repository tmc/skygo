package rpc_test

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

func TestQuoteScopedAuditHealth(t *testing.T) {
	for _, currency := range []string{"USD", "invalid"} {
		t.Run(currency, func(t *testing.T) {
			dir := t.TempDir()
			s, err := store.Open(filepath.Join(dir, "state"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			// Auth refuses a group- or other-writable credential parent.
			private := filepath.Join(dir, "auth")
			if err := os.Mkdir(private, 0700); err != nil {
				t.Fatal(err)
			}
			config := filepath.Join(private, "auth.json")
			output := filepath.Join(private, "token")
			if _, err := auth.Initialize(config, output, s.Owner()); err != nil {
				t.Fatal(err)
			}
			if err := s.ActivateScoped(); err != nil {
				t.Fatal(err)
			}
			engine, err := auth.NewFile(config, s.Owner())
			if err != nil {
				t.Fatal(err)
			}
			c, err := control.New(s, []backend.Target{{Name: "a", Binding: "a", Backend: &quoteOnlyBackend{}, Advice: quoteHTTPAdvice(1)}}, engine)
			if err != nil {
				t.Fatal(err)
			}
			token, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			actor, err := engine.Authenticate("Bearer " + strings.TrimSpace(string(token)))
			if err != nil {
				t.Fatal(err)
			}
			// Leave one of the fixed 256 audit queue entries available.
			for i := 0; i < 255; i++ {
				if err := c.RecordAccess(actor, "plan", "", "allowed"); err != nil {
					t.Fatal(err)
				}
			}
			server := httptest.NewServer(rpc.NewHandler(c, engine))
			defer server.Close()
			client := skyv1connect.NewControlServiceClient(server.Client(), server.URL)
			req := connect.NewRequest(&v1.QuoteTaskRequest{Task: &v1.Task{Argv: []string{"program"}}, Currency: currency, Unit: v1.RateUnit_RATE_UNIT_TARGET_HOUR})
			req.Header().Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
			_, err = client.QuoteTask(context.Background(), req)
			if currency == "USD" && err != nil || currency != "USD" && connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("quote request: %v", err)
			}
			// Reconciliation is deliberately not running. This checks scoped audit
			// validation/enqueue acceptance, not eventual persistence or delivery.
			if !c.AuditHealthy() {
				t.Fatal("quote handler rejected or overflowed its audit category")
			}
			if err := c.RecordAccess(actor, "plan", "", "allowed"); err == nil || err.Error() != "audit queue full" {
				t.Fatalf("quote did not occupy the final audit slot: %v", err)
			}
			if c.AuditHealthy() {
				t.Fatal("queue overflow was not reported")
			}
		})
	}
}
