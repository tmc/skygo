package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/tmc/skygo/internal/server"
)

func TestServerEnablesDurableLocal(t *testing.T) {
	var got server.Config
	cmd := command(func(_ context.Context, cfg server.Config) error {
		got = cfg
		return nil
	})
	cmd.SetArgs([]string{"server"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !got.DurableLocal || !filepath.IsAbs(got.LocalSupervisor) {
		t.Fatalf("DurableLocal=%v LocalSupervisor=%q", got.DurableLocal, got.LocalSupervisor)
	}
}
