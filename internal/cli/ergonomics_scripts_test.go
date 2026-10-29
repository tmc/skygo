package cli

import (
	"context"
	"testing"

	"rsc.io/script/scripttest"
)

func TestErgonomicsScripts(t *testing.T) {
	srv := serveErgonomics(t, new(ergonomicsService))
	scripttest.Test(t, context.Background(), scriptEngine(t), []string{"SKYGO_ENDPOINT=" + srv.URL}, "testdata/ergonomics/*.txt")
}
