package cli

import "testing"

// TestSkypilotRuntimeScripts checks the environment of actual local jobs
// submitted from SkyPilot YAML through the production CLI and daemon.
func TestSkypilotRuntimeScripts(t *testing.T) {
	exe := retentionScriptBinary(t)
	runRetentionDaemonScript(t, exe, exe, "testdata/skypilot/runtime.txt", "")
}
